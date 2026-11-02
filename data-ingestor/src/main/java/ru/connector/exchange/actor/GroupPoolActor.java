package ru.connector.exchange.actor;

import org.jspecify.annotations.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.reactive.socket.client.WebSocketClient;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import ru.connector.exceptions.SubscriptionNotFoundException;
import ru.connector.exchange.ExchangeAdapter;
import ru.connector.kafka.KafkaRawDataPublisher;
import ru.connector.models.GroupKey;
import ru.connector.models.SubscriptionDto;
import ru.datacollector.common.model.StreamKey;
import ru.connector.exchange.kukoin.KucoinRegistry;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

/**
 * Реактивный актор управления пулом сокетов для заданной группы (GroupKey).
 * <p>
 * Реализует:
 * - Последовательную обработку подписок/отписок без блокировок
 * - Стратегию уплотнения (First-Fit) до лимита емкости
 * - Автоматическое удаление и закрытие пустых сокетов при 0 активных подписок
 * - Быстрые потокобезопасные запросы O(1) через ConcurrentHashMap
 */
public final class GroupPoolActor implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(GroupPoolActor.class);

    private final GroupKey groupKey;
    private final ExchangeAdapter adapter;
    private final WebSocketClient wsClient;
    private final KafkaRawDataPublisher rawPublisher;
    private final Function<SubscriptionDto, String> subscribeTranslator;
    private final Function<SubscriptionDto, String> unsubscribeTranslator;
    private final int maxSubscriptionsPerConnection;

    private final Sinks.Many<PoolCommand> mailbox = Sinks.many().unicast().onBackpressureBuffer();
    private final Scheduler scheduler;
    private final AtomicInteger connectionIdSequence = new AtomicInteger(0);

    private final List<ConnectionActor> pool = new ArrayList<>();
    private final Map<StreamKey, SubscriptionDto> subscriptions = new HashMap<>();
    private final Map<StreamKey, ConnectionActor> streamToConnection = new HashMap<>();
    private final Map<ConnectionActor, Set<StreamKey>> connectionToStreams = new HashMap<>();

    private final Map<StreamKey, SubscriptionDto> readSubscriptions = new ConcurrentHashMap<>();

    private final AtomicBoolean isStarted = new AtomicBoolean(false);
    private volatile boolean closed = false;
    private Disposable mailboxLoop;

    public GroupPoolActor(
            GroupKey groupKey,
            ExchangeAdapter adapter,
            WebSocketClient wsClient,
            KafkaRawDataPublisher rawPublisher,
            Function<SubscriptionDto, String> subscribeTranslator,
            Function<SubscriptionDto, String> unsubscribeTranslator
    ) {
        this.groupKey = groupKey;
        this.adapter = adapter;
        this.wsClient = wsClient;
        this.rawPublisher = rawPublisher;
        this.subscribeTranslator = subscribeTranslator;
        this.unsubscribeTranslator = unsubscribeTranslator;
        this.maxSubscriptionsPerConnection = adapter.getMaxSubscriptionsPerConnection();
        this.scheduler = Schedulers.newSingle("pool-actor-" + groupKey.market() + "-" + groupKey.type());
    }

    public void start() {
        if (closed || !isStarted.compareAndSet(false, true)) return;

        this.mailboxLoop = mailbox.asFlux()
                .publishOn(scheduler)
                .concatMap(this::handleCommand)
                .subscribe();
    }

    private void registerSubscription(StreamKey key, SubscriptionDto req, ConnectionActor conn) {
        subscriptions.put(key, req);
        streamToConnection.put(key, conn);
        connectionToStreams.get(conn).add(key);
        readSubscriptions.put(key, req);
        log.info("[POOL-{}-{}] Подписка {} закреплена за сокетом #{}", groupKey.market(), groupKey.type(), key, conn.getId());
    }

    private void close(ConnectionActor conn) {closeConn(conn);}

    private @NonNull ConnectionActor getChosenConn() {
        return pool.stream()
            .filter(c -> !c.isClosed())
            .filter(c -> {
                Set<StreamKey> keys = connectionToStreams.get(c);
                return keys != null && keys.size() < maxSubscriptionsPerConnection;
            })
            .findFirst()
            .orElseGet(() -> {
                var conn = createConnection();
                pool.add(conn);
                connectionToStreams.put(conn, new HashSet<>());
                return conn;
            });
    }

    private Mono<Void> handleCommand(PoolCommand command) {
        return switch (command) {
            case PoolCommand.Subscribe sub -> handleSubscribe(sub);
            case PoolCommand.Unsubscribe unsub -> handleUnsubscribe(unsub);
            case PoolCommand.Close closeCmd -> handleClose(closeCmd);
        };
    }

    private Mono<Void> handleSubscribe(PoolCommand.Subscribe cmd) {
        SubscriptionDto request = cmd.request();
        StreamKey streamKey = request.key();

        return Mono.just(streamKey)
                .filter(key -> !subscriptions.containsKey(key))
                .flatMap(_ -> {
                    ConnectionActor conn = getChosenConn();
                    String frame = subscribeTranslator.apply(request);

                    return conn.subscribe(streamKey, frame)
                            .doOnSuccess(_ -> registerSubscription(streamKey, request, conn))
                            .doOnError(err -> handleSubscribeError(conn, err));
                })
                .doOnSuccess(_ -> cmd.reply().tryEmitEmpty())
                .doOnError(cmd.reply()::tryEmitError);
    }

    private Mono<Void> handleUnsubscribe(PoolCommand.Unsubscribe command) {
        StreamKey streamKey = command.key();

        return Mono.justOrEmpty(subscriptions.get(streamKey))
                .switchIfEmpty(Mono.error(() -> new SubscriptionNotFoundException(streamKey)))
                .flatMap(req -> Mono.justOrEmpty(streamToConnection.get(streamKey))
                .switchIfEmpty(Mono.error(() -> new IllegalStateException("Не найдено соединение для подписки: " + streamKey)))
                .flatMap(conn -> conn.unsubscribe(streamKey, unsubscribeTranslator.apply(req))
                .doFinally(_ -> cleanupSubscription(streamKey, conn))))
                .doOnSuccess(_ -> command.reply().tryEmitEmpty())
                .doOnError(command.reply()::tryEmitError);
    }

    private void handleSubscribeError(ConnectionActor conn, Throwable err) {
        log.error("Ошибка подписки на сокете {}", conn.getId(), err);
        Optional.ofNullable(connectionToStreams.get(conn))
                .filter(Set::isEmpty)
                .ifPresent(_ -> close(conn));
    }

    private void cleanupSubscription(StreamKey streamKey, ConnectionActor conn) {
        subscriptions.remove(streamKey);
        streamToConnection.remove(streamKey);
        readSubscriptions.remove(streamKey);

        log.info("[POOL-{}-{}] Подписка {} удалена из сокета #{}", groupKey.market(), groupKey.type(), streamKey, conn.getId());

        Optional.ofNullable(connectionToStreams.get(conn))
                .filter(keys -> keys.remove(streamKey) && keys.isEmpty())
                .ifPresent(_ -> closeConn(conn));
    }

    private void closeConn(ConnectionActor conn) {
        log.info("Соединение {} освободилось (0 подписок), закрываем сокет", conn.getId());
        conn.close();
        pool.remove(conn);
        connectionToStreams.remove(conn);
    }

    private ConnectionActor createConnection() {
        int connId = connectionIdSequence.incrementAndGet();
        URI url = URI.create(KucoinRegistry.getWsUrl(groupKey.market()));
        String partitionKey = "KUCOIN:" + groupKey.market();

        log.info("[POOL-{}-{}] Создание нового сокета #{}, URL={}", groupKey.market(), groupKey.type(), connId, url);

        ConnectionActor conn = ConnectionActor.builder()
                .id(connId)
                .exchangeUrl(url)
                .adapter(adapter)
                .wsClient(wsClient)
                .dataConsumer(bytes -> rawPublisher.publish(partitionKey, bytes))
                .fatalErrorConsumer(err -> log.error("Фатальная ошибка сокета {}", connId, err))
                .build();

        conn.start();
        return conn;
    }

    private Mono<Void> handleClose(PoolCommand.Close cmd) {
        close();
        cmd.reply().emitEmpty(Sinks.EmitFailureHandler.FAIL_FAST);
        return Mono.empty();
    }

    /**
     * Отправить команду подписки в очередь пула.
     */
    public Mono<Void> subscribe(SubscriptionDto request) {
        if (closed) return Mono.error(new IllegalStateException("Пул сокетов закрыт: " + groupKey));
        Sinks.One<Void> reply = Sinks.one();
        Sinks.EmitResult result = mailbox.tryEmitNext(new PoolCommand.Subscribe(request, reply));
        if (result.isFailure()) return Mono.error(new IllegalStateException("Очередь пула отклонила подписку: " + result));
        return reply.asMono();
    }

    /**
     * Отправить команду отписки в очередь пула.
     */
    public Mono<Void> unsubscribe(StreamKey key) {
        if (closed) return Mono.error(new IllegalStateException("Пул сокетов закрыт: " + groupKey));
        Sinks.One<Void> reply = Sinks.one();
        Sinks.EmitResult result = mailbox.tryEmitNext(new PoolCommand.Unsubscribe(key, reply));
        if (result.isFailure()) return Mono.error(new IllegalStateException("Очередь пула отклонила отписку: " + result));
        return reply.asMono();
    }

    public boolean exists(StreamKey key) {
        return readSubscriptions.containsKey(key);
    }

    public Optional<SubscriptionDto> findRequest(StreamKey key) {
        return Optional.ofNullable(readSubscriptions.get(key));
    }

    public List<SubscriptionDto> getAllActive() {
        return List.copyOf(readSubscriptions.values());
    }

    @Override
    public void close() {
        if (closed) return;
        closed = true;
        pool.forEach(ConnectionActor::close);
        pool.clear();
        connectionToStreams.clear();
        subscriptions.clear();
        streamToConnection.clear();
        readSubscriptions.clear();
        if (mailboxLoop != null && !mailboxLoop.isDisposed()) mailboxLoop.dispose();
        mailbox.tryEmitComplete();
        scheduler.dispose();
    }
}