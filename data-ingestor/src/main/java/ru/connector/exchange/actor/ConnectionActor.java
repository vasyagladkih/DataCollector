package ru.connector.exchange.actor;

import lombok.Builder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.jspecify.annotations.NonNull;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferUtils;
import org.springframework.web.reactive.socket.WebSocketHandler;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.WebSocketSession;
import org.springframework.web.reactive.socket.client.WebSocketClient;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.util.retry.Retry;
import ru.connector.exchange.ExchangeAdapter;
import ru.datacollector.common.model.StreamKey;

import java.net.URI;
import java.text.MessageFormat;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * Реактивный актор отдельного WebSocket-соединения.
 * <p>
 * Изолирует состояние подписок, автоматический реконнект с backoff, rate-limiting
 * и потокобезопасную передачу команд через однопоточный планировщик.
 */
@Slf4j
public final class ConnectionActor {

    private static final int DEFAULT_BUFFER_SIZE = 1024;
    private static final Duration DEFAULT_READ_IDLE_TIMEOUT = Duration.ofSeconds(60);
    private static final Duration DEFAULT_RATE_LIMIT_DELAY = Duration.ZERO;
    private static final Duration DEFAULT_RECONNECT_DELAY = Duration.ofSeconds(1);

    @Getter
    private final int id;
    private final URI exchangeUrl;
    private final ExchangeAdapter adapter;
    private final WebSocketClient wsClient;
    private final Consumer<byte[]> dataConsumer;
    private final Consumer<Throwable> fatalErrorConsumer;
    private final Duration readIdleTimeout;
    private final Duration rateLimitDelay;
    private final Duration reconnectDelay;

    private final Sinks.One<Void> connected = Sinks.one();

    private final Map<StreamKey, String> activeSubscriptions = new ConcurrentHashMap<>();
    private final Sinks.Many<String> outboundSink = Sinks.many().multicast().onBackpressureBuffer(DEFAULT_BUFFER_SIZE, false);


    private final Sinks.Many<ConnectionCommand> mailbox = Sinks.many().unicast().onBackpressureBuffer();
    private final Scheduler scheduler;


    // Атомарные флаги жизненного цикла актора.
    // Замена volatile boolean closed на AtomicBoolean устраняет гонки состояния (race condition)
    // между параллельными вызовами subscribe/unsubscribe/close (устранение антипаттерна Check-Then-Act).
    private final AtomicBoolean isClosed = new AtomicBoolean(false);
    private final AtomicBoolean isConnected = new AtomicBoolean(false);
    private final AtomicBoolean isStarted = new AtomicBoolean(false);

    private Disposable connection;
    private Disposable mailboxLoop;

    @Builder
    private ConnectionActor(
            int id,
            @NonNull URI exchangeUrl,
            @NonNull ExchangeAdapter adapter,
            @NonNull WebSocketClient wsClient,
            @NonNull Consumer<byte[]> dataConsumer,
            @NonNull Consumer<Throwable> fatalErrorConsumer,
            Duration readIdleTimeout,
            Duration rateLimitDelay,
            Duration reconnectDelay
    ) {
        this.id = id;
        this.exchangeUrl = exchangeUrl;
        this.adapter = adapter;
        this.wsClient = wsClient;
        this.dataConsumer = dataConsumer;
        this.fatalErrorConsumer = fatalErrorConsumer;

        this.readIdleTimeout = readIdleTimeout != null ? readIdleTimeout : DEFAULT_READ_IDLE_TIMEOUT;
        this.rateLimitDelay = rateLimitDelay != null ? rateLimitDelay : DEFAULT_RATE_LIMIT_DELAY;
        this.reconnectDelay = reconnectDelay != null ? reconnectDelay : DEFAULT_RECONNECT_DELAY;

        this.scheduler = Schedulers.newSingle("conn-actor-" + id);
    }

    public void start() {
        if (isClosed.get() || !isStarted.compareAndSet(false, true)) return;

        this.mailboxLoop = mailbox.asFlux()
                .publishOn(scheduler)
                .concatMap(this::handleCommand)
                .subscribe();

        WebSocketHandler handler = session -> {
            connected.tryEmitEmpty();
            log.info("[WS #{}] WebSocket-соединение УСТАНОВЛЕНО (Session ID: {})", id, session.getId());
            Mono<Void> inbound = inboundDataStream(session);
            Flux<WebSocketMessage> outbound = outboundDataStream(session);
            return session.send(outbound).and(inbound);
        };

        this.connection = wsClient.execute(exchangeUrl, handler)
                .doOnError(connected::tryEmitError)
                .retryWhen(Retry.backoff(Long.MAX_VALUE, Duration.ofMillis(200))
                    .maxBackoff(Duration.ofMinutes(1))
                    .filter(adapter::isRecoverable)
                    .doBeforeRetry(signal -> log.warn("[WS #{}] Разрыв связи. Попытка реконнекта #{}, причина: {}",
                            id, signal.totalRetries() + 1, signal.failure().getMessage())))
                .repeatWhen(completed -> completed.delayElements(reconnectDelay))
                .doOnError(err -> {
                    log.error("Фатальная ошибка WebSocket-соединения {}", id, err);
                    fatalErrorConsumer.accept(err);
                })
                .subscribe();
    }

    private @NonNull Flux<WebSocketMessage> outboundDataStream(WebSocketSession session) {
        Flux<String> outboundFlux = outboundSink.asFlux();
        Flux<String> commands;

        if (isConnected.compareAndSet(false, true)) {
            commands = outboundFlux;
        } else {
            List<String> snapshot = new ArrayList<>(activeSubscriptions.values());
            Flux<String> resubscribeStream = Flux.fromIterable(snapshot);
            commands = Flux.concat(resubscribeStream, outboundFlux);
        }

        if (!rateLimitDelay.isZero()) commands = commands.delayElements(rateLimitDelay);

        return Flux.merge(
                commands.map(session::textMessage),
                adapter.createPingStream(session)
        );
    }

    private @NonNull Mono<Void> inboundDataStream(WebSocketSession session) {
        return session.receive()
                .timeout(readIdleTimeout)
                .doOnNext(msg -> {

                    if (msg.getType() == WebSocketMessage.Type.PONG) return;
                    if (msg.getType() == WebSocketMessage.Type.PING) return;

                    DataBuffer buffer = msg.getPayload();
                    try {
                        // 2. Инвертированная проверка: если фрейм содержит полезную рыночную нагрузку (trade/depth),
                        // он сразу вычитывается и передается в Kafka (dataConsumer). Иначе передается в адаптер
                        // для обработки сервисных сообщений (ack подписки, welcome, ошибки биржи).
                        if (adapter.isPayload(buffer)) {
                            byte[] bytes = new byte[buffer.readableByteCount()]; // TODO
                            buffer.read(bytes);
                            dataConsumer.accept(bytes);
                        } else {
                            adapter.handleServiceMsg(buffer);
                        }
                    } catch (Exception e) {
                        log.error("Ошибка обработки входящего фрейма в сокете {}", id, e);
                    } finally {DataBufferUtils.release(buffer);}
                }).then();
    }

    private Mono<Void> handleCommand(ConnectionCommand command) {
        return switch (command) {
            case ConnectionCommand.Subscribe sub -> handleSubscribe(sub);
            case ConnectionCommand.Unsubscribe unsub -> handleUnsubscribe(unsub);
            case ConnectionCommand.Close closeCmd -> handleClose(closeCmd);
        };
    }

    private Mono<Void> handleSubscribe(ConnectionCommand.Subscribe cmd) {
        if (isClosed.get()) {
            cmd.reply().emitError(new IllegalStateException(MessageFormat.format("Соединение {0} закрыто", id)), Sinks.EmitFailureHandler.FAIL_FAST);
            return Mono.empty();
        }

        activeSubscriptions.put(cmd.key(), cmd.frame());
        log.debug("[WS #{}] Отправка фрейма подписки для {}: {}", id, cmd.key(), cmd.frame());
        Sinks.EmitResult result = outboundSink.tryEmitNext(cmd.frame());
        if (result.isFailure()) {
            activeSubscriptions.remove(cmd.key());
            cmd.reply().emitError(new IllegalStateException("Не удалось отправить фрейм подписки: " + result), Sinks.EmitFailureHandler.FAIL_FAST);
            return Mono.empty();
        }
        cmd.reply().emitEmpty(Sinks.EmitFailureHandler.FAIL_FAST);
        return Mono.empty();
    }

    private Mono<Void> handleUnsubscribe(ConnectionCommand.Unsubscribe cmd) {
        return Mono.just(isClosed.get())
                .filter(closed -> !closed)
                .switchIfEmpty(Mono.error(() -> new IllegalStateException(MessageFormat.format("Соединение {0} закрыто", id))))
                .flatMap(_ -> Mono.justOrEmpty(activeSubscriptions.remove(cmd.key())))
                .flatMap(removed -> {
                    log.debug("[WS #{}] Отправка фрейма отписки для {}: {}", id, cmd.key(), cmd.unsubscribeFrame());
                    Sinks.EmitResult result = outboundSink.tryEmitNext(cmd.unsubscribeFrame());
                    if (result.isFailure()) {
                        activeSubscriptions.put(cmd.key(), removed);
                        return Mono.error(new IllegalStateException("Не удалось отправить фрейм отписки: " + result));
                    }
                    return Mono.empty();
                })
                .doOnSuccess(_ -> cmd.reply().tryEmitEmpty())
                .doOnError(cmd.reply()::tryEmitError)
                .then();
    }

    private Mono<Void> handleClose(ConnectionCommand.Close cmd) {
        close();
        cmd.reply().emitEmpty(Sinks.EmitFailureHandler.FAIL_FAST);
        return Mono.empty();
    }

    public Mono<Void> subscribe(StreamKey key, String frame) {
        if (isClosed.get()) return Mono.error(new IllegalStateException("Соединение " + id + " закрыто"));
        return connected.asMono().then(Mono.defer(() -> {
            if (isClosed.get()) return Mono.error(new IllegalStateException("Соединение " + id + " закрыто"));
            Sinks.One<Void> reply = Sinks.one();
            Sinks.EmitResult result;
            // Project Reactor Sinks.many().unicast() не является потокобезопасным при параллельных вызовах.
            // Блок synchronized (mailbox) сериализует доступ к tryEmitNext из пула потоков,
            // предотвращая ошибку Sinks.EmitResult.FAIL_NON_SERIALIZED при конкурентной подписке.
            synchronized (mailbox) {
                result = mailbox.tryEmitNext(new ConnectionCommand.Subscribe(key, frame, reply));
            }
            if (result.isFailure()) return Mono.error(new IllegalStateException("Очередь актора отклонила подписку: " + result));
            return reply.asMono();
        }));
    }

    public Mono<Void> unsubscribe(StreamKey key, String unsubscribeFrame) {
        if (isClosed.get()) return Mono.error(new IllegalStateException("Соединение " + id + " закрыто"));
        Sinks.One<Void> reply = Sinks.one();
        Sinks.EmitResult result;
        // Сериализация параллельных запросов отписки для защиты от FAIL_NON_SERIALIZED
        synchronized (mailbox) {
            result = mailbox.tryEmitNext(new ConnectionCommand.Unsubscribe(key, unsubscribeFrame, reply));
        }
        if (result.isFailure()) return Mono.error(new IllegalStateException("Очередь актора отклонила отписку: " + result));
        return reply.asMono();
    }

    public void close() {
        // Атомарный CAS-переход гарантирует идемпотентность: ровно один поток выполняет процедуру закрытия
        if (!isClosed.compareAndSet(false, true)) return;
        log.info("[WS #{}] Соединение закрыто", id);
        connected.tryEmitEmpty();
        if (connection != null && !connection.isDisposed()) connection.dispose();
        if (mailboxLoop != null && !mailboxLoop.isDisposed()) mailboxLoop.dispose();
        outboundSink.tryEmitComplete();
        // Сериализация завершения очереди mailbox
        synchronized (mailbox) {
            mailbox.tryEmitComplete();
        }
        scheduler.dispose();
    }

    public boolean isClosed() {
        return isClosed.get();
    }
}