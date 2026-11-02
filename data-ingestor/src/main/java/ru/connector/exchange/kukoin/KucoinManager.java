package ru.connector.exchange.kukoin;

import org.springframework.stereotype.Component;
import org.springframework.web.reactive.socket.client.WebSocketClient;
import reactor.core.publisher.Mono;
import ru.connector.exceptions.SubscriptionNotFoundException;
import ru.connector.exchange.AbstractWebsocketManager;
import ru.connector.exchange.ExchangeAdapter;
import ru.connector.exchange.actor.GroupPoolActor;
import ru.connector.kafka.KafkaRawDataPublisher;
import ru.connector.models.Command;
import ru.connector.models.GroupKey;
import ru.connector.models.SubscriptionDto;
import ru.datacollector.common.model.Action;
import ru.datacollector.common.model.StreamKey;
import tools.jackson.databind.json.JsonMapper;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Менеджер подключения к бирже KuCoin на базе акторной модели.
 * <p>
 * Делегирует маршрутизацию подписок и управление пулом сокетов через {@link GroupPoolActor}.
 */
@Component("KUCOIN")
public class KucoinManager extends AbstractWebsocketManager {

    private final WebSocketClient wsClient;
    private final KafkaRawDataPublisher rawPublisher;
    private final JsonMapper jsonMapper;

    private final Map<GroupKey, GroupPoolActor> pools = new ConcurrentHashMap<>();

    public KucoinManager(WebSocketClient wsClient, KafkaRawDataPublisher rawPublisher, JsonMapper jsonMapper) {
        this.wsClient = wsClient;
        this.rawPublisher = rawPublisher;
        this.jsonMapper = jsonMapper;
    }

    @Override
    public boolean exists(StreamKey key) {
        return pools.values()
                .stream()
                .anyMatch(pool -> pool.exists(key));
    }

    @Override
    public Optional<SubscriptionDto> findRequest(StreamKey key) {
        return pools.values()
                .stream()
                .map(pool -> pool.findRequest(key))
                .flatMap(Optional::stream)
                .findFirst();
    }

    @Override
    public Mono<Void> subscribe(SubscriptionDto sub) {
        return Mono.defer(() -> {
            GroupKey groupKey = GroupKey.of(sub.market(), sub.type());
            GroupPoolActor poolActor = getOrCreatePool(groupKey);
            return poolActor.subscribe(sub);
        });
    }

    @Override
    public Mono<Void> unsubscribe(StreamKey key) {
        return pools.values()
                .stream()
                .filter(pool -> pool.exists(key))
                .findFirst()
                .map(pool -> pool.unsubscribe(key))
                .orElseGet(() -> Mono.error(new SubscriptionNotFoundException(key)));
    }

    @Override
    public List<SubscriptionDto> getAllActive() {
        return pools.values().stream()
                .flatMap(pool -> pool.getAllActive().stream())
                .toList();
    }

    @Override
    public String translate(SubscriptionDto request, Action action) {
        KucoinOutgoingMsg msg = resolveMsg(request, action);
        return jsonMapper.writeValueAsString(msg);
    }

    private KucoinOutgoingMsg resolveMsg(SubscriptionDto request, Action action) {

        String id = Long.toString(request.key().hashCode() & 0xFFFFFFFFL);
        String actionStr = KucoinRegistry.getAction(action);
        String marketType = KucoinRegistry.getMarketType(request.market());
        String symbol = KucoinRegistry.getSymbol(request.symbol(), request.market());
        String channel = KucoinRegistry.getChannel(request.command());

        return switch (request.command()) {
            case Command.Trades _ -> new KucoinOutgoingMsg.KucoinTradeMsg(id, actionStr, channel, marketType, symbol);

            case Command.BookTicker _ ->
                    new KucoinOutgoingMsg.KucoinTickerMsg(id, actionStr, channel, marketType, symbol);

            case Command.OrderBook ob -> new KucoinOutgoingMsg.KucoinOrderBookMsg(
                    id,
                    actionStr,
                    channel,
                    marketType,
                    symbol,
                    String.valueOf(ob.depth()),
                    0
            );
        };
    }

    private GroupPoolActor getOrCreatePool(GroupKey groupKey) {
        return pools.computeIfAbsent(groupKey, key -> {
            GroupPoolActor poolActor = new GroupPoolActor(
                    key,
                    getAdapter(),
                    wsClient,
                    rawPublisher,
                    sub -> translate(sub, Action.SUBSCRIBE),
                    sub -> translate(sub, Action.UNSUBSCRIBE)
            );
            poolActor.start();
            return poolActor;
        });
    }

    @Override
    public void shutdown() {
        pools.values().forEach(GroupPoolActor::close);
        pools.clear();
    }

    @Override
    public ExchangeAdapter getAdapter() {return new KucoinAdapter();}
}