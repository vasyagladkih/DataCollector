package ru.datacollector.service;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import ru.datacollector.common.model.Action;
import ru.datacollector.common.model.StreamKey;
import ru.datacollector.dto.Subscription;
import ru.datacollector.persistance.entity.SubscriptionEntity;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

@RequiredArgsConstructor
@Service
public class ConnectionPoolService {

    private static final int MAX_SUBS_PER_SOCKET = 100;

    private final Map<UUID, Subscription> registry = new ConcurrentHashMap<>();
    private final Map<StreamKey, List<ExchangeConnection>> connectionPools = new ConcurrentHashMap<>();

    public synchronized void subscribe(SubscriptionEntity entity) {
        if (registry.containsKey(entity.id())) return;

        StreamKey key = new StreamKey(entity.exchange(), entity.channel());
        ExchangeConnection conn = getOrCreateAvailableConnection(key);
        Subscription sub = new Subscription(entity.id(), key, entity.symbol(), conn);
        registry.put(entity.id(), sub);
        conn.send(entity.symbol(), Action.SUBSCRIBE);
    }

    public synchronized void restart(SubscriptionEntity entity) {
        Subscription sub = registry.get(entity.id());
        if (sub != null && sub.connection().isAlive()) {
            sub.connection().send(sub.symbol(), Action.SUBSCRIBE);
            return;
        }

        if (sub != null) {
            registry.remove(entity.id());
            sub.connection().releaseSlot();
        }
        subscribe(entity);
    }

    public synchronized void stop(UUID id) {
        Subscription sub = registry.remove(id);
        if (sub == null) return;

        ExchangeConnection conn = sub.connection();
        conn.send(sub.symbol(), Action.UNSUBSCRIBE);

        if (conn.releaseSlot() <= 0) {
            List<ExchangeConnection> pool = connectionPools.get(sub.streamKey());
            if (pool != null) {
                pool.remove(conn);
                if (pool.isEmpty()) {
                    connectionPools.remove(sub.streamKey());
                }
            }
            conn.close();
        }
    }

    private ExchangeConnection getOrCreateAvailableConnection(StreamKey key) {
        List<ExchangeConnection> pool = connectionPools.computeIfAbsent(key, _ -> new CopyOnWriteArrayList<>());

        Optional<ExchangeConnection> connection = pool.stream()
                .filter(ExchangeConnection::tryAcquireSlot)
                .findFirst();

        if (connection.isPresent()) return connection.get();

        ExchangeConnection newConn = new ExchangeConnection(key, MAX_SUBS_PER_SOCKET, this::handleReconnect);
        newConn.tryAcquireSlot();
        pool.add(newConn);
        return newConn;
    }

    private void handleReconnect(ExchangeConnection reconnectedSocket) {
        // TODO: добавить backoff и батчинг подписок (до 50 тикетов в сообщение)
        // Простое сравнение ссылок по ==: восстанавливаем только то, что шло в этот сокет
        registry.values().forEach(sub -> {
            if (sub.connection() == reconnectedSocket) {
                sub.connection().send(sub.symbol(), Action.SUBSCRIBE);
            }
        });
    }

    public boolean exists(UUID id) {return registry.containsKey(id);}
}
