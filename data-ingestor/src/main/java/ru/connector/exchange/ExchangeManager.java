package ru.connector.exchange;

import reactor.core.publisher.Mono;
import ru.connector.models.SubscriptionDto;
import ru.datacollector.common.model.StreamKey;

import java.util.List;
import java.util.Optional;

public interface ExchangeManager {
    boolean exists(StreamKey key);
    Optional<SubscriptionDto> findRequest(StreamKey key);
    Mono<Void> subscribe(SubscriptionDto request);
    Mono<Void> unsubscribe(StreamKey key);
    List<SubscriptionDto> getAllActive();
    void shutdown();
    ExchangeAdapter getAdapter();
}