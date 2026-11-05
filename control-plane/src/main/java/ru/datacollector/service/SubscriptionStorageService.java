package ru.datacollector.service;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.datacollector.dto.CreateSubscriptionRequest;
import ru.datacollector.exceptions.IllegalSubscriptionStateException;
import ru.datacollector.exceptions.SubscriptionNotFoundException;
import ru.datacollector.mapper.SubscriptionMapper;
import ru.datacollector.model.Status;
import ru.datacollector.persistance.SubscriptionRepository;
import ru.datacollector.persistance.entity.SubscriptionEntity;

import java.util.List;
import java.util.UUID;

@RequiredArgsConstructor
@Service
public class SubscriptionStorageService {

    private final SubscriptionRepository repository;
    private final SubscriptionMapper mapper;

    @Transactional
    public SubscriptionEntity getOrCreate(CreateSubscriptionRequest request) {
        return repository.findByExchangeAndChannelAndSymbol(
                request.exchange(), request.channel(), request.symbol()
        ).orElseGet(() -> repository.save(mapper.toEntity(request)));
    }

    @Transactional
    public SubscriptionEntity markRestarting(UUID id) {
        SubscriptionEntity entity = getExisting(id);
        if (entity.status() != Status.FAILED) {
            throw new IllegalSubscriptionStateException(
                    "Перезапуск возможен только для подписок в статусе FAILED. Текущий статус: " + entity.status()
            );
        }
        return repository.save(entity.withStatus(Status.CONNECTING));
    }

    @Transactional
    public SubscriptionEntity markStopped(UUID id) {
        SubscriptionEntity entity = getExisting(id);
        if (entity.status() != Status.STOPPED) {
            return repository.save(entity.withStatus(Status.STOPPED));
        }
        return entity;
    }

    public SubscriptionEntity getExisting(UUID id) {
        return repository.findById(id)
                .orElseThrow(() -> new SubscriptionNotFoundException(id));
    }

    public List<SubscriptionEntity> findAll(String exchange, String channel, Status status) {
        return repository.findAll(exchange, channel, status);
    }

    public List<SubscriptionEntity> findPendingRecovery() {
        return repository.findByStatusIn(List.of(
                Status.ACTIVE,
                Status.CONNECTING,
                Status.RECONNECTING
        ));
    }
}
