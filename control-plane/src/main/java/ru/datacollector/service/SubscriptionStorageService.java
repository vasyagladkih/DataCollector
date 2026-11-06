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
import java.util.function.UnaryOperator;

@RequiredArgsConstructor
@Service
public class SubscriptionStorageService {

    private final SubscriptionRepository repository;
    private final SubscriptionMapper mapper;

    @Transactional
    public SubscriptionEntity getOrCreate(CreateSubscriptionRequest request) {
        return repository.upsertAndIncrement(request.exchange(), request.channel(), request.symbol());
    }

    public SubscriptionEntity findById(UUID id) {
        return repository.findById(id)
                .orElseThrow(() -> new SubscriptionNotFoundException(id));
    }

    public List<SubscriptionEntity> findAll(String exchange, String channel, Status status) {
        return repository.findAll(exchange, channel, status);
    }

    public List<SubscriptionEntity> findPendingRecovery() {
        return repository.findPendingRecovery(List.of(
                Status.ACTIVE,
                Status.CONNECTING
        ));
    }

    private SubscriptionEntity update(UUID id, UnaryOperator<SubscriptionEntity> updater) {
        return repository.findByIdForUpdate(id)
                .map(updater)
                .map(repository::save)
                .orElseThrow(() -> new SubscriptionNotFoundException(id));
    }

    @Transactional
    public SubscriptionEntity markActive(UUID id) {
        return update(id, entity -> {
            if (entity.status() != Status.CONNECTING) return entity;
            return entity.withActive();
        });
    }

    @Transactional
    public SubscriptionEntity markFailed(UUID id, String message) {
        return update(id, entity -> {
            if (entity.status() != Status.CONNECTING) return entity;
            return entity.withFailed(message);
        });
    }

    @Transactional
    public SubscriptionEntity markRestarting(UUID id) {
        return update(id, entity -> {
            if (entity.status() != Status.FAILED) {
                throw new IllegalSubscriptionStateException(
                        "Перезапуск возможен только для подписок в статусе FAILED. Текущий статус: " + entity.status()
                );
            }
            return entity.withRestarting();
        });
    }

    @Transactional
    public SubscriptionEntity markStopped(UUID id) {
        return update(id, entity -> {
            if (entity.status() == Status.TERMINATED && entity.refCount() == 0) return entity;
            return entity.withTerminated();
        });
    }

    @Transactional
    public SubscriptionEntity decrementRefCount(UUID id) {
        return update(id, entity -> {
            if (entity.status() == Status.TERMINATED || entity.refCount() <= 0) {
                throw new IllegalSubscriptionStateException(
                        "Нельзя уменьшить счетчик: подписка " + id + " уже завершена"
                );
            }
            return entity.withDecrementedRef();
        });
    }

    @Transactional
    public SubscriptionEntity markStatus(UUID id, Status status) {
        return update(id, entity -> entity.withStatus(status));
    }
}
