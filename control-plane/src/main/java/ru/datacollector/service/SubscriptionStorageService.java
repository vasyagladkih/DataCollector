package ru.datacollector.service;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.datacollector.dto.CreateSubscriptionRequest;
import ru.datacollector.exception.IllegalSubscriptionStateException;
import ru.datacollector.exception.SubscriptionNotFoundException;
import ru.datacollector.model.Status;
import ru.datacollector.repository.SubscriptionRepository;
import ru.datacollector.repository.entity.SubscriptionEntity;

import java.util.List;
import java.util.function.UnaryOperator;

@RequiredArgsConstructor
@Service
public class SubscriptionStorageService {

    private final SubscriptionRepository repository;

    @Transactional
    public SubscriptionEntity getOrCreate(CreateSubscriptionRequest request) {
        return repository.upsertAndIncrement(request.exchange(), request.channel(), request.symbol());
    }

    public SubscriptionEntity findById(Long id) {
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

    private SubscriptionEntity update(Long id, UnaryOperator<SubscriptionEntity> updater) {
        return repository.findByIdForUpdate(id)
                .map(updater)
                .map(repository::save)
                .orElseThrow(() -> new SubscriptionNotFoundException(id));
    }

    @Transactional
    public SubscriptionEntity markActive(Long id) {
        return update(id, entity -> {
            if (entity.status() != Status.CONNECTING) return entity;
            return entity.withActive();
        });
    }

    @Transactional
    public SubscriptionEntity markFailed(Long id, String message) {
        return update(id, entity -> {
            if (entity.status() != Status.CONNECTING && entity.status() != Status.ACTIVE) return entity;
            return entity.withFailed(message);
        });
    }

    @Transactional
    public SubscriptionEntity markRestarting(Long id) {
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
    public SubscriptionEntity decrementRefCount(Long id) {
        return update(id, entity -> {
            if (entity.status() == Status.TERMINATED || entity.refCount() <= 0) {
                throw new IllegalSubscriptionStateException(
                        "Нельзя уменьшить счетчик: подписка " + id + " уже завершена"
                );
            }
            return entity.withDecrementedRef();
        });
    }
}