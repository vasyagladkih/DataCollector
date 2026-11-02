package ru.datacollector.service;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionTemplate;
import ru.datacollector.dto.CreateSubscriptionRequest;
import ru.datacollector.dto.CreateSubscriptionResponse;
import ru.datacollector.exceptions.IllegalSubscriptionStateException;
import ru.datacollector.exceptions.SubscriptionNotFoundException;
import ru.datacollector.exceptions.SymbolNotFoundException;
import ru.datacollector.mapper.SubscriptionMapper;
import ru.datacollector.model.Status;
import ru.datacollector.persistance.UserSubscriptionRepository;
import ru.datacollector.persistance.entity.SubscriptionEntity;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

@RequiredArgsConstructor
@Service
public class SubscriptionService {

    private final UserSubscriptionRepository userSubscriptionRepository;
    private final SubscriptionMapper mapper;
    private final TransactionTemplate transactionTemplate;

    public CreateSubscriptionResponse subscribe(CreateSubscriptionRequest request) {
        String result = fetch(); // TODO валидация
        if (result == null) {
            throw new SymbolNotFoundException("Валюта с текущим названием не найдена");
        }

        return transactionTemplate.execute(_ -> {
            Optional<SubscriptionEntity> existing = userSubscriptionRepository
                    .findByExchangeAndActionAndSymbol(request.exchange(), request.action(), request.symbol());

            if (existing.isPresent()) return mapper.toDto(existing.get());

            SubscriptionEntity entity = mapper.toEntity(request);
            SubscriptionEntity saved = userSubscriptionRepository.save(entity);
            return mapper.toDto(saved);
        });
    }

    public String fetch() {
        return null;
    }

    @Transactional(readOnly = true)
    public CreateSubscriptionResponse findById(UUID id) {
        return userSubscriptionRepository.findById(id)
                .map(mapper::toDto)
                .orElseThrow(() -> new SubscriptionNotFoundException(id));
    }

    @Transactional(readOnly = true)
    public List<CreateSubscriptionResponse> findAll(String exchange, String action, Status status) {
        return userSubscriptionRepository.findAll(exchange, action, status)
                .stream()
                .map(mapper::toDto)
                .toList();
    }

    @Transactional
    public CreateSubscriptionResponse restart(UUID id) {
        SubscriptionEntity entity = userSubscriptionRepository.findById(id)
                .orElseThrow(() -> new SubscriptionNotFoundException(id));

        if (entity.status() != Status.FAILED) {
            throw new IllegalSubscriptionStateException(
                    "Перезапуск возможен только для подписок в статусе FAILED. Текущий статус: " + entity.status()
            );
        }

        SubscriptionEntity updated = entity.withStatus(Status.CONNECTING);
        SubscriptionEntity saved = userSubscriptionRepository.save(updated);
        return mapper.toDto(saved);
    }

    @Transactional
    public void stop(UUID id) {
        SubscriptionEntity entity = userSubscriptionRepository.findById(id)
                .orElseThrow(() -> new SubscriptionNotFoundException(id));

        if (entity.status() != Status.STOPPED) {
            SubscriptionEntity updated = entity.withStatus(Status.STOPPED);
            userSubscriptionRepository.save(updated);
        }
    }
}
