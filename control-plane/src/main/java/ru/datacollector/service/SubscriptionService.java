package ru.datacollector.service;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import ru.datacollector.dto.CreateSubscriptionRequest;
import ru.datacollector.dto.CreateSubscriptionResponse;
import ru.datacollector.exceptions.SymbolNotFoundException;
import ru.datacollector.mapper.SubscriptionMapper;
import ru.datacollector.model.Status;
import ru.datacollector.persistance.entity.SubscriptionEntity;

import java.util.List;
import java.util.UUID;

@RequiredArgsConstructor
@Service
public class SubscriptionService {

    private final SubscriptionStorageService storageService;
    private final ConnectionPoolService connectionPoolService;
    private final KucoinRestService kucoinRestService;
    private final SubscriptionMapper mapper;

    public CreateSubscriptionResponse subscribe(CreateSubscriptionRequest request) {
        String result = kucoinRestService.fetch();
        if (result == null) {
            throw new SymbolNotFoundException("Валюта с текущим названием не найдена");
        }

        SubscriptionEntity entity = storageService.getOrCreate(request);
        connectionPoolService.subscribe(entity);

        return mapper.toDto(entity);
    }

    public CreateSubscriptionResponse restart(UUID id) {
        SubscriptionEntity entity = storageService.markRestarting(id);
        connectionPoolService.restart(entity);
        return mapper.toDto(entity);
    }

    public void stop(UUID id) {
        storageService.markStopped(id);
        connectionPoolService.stop(id);
    }

    public CreateSubscriptionResponse findById(UUID id) {
        return mapper.toDto(storageService.getExisting(id));
    }

    public List<CreateSubscriptionResponse> findAll(String exchange, String channel, Status status) {
        return storageService.findAll(exchange, channel, status)
                .stream()
                .map(mapper::toDto)
                .toList();
    }
}
