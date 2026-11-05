package ru.datacollector.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;
import ru.datacollector.persistance.entity.SubscriptionEntity;

import java.util.List;

@Slf4j
@RequiredArgsConstructor
@Component
public class SubscriptionRecoveryListener {

    private final SubscriptionStorageService storageService;
    private final ConnectionPoolService connectionPoolService;

    @EventListener(ApplicationReadyEvent.class)
    public void onApplicationReady() {
        List<SubscriptionEntity> pending = storageService.findPendingRecovery();
        log.info("Запуск Crash Recovery: обнаружено {} подписок для восстановления", pending.size());
        for (SubscriptionEntity entity : pending) {connectionPoolService.subscribe(entity);}
        log.info("Crash Recovery завершен: {} подписок зарегистрировано в пуле соединений", pending.size());
    }
}
