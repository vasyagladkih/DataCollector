package ru.datacollector.listener;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;
import ru.datacollector.model.Status;
import ru.datacollector.repository.entity.SubscriptionEntity;
import ru.datacollector.service.SubscriptionService;
import ru.datacollector.service.SubscriptionStorageService;

import java.lang.reflect.Method;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class SubscriptionRecoveryListenerTest {

    private static final Long TEST_ID = 1L;
    private static final Long TEST_ID_2 = 2L;
    private static final String EXCHANGE = "MOEX";
    private static final String CHANNEL = "trades";
    private static final String SYMBOL = "BBG004730N88";
    private static final String SYMBOL_2 = "SBER";
    private static final Instant TEST_TIMESTAMP = Instant.parse("2026-01-01T10:00:00Z");

    @Mock
    private SubscriptionStorageService storageService;

    @Mock
    private SubscriptionService subscriptionService;

    @InjectMocks
    private SubscriptionRecoveryListener recoveryListener;

    private SubscriptionEntity createEntity(Long id, String symbol) {
        return new SubscriptionEntity(
                id,
                EXCHANGE,
                CHANNEL,
                symbol,
                Status.CONNECTING,
                null,
                TEST_TIMESTAMP,
                TEST_TIMESTAMP,
                1
        );
    }

    @Test
    @DisplayName("1. onApplicationReady: извлекает незавершенные подписки из хранилища и передает в SubscriptionService")
    void onApplicationReady_whenPendingSubscriptionsExist_passesThemToSubscriptionService() {
        SubscriptionEntity entity1 = createEntity(TEST_ID, SYMBOL);
        SubscriptionEntity entity2 = createEntity(TEST_ID_2, SYMBOL_2);
        List<SubscriptionEntity> pending = List.of(entity1, entity2);

        when(storageService.findPendingRecovery()).thenReturn(pending);

        recoveryListener.onApplicationReady();

        verify(storageService).findPendingRecovery();
        verify(subscriptionService).recover(pending);
        verifyNoMoreInteractions(storageService, subscriptionService);
    }

    @Test
    @DisplayName("2. onApplicationReady: при отсутствии незавершенных подписок передает пустой список")
    void onApplicationReady_whenNoPendingSubscriptions_passesEmptyListToSubscriptionService() {
        List<SubscriptionEntity> emptyList = List.of();

        when(storageService.findPendingRecovery()).thenReturn(emptyList);

        recoveryListener.onApplicationReady();

        verify(storageService).findPendingRecovery();
        verify(subscriptionService).recover(emptyList);
        verifyNoMoreInteractions(storageService, subscriptionService);
    }

    @Test
    @DisplayName("3. onApplicationReady: метод аннотирован @EventListener с подпиской на ApplicationReadyEvent")
    void onApplicationReady_isAnnotatedWithApplicationReadyEventListener() throws NoSuchMethodException {
        Method method = SubscriptionRecoveryListener.class.getMethod("onApplicationReady");
        EventListener eventListener = method.getAnnotation(EventListener.class);

        assertThat(eventListener).isNotNull();
        assertThat(eventListener.value()).contains(ApplicationReadyEvent.class);
        assertThat(SubscriptionRecoveryListener.class.isAnnotationPresent(Component.class)).isTrue();
    }

    @Test
    @DisplayName("4. onApplicationReady: при исключении в хранилище пробрасывает ошибку и не вызывает SubscriptionService")
    void onApplicationReady_whenStorageThrowsException_propagatesAndDoesNotCallService() {
        when(storageService.findPendingRecovery()).thenThrow(new RuntimeException("DB error"));

        assertThatThrownBy(() -> recoveryListener.onApplicationReady())
                .isInstanceOf(RuntimeException.class)
                .hasMessage("DB error");

        verify(storageService).findPendingRecovery();
        verify(subscriptionService, never()).recover(any());
        verifyNoMoreInteractions(storageService, subscriptionService);
    }
}
