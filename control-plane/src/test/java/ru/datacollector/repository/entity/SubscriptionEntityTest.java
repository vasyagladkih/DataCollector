package ru.datacollector.repository.entity;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import ru.datacollector.model.Status;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

class SubscriptionEntityTest {

    private static final Long TEST_ID = 1L;
    private static final String EXCHANGE = "MOEX";
    private static final String CHANNEL = "trades";
    private static final String SYMBOL = "BBG004730N88";
    private static final String TEST_ERROR = "Network connection timeout";

    private SubscriptionEntity createEntity(Status status, int refCount, String errorMessage) {
        Instant timestamp = Instant.parse("2026-01-01T10:00:00Z");
        return new SubscriptionEntity(
                TEST_ID,
                EXCHANGE,
                CHANNEL,
                SYMBOL,
                status,
                errorMessage,
                timestamp,
                timestamp,
                refCount
        );
    }

    @Test
    @DisplayName("withDecrementedRef: уменьшает refCount на 1 и сохраняет статус ACTIVE, если остаются потребители")
    void withDecrementedRef_whenMultipleRefs_decrementsCountAndPreservesStatus() {
        SubscriptionEntity entity = createEntity(Status.ACTIVE, 3, null);

        SubscriptionEntity updated = entity.withDecrementedRef();

        assertThat(updated.id()).isEqualTo(entity.id());
        assertThat(updated.exchange()).isEqualTo(entity.exchange());
        assertThat(updated.channel()).isEqualTo(entity.channel());
        assertThat(updated.symbol()).isEqualTo(entity.symbol());
        assertThat(updated.refCount()).isEqualTo(2);
        assertThat(updated.status()).isEqualTo(Status.ACTIVE);
        assertThat(updated.errorMessage()).isNull();
        assertThat(updated.createdAt()).isEqualTo(entity.createdAt());
        assertThat(updated.updatedAt()).isAfterOrEqualTo(entity.updatedAt());
    }

    @Test
    @DisplayName("withDecrementedRef: при обнулении счетчика (с 1 до 0) переводит подписку в статус TERMINATED")
    void withDecrementedRef_whenLastRef_decrementsToZeroAndSetsTerminated() {
        SubscriptionEntity entity = createEntity(Status.ACTIVE, 1, null);

        SubscriptionEntity updated = entity.withDecrementedRef();

        assertThat(updated.id()).isEqualTo(entity.id());
        assertThat(updated.exchange()).isEqualTo(entity.exchange());
        assertThat(updated.channel()).isEqualTo(entity.channel());
        assertThat(updated.symbol()).isEqualTo(entity.symbol());
        assertThat(updated.refCount()).isZero();
        assertThat(updated.status()).isEqualTo(Status.TERMINATED);
        assertThat(updated.errorMessage()).isNull();
        assertThat(updated.createdAt()).isEqualTo(entity.createdAt());
        assertThat(updated.updatedAt()).isAfterOrEqualTo(entity.updatedAt());
    }

    @Test
    @DisplayName("withDecrementedRef: при нулевом счетчике не уходит в минус и сохраняет TERMINATED")
    void withDecrementedRef_whenAlreadyZero_remainsZeroAndTerminated() {
        SubscriptionEntity entity = createEntity(Status.TERMINATED, 0, null);

        SubscriptionEntity updated = entity.withDecrementedRef();

        assertThat(updated.id()).isEqualTo(entity.id());
        assertThat(updated.exchange()).isEqualTo(entity.exchange());
        assertThat(updated.channel()).isEqualTo(entity.channel());
        assertThat(updated.symbol()).isEqualTo(entity.symbol());
        assertThat(updated.refCount()).isZero();
        assertThat(updated.status()).isEqualTo(Status.TERMINATED);
        assertThat(updated.errorMessage()).isNull();
        assertThat(updated.createdAt()).isEqualTo(entity.createdAt());
        assertThat(updated.updatedAt()).isAfterOrEqualTo(entity.updatedAt());
    }

    @Test
    @DisplayName("withDecrementedRef: гарантирует иммутабельность оригинала и сохраняет бизнес-идентификаторы")
    void withDecrementedRef_preservesIdentityAndAuditTimestamps() {
        SubscriptionEntity original = createEntity(Status.ACTIVE, 2, null);

        SubscriptionEntity updated = original.withDecrementedRef();

        // Проверка неизменности оригинала (Java Record immutability)
        assertThat(original.refCount()).isEqualTo(2);
        assertThat(original.status()).isEqualTo(Status.ACTIVE);

        // Проверка идентичности бизнес-полей
        assertThat(updated.id()).isEqualTo(original.id());
        assertThat(updated.exchange()).isEqualTo(original.exchange());
        assertThat(updated.channel()).isEqualTo(original.channel());
        assertThat(updated.symbol()).isEqualTo(original.symbol());
        assertThat(updated.createdAt()).isEqualTo(original.createdAt());
        assertThat(updated.updatedAt()).isAfterOrEqualTo(original.updatedAt());
    }

    @Test
    @DisplayName("withActive: устанавливает статус ACTIVE и очищает сообщение об ошибке")
    void withActive_setsActiveStatusAndClearsErrorMessage() {
        SubscriptionEntity entity = createEntity(Status.CONNECTING, 1, TEST_ERROR);

        SubscriptionEntity updated = entity.withActive();

        assertThat(updated.id()).isEqualTo(entity.id());
        assertThat(updated.exchange()).isEqualTo(entity.exchange());
        assertThat(updated.channel()).isEqualTo(entity.channel());
        assertThat(updated.symbol()).isEqualTo(entity.symbol());
        assertThat(updated.status()).isEqualTo(Status.ACTIVE);
        assertThat(updated.errorMessage()).isNull();
        assertThat(updated.refCount()).isEqualTo(1);
        assertThat(updated.createdAt()).isEqualTo(entity.createdAt());
        assertThat(updated.updatedAt()).isAfterOrEqualTo(entity.updatedAt());
    }

    @Test
    @DisplayName("withFailed: устанавливает статус FAILED и фиксирует причину ошибки")
    void withFailed_setsFailedStatusAndErrorMessage() {
        SubscriptionEntity entity = createEntity(Status.ACTIVE, 1, null);

        SubscriptionEntity updated = entity.withFailed(TEST_ERROR);

        assertThat(updated.id()).isEqualTo(entity.id());
        assertThat(updated.exchange()).isEqualTo(entity.exchange());
        assertThat(updated.channel()).isEqualTo(entity.channel());
        assertThat(updated.symbol()).isEqualTo(entity.symbol());
        assertThat(updated.status()).isEqualTo(Status.FAILED);
        assertThat(updated.errorMessage()).isEqualTo(TEST_ERROR);
        assertThat(updated.refCount()).isEqualTo(1);
        assertThat(updated.createdAt()).isEqualTo(entity.createdAt());
        assertThat(updated.updatedAt()).isAfterOrEqualTo(entity.updatedAt());
    }

    @Test
    @DisplayName("withRestarting: переводит подписку в CONNECTING и сбрасывает старую ошибку")
    void withRestarting_setsConnectingStatusAndClearsErrorMessage() {
        SubscriptionEntity entity = createEntity(Status.FAILED, 1, TEST_ERROR);

        SubscriptionEntity updated = entity.withRestarting();

        assertThat(updated.id()).isEqualTo(entity.id());
        assertThat(updated.exchange()).isEqualTo(entity.exchange());
        assertThat(updated.channel()).isEqualTo(entity.channel());
        assertThat(updated.symbol()).isEqualTo(entity.symbol());
        assertThat(updated.status()).isEqualTo(Status.CONNECTING);
        assertThat(updated.errorMessage()).isNull();
        assertThat(updated.refCount()).isEqualTo(1);
        assertThat(updated.createdAt()).isEqualTo(entity.createdAt());
        assertThat(updated.updatedAt()).isAfterOrEqualTo(entity.updatedAt());
    }
}
