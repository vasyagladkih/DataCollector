package ru.datacollector.repository;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.data.jdbc.test.autoconfigure.DataJdbcTest;
import org.springframework.boot.jdbc.test.autoconfigure.AutoConfigureTestDatabase;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import ru.datacollector.model.Status;
import ru.datacollector.repository.entity.SubscriptionEntity;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@DataJdbcTest
@Testcontainers
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
class SubscriptionRepositoryIT {

    @Container
    @ServiceConnection
    static PostgreSQLContainer postgres = new PostgreSQLContainer("postgres:16-alpine");

    private static final String EXCHANGE = "MOEX";
    private static final String CHANNEL = "trades";
    private static final String SYMBOL = "BBG004730N88";
    private static final String SYMBOL_2 = "SBER";
    private static final String CONCURRENT_SYMBOL = "CONCURRENT_SYM";
    private static final String ERROR_MSG = "Old error message";

    @Autowired
    private SubscriptionRepository repository;

    private SubscriptionEntity createAndSave(String exchange, String channel, String symbol,
                                             Status status, String error, int refCount) {
        Instant past = Instant.now().minus(1, ChronoUnit.SECONDS).truncatedTo(ChronoUnit.MICROS);
        SubscriptionEntity entity = new SubscriptionEntity(
                null,
                exchange,
                channel,
                symbol,
                status,
                error,
                past,
                past,
                refCount
        );
        return repository.save(entity);
    }

    // =========================================================================
    // Группа 1: Атомарный upsertAndIncrement
    // =========================================================================

    @Test
    @DisplayName("1. upsertAndIncrement: при первом обращении вставляет новую запись со статусом CONNECTING и ref_count = 1")
    void upsertAndIncrement_whenNewRecord_insertsConnectingWithRefCount1() {
        SubscriptionEntity entity = repository.upsertAndIncrement(EXCHANGE, CHANNEL, SYMBOL);

        assertThat(entity).isNotNull();
        assertThat(entity.id()).isNotNull();
        assertThat(entity.exchange()).isEqualTo(EXCHANGE);
        assertThat(entity.channel()).isEqualTo(CHANNEL);
        assertThat(entity.symbol()).isEqualTo(SYMBOL);
        assertThat(entity.status()).isEqualTo(Status.CONNECTING);
        assertThat(entity.errorMessage()).isNull();
        assertThat(entity.refCount()).isEqualTo(1);
        assertThat(entity.createdAt()).isNotNull();
        assertThat(entity.updatedAt()).isNotNull();
    }

    @Test
    @DisplayName("2. upsertAndIncrement: для уже активной подписки инкрементирует ref_count и сохраняет статус ACTIVE")
    void upsertAndIncrement_whenExistingActive_incrementsRefCountAndKeepsActive() {
        SubscriptionEntity saved = createAndSave(EXCHANGE, CHANNEL, SYMBOL_2, Status.ACTIVE, null, 2);

        SubscriptionEntity updated = repository.upsertAndIncrement(EXCHANGE, CHANNEL, SYMBOL_2);

        assertThat(updated.id()).isEqualTo(saved.id());
        assertThat(updated.exchange()).isEqualTo(EXCHANGE);
        assertThat(updated.channel()).isEqualTo(CHANNEL);
        assertThat(updated.symbol()).isEqualTo(SYMBOL_2);
        assertThat(updated.status()).isEqualTo(Status.ACTIVE);
        assertThat(updated.errorMessage()).isNull();
        assertThat(updated.refCount()).isEqualTo(3);
        assertThat(updated.createdAt()).isEqualTo(saved.createdAt());
        assertThat(updated.updatedAt()).isAfterOrEqualTo(saved.updatedAt());
    }

    @Test
    @DisplayName("3. upsertAndIncrement: для завершенной подписки (TERMINATED, ref_count = 0) сбрасывает в CONNECTING и ref_count = 1")
    void upsertAndIncrement_whenTerminated_resetsToConnectingAndClearsError() {
        String symbol = "TERM_SYM";
        SubscriptionEntity saved = createAndSave(EXCHANGE, CHANNEL, symbol, Status.TERMINATED, ERROR_MSG, 0);

        SubscriptionEntity updated = repository.upsertAndIncrement(EXCHANGE, CHANNEL, symbol);

        assertThat(updated.id()).isEqualTo(saved.id());
        assertThat(updated.exchange()).isEqualTo(EXCHANGE);
        assertThat(updated.channel()).isEqualTo(CHANNEL);
        assertThat(updated.symbol()).isEqualTo(symbol);
        assertThat(updated.status()).isEqualTo(Status.CONNECTING);
        assertThat(updated.errorMessage()).isNull();
        assertThat(updated.refCount()).isEqualTo(1);
        assertThat(updated.createdAt()).isEqualTo(saved.createdAt());
        assertThat(updated.updatedAt()).isAfterOrEqualTo(saved.updatedAt());
    }

    @Test
    @DisplayName("4. upsertAndIncrement: при 10 конкурентных запросах атомарно обновляет ref_count до 10 без дубликатов")
    void upsertAndIncrement_concurrentAccess_preventsRaceConditionAndMaintainsExactCount() throws InterruptedException {
        int threadsCount = 10;
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch finishLatch = new CountDownLatch(threadsCount);
        try (ExecutorService executor = Executors.newFixedThreadPool(threadsCount)) {
            for (int i = 0; i < threadsCount; i++) {
                executor.submit(() -> {
                    try {
                        startLatch.await();
                        repository.upsertAndIncrement(EXCHANGE, CHANNEL, CONCURRENT_SYMBOL);
                    } catch (InterruptedException _) {
                        Thread.currentThread().interrupt();
                    } finally {
                        finishLatch.countDown();
                    }
                });
            }

            startLatch.countDown();
            boolean completed = finishLatch.await(10, TimeUnit.SECONDS);
            assertThat(completed).isTrue();
        }

        List<SubscriptionEntity> results = repository.findAll(EXCHANGE, CHANNEL, null);
        List<SubscriptionEntity> matched = results.stream()
                .filter(e -> CONCURRENT_SYMBOL.equals(e.symbol()))
                .toList();

        assertThat(matched).hasSize(1);
        assertThat(matched.getFirst().refCount()).isEqualTo(threadsCount);
    }

    // =========================================================================
    // Группа 2: Выборка findPendingRecovery
    // =========================================================================

    @Test
    @DisplayName("5. findPendingRecovery: возвращает только подписки со статусом ACTIVE/CONNECTING и ref_count > 0")
    void findPendingRecovery_returnsActiveAndConnectingWithPositiveRefCountOnly() {
        SubscriptionEntity e1 = createAndSave(EXCHANGE, CHANNEL, "SYM_1", Status.CONNECTING, null, 1);
        SubscriptionEntity e2 = createAndSave(EXCHANGE, CHANNEL, "SYM_2", Status.ACTIVE, null, 2);
        createAndSave(EXCHANGE, CHANNEL, "SYM_3", Status.CONNECTING, null, 0); // refCount = 0
        createAndSave(EXCHANGE, CHANNEL, "SYM_4", Status.FAILED, ERROR_MSG, 1); // FAILED
        createAndSave(EXCHANGE, CHANNEL, "SYM_5", Status.TERMINATED, null, 0); // TERMINATED

        List<SubscriptionEntity> pending = repository.findPendingRecovery(List.of(Status.ACTIVE, Status.CONNECTING));

        assertThat(pending)
                .extracting(SubscriptionEntity::id)
                .containsExactlyInAnyOrder(e1.id(), e2.id());
    }

    // =========================================================================
    // Группа 3: Пессимистическая блокировка findByIdForUpdate
    // =========================================================================

    @Test
    @DisplayName("6. findByIdForUpdate: находит существующую запись по ID")
    void findByIdForUpdate_whenExists_returnsEntity() {
        SubscriptionEntity saved = createAndSave(EXCHANGE, CHANNEL, "SYM_LOCK", Status.ACTIVE, null, 1);

        Optional<SubscriptionEntity> found = repository.findByIdForUpdate(saved.id());

        assertThat(found).isPresent();
        SubscriptionEntity entity = found.get();
        assertThat(entity.id()).isEqualTo(saved.id());
        assertThat(entity.exchange()).isEqualTo(EXCHANGE);
        assertThat(entity.channel()).isEqualTo(CHANNEL);
        assertThat(entity.symbol()).isEqualTo("SYM_LOCK");
        assertThat(entity.status()).isEqualTo(Status.ACTIVE);
        assertThat(entity.errorMessage()).isNull();
        assertThat(entity.refCount()).isEqualTo(1);
    }

    @Test
    @DisplayName("7. findByIdForUpdate: возвращает empty Optional при отсутствии записи")
    void findByIdForUpdate_whenNotExists_returnsEmpty() {
        Optional<SubscriptionEntity> found = repository.findByIdForUpdate(999_999L);

        assertThat(found).isEmpty();
    }

    // =========================================================================
    // Группа 4: Динамическая фильтрация findAll
    // =========================================================================

    @Test
    @DisplayName("8. findAll: корректно фильтрует при различных комбинациях null-параметров")
    void findAll_whenFiltersProvided_filtersCorrectly() {
        SubscriptionEntity moexTrades = createAndSave("MOEX", "trades", "SYM_A", Status.ACTIVE, null, 1);
        createAndSave("TINKOFF", "trades", "SYM_B", Status.ACTIVE, null, 1);
        createAndSave("MOEX", "orderbook", "SYM_C", Status.CONNECTING, null, 1);

        List<SubscriptionEntity> all = repository.findAll(null, null, null);
        assertThat(all).extracting(SubscriptionEntity::id).contains(moexTrades.id());

        List<SubscriptionEntity> moexOnly = repository.findAll("MOEX", null, null);
        assertThat(moexOnly)
                .isNotEmpty()
                .allMatch(e -> "MOEX".equals(e.exchange()));

        List<SubscriptionEntity> matched = repository.findAll("MOEX", "trades", Status.ACTIVE);
        assertThat(matched)
                .extracting(SubscriptionEntity::id)
                .containsExactly(moexTrades.id());
    }
}
