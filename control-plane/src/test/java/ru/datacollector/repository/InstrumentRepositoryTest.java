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
import ru.datacollector.repository.entity.InstrumentEntity;

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
class InstrumentRepositoryTest {

    @Container
    @ServiceConnection
    static PostgreSQLContainer postgres = new PostgreSQLContainer("postgres:16-alpine");

    @Autowired
    private InstrumentRepository repository;

    private InstrumentEntity createEntity(String uid, String ticker, String name, boolean isTradable) {
        return new InstrumentEntity(
                uid,
                ticker,
                "TQBR",
                "share",
                name,
                10,
                "RUB",
                isTradable,
                Instant.now().truncatedTo(ChronoUnit.MICROS)
        );
    }

    @Test
    @DisplayName("1. upsert: при новом инструменте успешно вставляет строку в БД")
    void upsert_whenNewInstrument_insertsRow() {
        InstrumentEntity entity = createEntity("uid-sber-1", "SBER", "Сбербанк", true);

        repository.upsert(entity);

        Optional<InstrumentEntity> found = repository.findById("uid-sber-1");
        assertThat(found).isPresent();
        InstrumentEntity actual = found.get();
        assertThat(actual.instrumentUid()).isEqualTo("uid-sber-1");
        assertThat(actual.ticker()).isEqualTo("SBER");
        assertThat(actual.classCode()).isEqualTo("TQBR");
        assertThat(actual.instrumentType()).isEqualTo("share");
        assertThat(actual.name()).isEqualTo("Сбербанк");
        assertThat(actual.lot()).isEqualTo(10);
        assertThat(actual.currency()).isEqualTo("RUB");
        assertThat(actual.isTradable()).isTrue();
        assertThat(actual.updatedAt()).isNotNull();
    }

    @Test
    @DisplayName("2. upsert: для существующего инструмента идемпотентно обновляет поля без дублирования")
    void upsert_whenInstrumentExists_updatesFieldsIdempotently() {
        InstrumentEntity initial = createEntity("uid-sber-2", "SBER2", "Сбербанк", true);
        repository.upsert(initial);

        InstrumentEntity updated = new InstrumentEntity(
                "uid-sber-2",
                "SBER2",
                "TQBR",
                "share",
                "Сбербанк ПАО (Обновлено)",
                100,
                "RUB",
                false,
                Instant.now().plus(1, ChronoUnit.HOURS).truncatedTo(ChronoUnit.MICROS)
        );
        repository.upsert(updated);

        Optional<InstrumentEntity> found = repository.findById("uid-sber-2");
        assertThat(found).isPresent();
        InstrumentEntity actual = found.get();
        assertThat(actual.name()).isEqualTo("Сбербанк ПАО (Обновлено)");
        assertThat(actual.lot()).isEqualTo(100);
        assertThat(actual.isTradable()).isFalse();
        assertThat(actual.updatedAt()).isAfterOrEqualTo(initial.updatedAt());
    }

    @Test
    @DisplayName("3. upsertAll: пакетная вставка новых инструментов сохраняет все записи")
    void upsertAll_whenBatchOfNewInstruments_insertsAll() {
        InstrumentEntity e1 = createEntity("uid-batch-1", "TICK1", "Name 1", true);
        InstrumentEntity e2 = createEntity("uid-batch-2", "TICK2", "Name 2", true);
        InstrumentEntity e3 = createEntity("uid-batch-3", "TICK3", "Name 3", false);

        repository.upsertAll(List.of(e1, e2, e3));

        List<InstrumentEntity> all = repository.findAll();
        assertThat(all)
                .extracting(InstrumentEntity::instrumentUid)
                .contains(e1.instrumentUid(), e2.instrumentUid(), e3.instrumentUid());
    }

    @Test
    @DisplayName("4. upsertAll: батч с обновлениями и дубликатами корректно мержит данные")
    void upsertAll_whenBatchContainsDuplicatesAndUpdates_mergesCorrectly() {
        InstrumentEntity initial = createEntity("uid-merge-1", "MERGE1", "Старое имя", true);
        repository.upsert(initial);

        InstrumentEntity updateExisting = new InstrumentEntity(
                "uid-merge-1",
                "MERGE1",
                "TQBR",
                "share",
                "Новое имя",
                50,
                "RUB",
                false,
                Instant.now().plus(10, ChronoUnit.SECONDS).truncatedTo(ChronoUnit.MICROS)
        );
        InstrumentEntity newEntity = createEntity("uid-merge-2", "MERGE2", "Второй инструмент", true);

        repository.upsertAll(List.of(updateExisting, newEntity));

        Optional<InstrumentEntity> foundUpdated = repository.findById("uid-merge-1");
        assertThat(foundUpdated).isPresent();
        assertThat(foundUpdated.get().name()).isEqualTo("Новое имя");
        assertThat(foundUpdated.get().lot()).isEqualTo(50);
        assertThat(foundUpdated.get().isTradable()).isFalse();

        Optional<InstrumentEntity> foundNew = repository.findById("uid-merge-2");
        assertThat(foundNew).isPresent();
        assertThat(foundNew.get().name()).isEqualTo("Второй инструмент");
    }

    @Test
    @DisplayName("5. findByTicker: при наличии инструмента возвращает сущность")
    void findByTicker_whenExists_returnsInstrument() {
        InstrumentEntity entity = createEntity("uid-gazp-1", "GAZP", "Газпром", true);
        repository.upsert(entity);

        Optional<InstrumentEntity> found = repository.findByTicker("GAZP");

        assertThat(found).isPresent();
        assertThat(found.get().instrumentUid()).isEqualTo("uid-gazp-1");
        assertThat(found.get().ticker()).isEqualTo("GAZP");
    }

    @Test
    @DisplayName("6. findByTicker: при отсутствии инструмента возвращает пустой Optional")
    void findByTicker_whenNotExists_returnsEmpty() {
        Optional<InstrumentEntity> found = repository.findByTicker("NON_EXISTING_TICKER");

        assertThat(found).isEmpty();
    }

    @Test
    @DisplayName("7. upsert_concurrency_noDeadlocks: параллельная вставка одной записи не вызывает deadlock")
    void upsert_concurrency_noDeadlocks() throws InterruptedException {
        int threadsCount = 10;
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch finishLatch = new CountDownLatch(threadsCount);
        String concurrentUid = "uid-concurrent-1";

        try (ExecutorService executor = Executors.newFixedThreadPool(threadsCount)) {
            for (int i = 0; i < threadsCount; i++) {
                final int index = i;
                executor.submit(() -> {
                    try {
                        startLatch.await();
                        InstrumentEntity entity = new InstrumentEntity(
                                concurrentUid,
                                "CONC",
                                "TQBR",
                                "share",
                                "Конкурентное имя " + index,
                                index + 1,
                                "RUB",
                                true,
                                Instant.now()
                        );
                        repository.upsert(entity);
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

        Optional<InstrumentEntity> found = repository.findById(concurrentUid);
        assertThat(found).isPresent();
        assertThat(found.get().instrumentUid()).isEqualTo(concurrentUid);

        long countForUid = repository.findAll().stream()
                .filter(e -> concurrentUid.equals(e.instrumentUid()))
                .count();
        assertThat(countForUid).isEqualTo(1);
    }
}
