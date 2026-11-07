package ru.datacollector.service;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import ru.datacollector.dto.CreateSubscriptionRequest;
import ru.datacollector.exception.IllegalSubscriptionStateException;
import ru.datacollector.exception.SubscriptionNotFoundException;
import ru.datacollector.model.Status;
import ru.datacollector.repository.SubscriptionRepository;
import ru.datacollector.repository.entity.SubscriptionEntity;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class SubscriptionStorageServiceTest {

    private static final Long TEST_ID = 1L;
    private static final String EXCHANGE = "MOEX";
    private static final String CHANNEL = "trades";
    private static final String SYMBOL = "BBG004730N88";
    private static final String ERROR_MESSAGE = "Connection reset by peer";

    @Mock
    private SubscriptionRepository repository;

    @InjectMocks
    private SubscriptionStorageService storageService;

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

    // =========================================================================
    // Группа 1: Поиск и создание (Read & Upsert)
    // =========================================================================

    @Test
    @DisplayName("1. getOrCreate: для нового инструмента создает запись со статусом CONNECTING и refCount = 1")
    void getOrCreate_whenNew_returnsCreated() {
        var request = new CreateSubscriptionRequest(EXCHANGE, SYMBOL, CHANNEL);
        SubscriptionEntity newEntity = createEntity(Status.CONNECTING, 1, null);
        when(repository.upsertAndIncrement(EXCHANGE, CHANNEL, SYMBOL)).thenReturn(newEntity);

        SubscriptionEntity result = storageService.getOrCreate(request);

        assertThat(result).isEqualTo(newEntity);
        assertThat(result.id()).isEqualTo(newEntity.id());
        assertThat(result.exchange()).isEqualTo(EXCHANGE);
        assertThat(result.channel()).isEqualTo(CHANNEL);
        assertThat(result.symbol()).isEqualTo(SYMBOL);
        assertThat(result.refCount()).isEqualTo(1);
        assertThat(result.status()).isEqualTo(Status.CONNECTING);
        assertThat(result.errorMessage()).isNull();
        verify(repository).upsertAndIncrement(EXCHANGE, CHANNEL, SYMBOL);
        verifyNoMoreInteractions(repository);
    }

    @Test
    @DisplayName("2. getOrCreate: при наличии подписки возвращает существующую с увеличенным refCount")
    void getOrCreate_whenExists_returnsIncremented() {
        var request = new CreateSubscriptionRequest(EXCHANGE, SYMBOL, CHANNEL);
        SubscriptionEntity existingEntity = createEntity(Status.ACTIVE, 2, null);
        when(repository.upsertAndIncrement(EXCHANGE, CHANNEL, SYMBOL)).thenReturn(existingEntity);

        SubscriptionEntity result = storageService.getOrCreate(request);

        assertThat(result).isEqualTo(existingEntity);
        assertThat(result.id()).isEqualTo(existingEntity.id());
        assertThat(result.exchange()).isEqualTo(EXCHANGE);
        assertThat(result.channel()).isEqualTo(CHANNEL);
        assertThat(result.symbol()).isEqualTo(SYMBOL);
        assertThat(result.refCount()).isEqualTo(2);
        assertThat(result.status()).isEqualTo(Status.ACTIVE);
        assertThat(result.errorMessage()).isNull();
        verify(repository).upsertAndIncrement(EXCHANGE, CHANNEL, SYMBOL);
        verifyNoMoreInteractions(repository);
    }

    @Test
    @DisplayName("3. findById: возвращает сущность, если она найдена в БД")
    void findById_whenFound_returnsEntity() {
        SubscriptionEntity entity = createEntity(Status.ACTIVE, 1, null);
        when(repository.findById(TEST_ID)).thenReturn(Optional.of(entity));

        SubscriptionEntity result = storageService.findById(TEST_ID);

        assertThat(result).isEqualTo(entity);
        verify(repository).findById(TEST_ID);
        verifyNoMoreInteractions(repository);
    }

    @Test
    @DisplayName("4. findById: выбрасывает SubscriptionNotFoundException, если подписка отсутствует в БД")
    void findById_whenNotFound_throwsSubscriptionNotFoundException() {
        when(repository.findById(TEST_ID)).thenReturn(Optional.empty());

        assertThatThrownBy(() -> storageService.findById(TEST_ID))
                .isInstanceOf(SubscriptionNotFoundException.class)
                .hasMessage("Подписка с идентификатором " + TEST_ID + " не найдена");
        verify(repository).findById(TEST_ID);
        verifyNoMoreInteractions(repository);
    }

    @Test
    @DisplayName("5. findAll: корректно делегирует фильтры поиска в репозиторий")
    void findAll_shouldDelegateToRepository() {
        List<SubscriptionEntity> expectedList = List.of(createEntity(Status.ACTIVE, 1, null));
        when(repository.findAll(EXCHANGE, CHANNEL, Status.ACTIVE)).thenReturn(expectedList);

        List<SubscriptionEntity> result = storageService.findAll(EXCHANGE, CHANNEL, Status.ACTIVE);

        assertThat(result).isEqualTo(expectedList);
        verify(repository).findAll(EXCHANGE, CHANNEL, Status.ACTIVE);
        verifyNoMoreInteractions(repository);
    }

    @Test
    @DisplayName("6. findPendingRecovery: запрашивает у репозитория строго статусы ACTIVE и CONNECTING")
    void findPendingRecovery_shouldRequestActiveAndConnecting() {

        List<SubscriptionEntity> pending = List.of(
                createEntity(Status.CONNECTING, 1, null),
                createEntity(Status.ACTIVE, 5, null)
        );
        when(repository.findPendingRecovery(any())).thenReturn(pending);

        List<SubscriptionEntity> result = storageService.findPendingRecovery();
        assertThat(result).isEqualTo(pending);

        verify(repository).findPendingRecovery(List.of(Status.ACTIVE, Status.CONNECTING));
        verifyNoMoreInteractions(repository);
    }

    // =========================================================================
    // Группа 2: Переходы состояний (markActive и markFailed)
    // =========================================================================

    @Test
    @DisplayName("7. markActive: переводит подписку из CONNECTING в ACTIVE и сохраняет в БД")
    void markActive_whenConnecting_shouldTransitionToActive() {
        SubscriptionEntity entity = createEntity(Status.CONNECTING, 1, ERROR_MESSAGE);
        when(repository.findByIdForUpdate(TEST_ID)).thenReturn(Optional.of(entity));
        when(repository.save(any(SubscriptionEntity.class))).thenAnswer(inv -> inv.getArgument(0));

        SubscriptionEntity result = storageService.markActive(TEST_ID);

        assertThat(result.id()).isEqualTo(TEST_ID);
        assertThat(result.exchange()).isEqualTo(EXCHANGE);
        assertThat(result.channel()).isEqualTo(CHANNEL);
        assertThat(result.symbol()).isEqualTo(SYMBOL);
        assertThat(result.status()).isEqualTo(Status.ACTIVE);
        assertThat(result.errorMessage()).isNull();
        assertThat(result.refCount()).isEqualTo(1);
        verify(repository).findByIdForUpdate(TEST_ID);
        verify(repository).save(argThat(saved -> saved.status() == Status.ACTIVE && saved.errorMessage() == null));
        verifyNoMoreInteractions(repository);
    }

    @Test
    @DisplayName("8. markActive: игнорирует перевод в ACTIVE, если подписка не в статусе CONNECTING")
    void markActive_whenNotConnecting_shouldIgnoreTransition() {
        SubscriptionEntity entity = createEntity(Status.FAILED, 1, ERROR_MESSAGE);
        when(repository.findByIdForUpdate(TEST_ID)).thenReturn(Optional.of(entity));
        when(repository.save(any(SubscriptionEntity.class))).thenAnswer(inv -> inv.getArgument(0));

        SubscriptionEntity result = storageService.markActive(TEST_ID);

        assertThat(result.id()).isEqualTo(TEST_ID);
        assertThat(result.exchange()).isEqualTo(EXCHANGE);
        assertThat(result.channel()).isEqualTo(CHANNEL);
        assertThat(result.symbol()).isEqualTo(SYMBOL);
        assertThat(result.status()).isEqualTo(Status.FAILED);
        assertThat(result.errorMessage()).isEqualTo(ERROR_MESSAGE);
        assertThat(result.refCount()).isEqualTo(1);
        verify(repository).findByIdForUpdate(TEST_ID);
        verify(repository).save(entity);
        verifyNoMoreInteractions(repository);
    }

    @Test
    @DisplayName("9. markFailed: переводит подписку из CONNECTING в FAILED с фиксацией текста ошибки")
    void markFailed_whenConnecting_shouldTransitionToFailed() {
        SubscriptionEntity entity = createEntity(Status.CONNECTING, 1, null);
        when(repository.findByIdForUpdate(TEST_ID)).thenReturn(Optional.of(entity));
        when(repository.save(any(SubscriptionEntity.class))).thenAnswer(inv -> inv.getArgument(0));

        SubscriptionEntity result = storageService.markFailed(TEST_ID, ERROR_MESSAGE);

        assertThat(result.id()).isEqualTo(TEST_ID);
        assertThat(result.exchange()).isEqualTo(EXCHANGE);
        assertThat(result.channel()).isEqualTo(CHANNEL);
        assertThat(result.symbol()).isEqualTo(SYMBOL);
        assertThat(result.status()).isEqualTo(Status.FAILED);
        assertThat(result.errorMessage()).isEqualTo(ERROR_MESSAGE);
        assertThat(result.refCount()).isEqualTo(1);
        verify(repository).findByIdForUpdate(TEST_ID);
        verify(repository).save(argThat(saved -> saved.status() == Status.FAILED && ERROR_MESSAGE.equals(saved.errorMessage())));
        verifyNoMoreInteractions(repository);
    }

    @Test
    @DisplayName("10. markFailed: переводит живой стрим из ACTIVE в FAILED при разрыве в процессе работы")
    void markFailed_whenActive_shouldTransitionToFailed() {
        SubscriptionEntity entity = createEntity(Status.ACTIVE, 1, null);
        when(repository.findByIdForUpdate(TEST_ID)).thenReturn(Optional.of(entity));
        when(repository.save(any(SubscriptionEntity.class))).thenAnswer(inv -> inv.getArgument(0));

        SubscriptionEntity result = storageService.markFailed(TEST_ID, ERROR_MESSAGE);

        assertThat(result.id()).isEqualTo(TEST_ID);
        assertThat(result.exchange()).isEqualTo(EXCHANGE);
        assertThat(result.channel()).isEqualTo(CHANNEL);
        assertThat(result.symbol()).isEqualTo(SYMBOL);
        assertThat(result.status()).isEqualTo(Status.FAILED);
        assertThat(result.errorMessage()).isEqualTo(ERROR_MESSAGE);
        assertThat(result.refCount()).isEqualTo(1);
        verify(repository).findByIdForUpdate(TEST_ID);
        verify(repository).save(argThat(saved -> saved.status() == Status.FAILED && ERROR_MESSAGE.equals(saved.errorMessage())));
        verifyNoMoreInteractions(repository);
    }

    @Test
    @DisplayName("11. markFailed: игнорирует ошибку для уже завершенной подписки TERMINATED")
    void markFailed_whenTerminated_shouldIgnoreTransition() {
        SubscriptionEntity entity = createEntity(Status.TERMINATED, 0, null);
        when(repository.findByIdForUpdate(TEST_ID)).thenReturn(Optional.of(entity));
        when(repository.save(any(SubscriptionEntity.class))).thenAnswer(inv -> inv.getArgument(0));

        SubscriptionEntity result = storageService.markFailed(TEST_ID, ERROR_MESSAGE);

        assertThat(result.id()).isEqualTo(TEST_ID);
        assertThat(result.exchange()).isEqualTo(EXCHANGE);
        assertThat(result.channel()).isEqualTo(CHANNEL);
        assertThat(result.symbol()).isEqualTo(SYMBOL);
        assertThat(result.status()).isEqualTo(Status.TERMINATED);
        assertThat(result.errorMessage()).isNull();
        assertThat(result.refCount()).isZero();
        verify(repository).findByIdForUpdate(TEST_ID);
        verify(repository).save(entity);
        verifyNoMoreInteractions(repository);
    }

    // =========================================================================
    // Группа 3: Защита рестарта (markRestarting)
    // =========================================================================

    @Test
    @DisplayName("12. markRestarting: переводит сбойную подписку FAILED в CONNECTING и сбрасывает ошибку")
    void markRestarting_whenFailed_shouldTransitionToConnecting() {
        SubscriptionEntity entity = createEntity(Status.FAILED, 1, ERROR_MESSAGE);
        when(repository.findByIdForUpdate(TEST_ID)).thenReturn(Optional.of(entity));
        when(repository.save(any(SubscriptionEntity.class))).thenAnswer(inv -> inv.getArgument(0));

        SubscriptionEntity result = storageService.markRestarting(TEST_ID);

        assertThat(result.id()).isEqualTo(TEST_ID);
        assertThat(result.exchange()).isEqualTo(EXCHANGE);
        assertThat(result.channel()).isEqualTo(CHANNEL);
        assertThat(result.symbol()).isEqualTo(SYMBOL);
        assertThat(result.status()).isEqualTo(Status.CONNECTING);
        assertThat(result.errorMessage()).isNull();
        assertThat(result.refCount()).isEqualTo(1);
        verify(repository).findByIdForUpdate(TEST_ID);
        verify(repository).save(argThat(saved -> saved.status() == Status.CONNECTING && saved.errorMessage() == null));
        verifyNoMoreInteractions(repository);
    }

    @Test
    @DisplayName("13. markRestarting: блокирует перезапуск подписки со статусом ACTIVE с ошибкой IllegalSubscriptionStateException")
    void markRestarting_whenActive_shouldThrowIllegalSubscriptionStateException() {
        SubscriptionEntity entity = createEntity(Status.ACTIVE, 1, null);
        when(repository.findByIdForUpdate(TEST_ID)).thenReturn(Optional.of(entity));

        assertThatThrownBy(() -> storageService.markRestarting(TEST_ID))
                .isInstanceOf(IllegalSubscriptionStateException.class)
                .hasMessage("Перезапуск возможен только для подписок в статусе FAILED. Текущий статус: ACTIVE");

        verify(repository).findByIdForUpdate(TEST_ID);
        verify(repository, never()).save(any());
        verifyNoMoreInteractions(repository);
    }

    @Test
    @DisplayName("14. markRestarting: блокирует перезапуск завершенной подписки TERMINATED")
    void markRestarting_whenTerminated_shouldThrowIllegalSubscriptionStateException() {
        SubscriptionEntity entity = createEntity(Status.TERMINATED, 0, null);
        when(repository.findByIdForUpdate(TEST_ID)).thenReturn(Optional.of(entity));

        assertThatThrownBy(() -> storageService.markRestarting(TEST_ID))
                .isInstanceOf(IllegalSubscriptionStateException.class)
                .hasMessage("Перезапуск возможен только для подписок в статусе FAILED. Текущий статус: TERMINATED");

        verify(repository).findByIdForUpdate(TEST_ID);
        verify(repository, never()).save(any());
        verifyNoMoreInteractions(repository);
    }

    // =========================================================================
    // Группа 4: Управление счетчиком ссылок (decrementRefCount) и обработка 404
    // =========================================================================

    @Test
    @DisplayName("15. decrementRefCount: уменьшает счетчик на 1 и сохраняет статус ACTIVE, если refCount > 1")
    void decrementRefCount_whenMultipleRefs_shouldDecrementAndKeepStatus() {
        SubscriptionEntity entity = createEntity(Status.ACTIVE, 3, null);
        when(repository.findByIdForUpdate(TEST_ID)).thenReturn(Optional.of(entity));
        when(repository.save(any(SubscriptionEntity.class))).thenAnswer(inv -> inv.getArgument(0));

        SubscriptionEntity result = storageService.decrementRefCount(TEST_ID);

        assertThat(result.id()).isEqualTo(TEST_ID);
        assertThat(result.exchange()).isEqualTo(EXCHANGE);
        assertThat(result.channel()).isEqualTo(CHANNEL);
        assertThat(result.symbol()).isEqualTo(SYMBOL);
        assertThat(result.refCount()).isEqualTo(2);
        assertThat(result.status()).isEqualTo(Status.ACTIVE);
        assertThat(result.errorMessage()).isNull();
        verify(repository).findByIdForUpdate(TEST_ID);
        verify(repository).save(argThat(saved -> saved.refCount() == 2 && saved.status() == Status.ACTIVE));
        verifyNoMoreInteractions(repository);
    }

    @Test
    @DisplayName("16. decrementRefCount: при уменьшении последнего клиента (refCount = 1) переводит в TERMINATED")
    void decrementRefCount_whenLastRef_shouldTerminate() {
        SubscriptionEntity entity = createEntity(Status.ACTIVE, 1, null);
        when(repository.findByIdForUpdate(TEST_ID)).thenReturn(Optional.of(entity));
        when(repository.save(any(SubscriptionEntity.class))).thenAnswer(inv -> inv.getArgument(0));

        SubscriptionEntity result = storageService.decrementRefCount(TEST_ID);

        assertThat(result.id()).isEqualTo(TEST_ID);
        assertThat(result.exchange()).isEqualTo(EXCHANGE);
        assertThat(result.channel()).isEqualTo(CHANNEL);
        assertThat(result.symbol()).isEqualTo(SYMBOL);
        assertThat(result.refCount()).isZero();
        assertThat(result.status()).isEqualTo(Status.TERMINATED);
        assertThat(result.errorMessage()).isNull();
        verify(repository).findByIdForUpdate(TEST_ID);
        verify(repository).save(argThat(saved -> saved.refCount() == 0 && saved.status() == Status.TERMINATED));
        verifyNoMoreInteractions(repository);
    }

    @Test
    @DisplayName("17. decrementRefCount: запрещает декремент для уже завершенной подписки TERMINATED")
    void decrementRefCount_whenAlreadyTerminated_shouldThrowIllegalSubscriptionStateException() {
        SubscriptionEntity entity = createEntity(Status.TERMINATED, 0, null);
        when(repository.findByIdForUpdate(TEST_ID)).thenReturn(Optional.of(entity));

        assertThatThrownBy(() -> storageService.decrementRefCount(TEST_ID))
                .isInstanceOf(IllegalSubscriptionStateException.class)
                .hasMessage("Нельзя уменьшить счетчик: подписка " + TEST_ID + " уже завершена");

        verify(repository).findByIdForUpdate(TEST_ID);
        verify(repository, never()).save(any());
        verifyNoMoreInteractions(repository);
    }

    @Test
    @DisplayName("18. update: выбрасывает SubscriptionNotFoundException, если подписка не найдена в БД при мутации")
    void update_whenSubscriptionNotFound_shouldThrowSubscriptionNotFoundException() {
        when(repository.findByIdForUpdate(TEST_ID)).thenReturn(Optional.empty());

        assertThatThrownBy(() -> storageService.markActive(TEST_ID))
                .isInstanceOf(SubscriptionNotFoundException.class)
                .hasMessage("Подписка с идентификатором " + TEST_ID + " не найдена");

        verify(repository).findByIdForUpdate(TEST_ID);
        verify(repository, never()).save(any());
        verifyNoMoreInteractions(repository);
    }

    // =========================================================================
    // Группа 5: Дополнительные граничные случаи
    // =========================================================================

    @Test
    @DisplayName("19. markRestarting: блокирует перезапуск подписки со статусом CONNECTING")
    void markRestarting_whenConnecting_shouldThrowIllegalSubscriptionStateException() {
        SubscriptionEntity entity = createEntity(Status.CONNECTING, 1, null);
        when(repository.findByIdForUpdate(TEST_ID)).thenReturn(Optional.of(entity));

        assertThatThrownBy(() -> storageService.markRestarting(TEST_ID))
                .isInstanceOf(IllegalSubscriptionStateException.class)
                .hasMessage("Перезапуск возможен только для подписок в статусе FAILED. Текущий статус: CONNECTING");

        verify(repository).findByIdForUpdate(TEST_ID);
        verify(repository, never()).save(any());
        verifyNoMoreInteractions(repository);
    }

    @Test
    @DisplayName("20. decrementRefCount: запрещает декремент при refCount <= 0 даже если статус не TERMINATED")
    void decrementRefCount_whenRefCountZero_shouldThrowIllegalSubscriptionStateException() {
        SubscriptionEntity entity = createEntity(Status.FAILED, 0, ERROR_MESSAGE);
        when(repository.findByIdForUpdate(TEST_ID)).thenReturn(Optional.of(entity));

        assertThatThrownBy(() -> storageService.decrementRefCount(TEST_ID))
                .isInstanceOf(IllegalSubscriptionStateException.class)
                .hasMessage("Нельзя уменьшить счетчик: подписка " + TEST_ID + " уже завершена");

        verify(repository).findByIdForUpdate(TEST_ID);
        verify(repository, never()).save(any());
        verifyNoMoreInteractions(repository);
    }

    @Test
    @DisplayName("21. markFailed: игнорирует перевод в FAILED, если подписка уже находится в статусе FAILED")
    void markFailed_whenAlreadyFailed_shouldIgnoreTransition() {
        SubscriptionEntity entity = createEntity(Status.FAILED, 1, "Initial error");
        when(repository.findByIdForUpdate(TEST_ID)).thenReturn(Optional.of(entity));
        when(repository.save(any(SubscriptionEntity.class))).thenAnswer(inv -> inv.getArgument(0));

        SubscriptionEntity result = storageService.markFailed(TEST_ID, "New error");

        assertThat(result.id()).isEqualTo(TEST_ID);
        assertThat(result.status()).isEqualTo(Status.FAILED);
        assertThat(result.errorMessage()).isEqualTo("Initial error");
        verify(repository).findByIdForUpdate(TEST_ID);
        verify(repository).save(entity);
        verifyNoMoreInteractions(repository);
    }
}
