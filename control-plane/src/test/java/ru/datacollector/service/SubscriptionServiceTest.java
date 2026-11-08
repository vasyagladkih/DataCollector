package ru.datacollector.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import ru.datacollector.dto.CreateSubscriptionRequest;
import ru.datacollector.dto.SubscriptionResponse;
import ru.datacollector.exception.IllegalSubscriptionStateException;
import ru.datacollector.exception.SymbolNotFoundException;
import ru.datacollector.mapper.SubscriptionMapper;
import ru.datacollector.model.Status;
import ru.datacollector.repository.entity.InstrumentEntity;
import ru.datacollector.repository.entity.SubscriptionEntity;
import ru.datacollector.streaming.DataCollectorMarketDataStreamManager;
import ru.tinkoff.piapi.contract.v1.SubscribeTradesResponse;
import ru.tinkoff.piapi.contract.v1.Trade;
import ru.tinkoff.piapi.contract.v1.TradeSourceType;
import ru.tinkoff.piapi.contract.v1.TradeSubscription;
import ru.ttech.piapi.core.connector.streaming.listeners.OnNextListener;
import ru.ttech.piapi.core.impl.marketdata.MarketDataResponseType;
import ru.ttech.piapi.core.impl.marketdata.subscription.Instrument;
import ru.ttech.piapi.core.impl.marketdata.subscription.MarketDataSubscriptionResult;
import ru.ttech.piapi.core.impl.marketdata.subscription.RequestAction;
import ru.ttech.piapi.core.impl.marketdata.subscription.SubscriptionResultMapper;
import ru.ttech.piapi.core.impl.marketdata.subscription.SubscriptionStatus;
import ru.ttech.piapi.core.impl.marketdata.wrapper.OpenInterestWrapper;
import ru.ttech.piapi.core.impl.marketdata.wrapper.TradeWrapper;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class SubscriptionServiceTest {

    private static final Long TEST_ID = 1L;
    private static final Long TEST_ID_2 = 2L;
    private static final String EXCHANGE = "MOEX";
    private static final String CHANNEL = "trades";
    private static final String SYMBOL = "BBG004730N88";
    private static final String SYMBOL_2 = "SBER";
    private static final Instrument INSTRUMENT = new Instrument(SYMBOL);
    private static final Instrument INSTRUMENT_2 = new Instrument(SYMBOL_2);
    private static final Instant TEST_TIMESTAMP = Instant.parse("2026-01-01T10:00:00Z");
    private static final String ERROR_MESSAGE = "Network timeout";
    private static final String REJECTED_BY_EXCHANGE = "Subscription rejected by exchange";

    @Mock
    private SubscriptionStorageService storageService;

    @Mock
    private SubscriptionMapper mapper;

    @Mock
    private DataCollectorMarketDataStreamManager marketDataStreamManager;

    @Mock
    private TinvestService tinvestService;

    @InjectMocks
    private SubscriptionService subscriptionService;

    @BeforeEach
    void setUp() {
        lenient().when(tinvestService.resolve(any())).thenAnswer(invocation -> {
            String symbol = invocation.getArgument(0);
            return new InstrumentEntity(
                    symbol,
                    symbol,
                    "TQBR",
                    "share",
                    "Test",
                    1,
                    "RUB",
                    true,
                    TEST_TIMESTAMP
            );
        });
    }

    private SubscriptionEntity createEntity(Status status, int refCount) {
        return new SubscriptionEntity(
                TEST_ID,
                EXCHANGE,
                CHANNEL,
                SYMBOL,
                status,
                null,
                TEST_TIMESTAMP,
                TEST_TIMESTAMP,
                refCount
        );
    }

    private SubscriptionResponse createResponse(Status status) {
        return new SubscriptionResponse(
                TEST_ID,
                EXCHANGE,
                CHANNEL,
                SYMBOL,
                status.name(),
                null,
                TEST_TIMESTAMP,
                TEST_TIMESTAMP
        );
    }

    // =========================================================================
    // Группа 1: Создание подписки (subscribe)
    // =========================================================================

    @Test
    @DisplayName("1. subscribe: для первого клиента (refCount = 1) запускает сетевой стрим к бирже")
    void subscribe_whenFirstClient_startsStreamSubscription() {
        var request = new CreateSubscriptionRequest(EXCHANGE, SYMBOL, CHANNEL);
        SubscriptionEntity entity = createEntity(Status.CONNECTING, 1);
        SubscriptionResponse expectedResponse = createResponse(Status.CONNECTING);

        when(storageService.getOrCreate(request)).thenReturn(entity);
        when(mapper.toDto(entity)).thenReturn(expectedResponse);
        when(marketDataStreamManager.subscribeTradeAsync(any(), any(), anyBoolean(), any(), any()))
                .thenReturn(new CompletableFuture<>());

        SubscriptionResponse actual = subscriptionService.subscribe(request);

        assertThat(actual).isEqualTo(expectedResponse);
        verify(storageService).getOrCreate(request);
        verify(mapper).toDto(entity);
        verify(marketDataStreamManager).subscribeTradeAsync(
                eq(INSTRUMENT),
                eq(TradeSourceType.TRADE_SOURCE_ALL),
                eq(false),
                any(),
                any()
        );
        verifyNoMoreInteractions(storageService, mapper, marketDataStreamManager);
    }

    @Test
    @DisplayName("2. subscribe: для повторного клиента (refCount > 1) не запускает стрим повторно")
    void subscribe_whenSubsequentClient_doesNotStartStreamSubscription() {
        var request = new CreateSubscriptionRequest(EXCHANGE, SYMBOL, CHANNEL);
        SubscriptionEntity entity = createEntity(Status.ACTIVE, 2);
        SubscriptionResponse expectedResponse = createResponse(Status.ACTIVE);

        when(storageService.getOrCreate(request)).thenReturn(entity);
        when(mapper.toDto(entity)).thenReturn(expectedResponse);

        SubscriptionResponse actual = subscriptionService.subscribe(request);

        assertThat(actual).isEqualTo(expectedResponse);
        verify(storageService).getOrCreate(request);
        verify(mapper).toDto(entity);
        verify(marketDataStreamManager, never()).subscribeTradeAsync(any(), any(), anyBoolean(), any(), any());
        verifyNoMoreInteractions(storageService, mapper, marketDataStreamManager);
    }

    @Test
    @DisplayName("3. subscribe: при успешном подтверждении биржей (OK) переводит подписку в ACTIVE")
    void subscribe_whenStreamSucceeds_marksActiveInStorage() {
        var request = new CreateSubscriptionRequest(EXCHANGE, SYMBOL, CHANNEL);
        SubscriptionEntity entity = createEntity(Status.CONNECTING, 1);

        when(storageService.getOrCreate(request)).thenReturn(entity);
        when(marketDataStreamManager.subscribeTradeAsync(any(), any(), anyBoolean(), any(), any()))
                .thenReturn(CompletableFuture.completedFuture(SubscriptionStatus.OK));

        subscriptionService.subscribe(request);

        verify(storageService).getOrCreate(request);
        verify(storageService).markActive(TEST_ID);
        verify(storageService, never()).markFailed(any(), any());
        verify(mapper).toDto(entity);
        verify(marketDataStreamManager).subscribeTradeAsync(
                eq(INSTRUMENT),
                eq(TradeSourceType.TRADE_SOURCE_ALL),
                eq(false),
                any(),
                any()
        );
        verifyNoMoreInteractions(storageService, mapper, marketDataStreamManager);
    }

    @Test
    @DisplayName("4. subscribe: при отклонении подписки биржей переводит подписку в FAILED")
    void subscribe_whenStreamRejectedByExchange_marksFailedInStorage() {
        var request = new CreateSubscriptionRequest(EXCHANGE, SYMBOL, CHANNEL);
        SubscriptionEntity entity = createEntity(Status.CONNECTING, 1);

        when(storageService.getOrCreate(request)).thenReturn(entity);
        when(marketDataStreamManager.subscribeTradeAsync(any(), any(), anyBoolean(), any(), any()))
                .thenReturn(CompletableFuture.completedFuture(SubscriptionStatus.ERROR));

        subscriptionService.subscribe(request);

        verify(storageService).getOrCreate(request);
        verify(storageService).markFailed(TEST_ID, REJECTED_BY_EXCHANGE);
        verify(storageService, never()).markActive(any());
        verify(mapper).toDto(entity);
        verify(marketDataStreamManager).subscribeTradeAsync(
                eq(INSTRUMENT),
                eq(TradeSourceType.TRADE_SOURCE_ALL),
                eq(false),
                any(),
                any()
        );
        verifyNoMoreInteractions(storageService, mapper, marketDataStreamManager);
    }

    @Test
    @DisplayName("5. subscribe: при сетевом сбое gRPC-стрима переводит подписку в FAILED")
    void subscribe_whenStreamCompletesExceptionally_marksFailedInStorage() {
        var request = new CreateSubscriptionRequest(EXCHANGE, SYMBOL, CHANNEL);
        SubscriptionEntity entity = createEntity(Status.CONNECTING, 1);

        when(storageService.getOrCreate(request)).thenReturn(entity);
        when(marketDataStreamManager.subscribeTradeAsync(any(), any(), anyBoolean(), any(), any()))
                .thenReturn(CompletableFuture.failedFuture(new RuntimeException(ERROR_MESSAGE)));

        subscriptionService.subscribe(request);

        verify(storageService).getOrCreate(request);
        verify(storageService).markFailed(TEST_ID, ERROR_MESSAGE);
        verify(storageService, never()).markActive(any());
        verify(mapper).toDto(entity);
        verify(marketDataStreamManager).subscribeTradeAsync(
                eq(INSTRUMENT),
                eq(TradeSourceType.TRADE_SOURCE_ALL),
                eq(false),
                any(),
                any()
        );
        verifyNoMoreInteractions(storageService, mapper, marketDataStreamManager);
    }

    // =========================================================================
    // Группа 2: Отписка (unsubscribe)
    // =========================================================================

    @Test
    @DisplayName("6. unsubscribe: если остаются другие клиенты (refCount > 0), стрим в SDK не закрывается")
    void unsubscribe_whenRemainingRefs_doesNotUnsubscribeFromManager() {
        SubscriptionEntity entity = createEntity(Status.ACTIVE, 1);
        SubscriptionResponse expectedResponse = createResponse(Status.ACTIVE);

        when(storageService.decrementRefCount(TEST_ID)).thenReturn(entity);
        when(mapper.toDto(entity)).thenReturn(expectedResponse);

        SubscriptionResponse actual = subscriptionService.unsubscribe(TEST_ID);

        assertThat(actual).isEqualTo(expectedResponse);
        verify(storageService).decrementRefCount(TEST_ID);
        verify(mapper).toDto(entity);
        verify(marketDataStreamManager, never()).unsubscribeTrades(any(), anyBoolean(), any());
        verifyNoMoreInteractions(storageService, mapper, marketDataStreamManager);
    }

    @Test
    @DisplayName("7. unsubscribe: при уходе последнего клиента (refCount <= 0) закрывает физический стрим")
    void unsubscribe_whenLastRef_unsubscribesFromManager() {
        SubscriptionEntity entity = createEntity(Status.TERMINATED, 0);
        SubscriptionResponse expectedResponse = createResponse(Status.TERMINATED);

        when(storageService.decrementRefCount(TEST_ID)).thenReturn(entity);
        when(mapper.toDto(entity)).thenReturn(expectedResponse);

        SubscriptionResponse actual = subscriptionService.unsubscribe(TEST_ID);

        assertThat(actual).isEqualTo(expectedResponse);
        verify(storageService).decrementRefCount(TEST_ID);
        verify(mapper).toDto(entity);
        verify(marketDataStreamManager).unsubscribeTrades(
                Set.of(INSTRUMENT),
                false,
                TradeSourceType.TRADE_SOURCE_ALL
        );
        verifyNoMoreInteractions(storageService, mapper, marketDataStreamManager);
    }

    // =========================================================================
    // Группа 3: Перезапуск (restart)
    // =========================================================================

    @Test
    @DisplayName("8. restart: для сбойной подписки переводит статус в хранилище и запускает стрим")
    void restart_whenValid_restartsStreamSubscription() {
        SubscriptionEntity entity = createEntity(Status.CONNECTING, 1);
        SubscriptionResponse expectedResponse = createResponse(Status.CONNECTING);

        when(storageService.markRestarting(TEST_ID)).thenReturn(entity);
        when(mapper.toDto(entity)).thenReturn(expectedResponse);
        when(marketDataStreamManager.subscribeTradeAsync(any(), any(), anyBoolean(), any(), any()))
                .thenReturn(new CompletableFuture<>());

        SubscriptionResponse actual = subscriptionService.restart(TEST_ID);

        assertThat(actual).isEqualTo(expectedResponse);
        verify(storageService).markRestarting(TEST_ID);
        verify(mapper).toDto(entity);
        verify(marketDataStreamManager).subscribeTradeAsync(
                eq(INSTRUMENT),
                eq(TradeSourceType.TRADE_SOURCE_ALL),
                eq(false),
                any(),
                any()
        );
        verifyNoMoreInteractions(storageService, mapper, marketDataStreamManager);
    }

    @Test
    @DisplayName("9. restart: если хранилище запрещает перезапуск (IllegalSubscriptionStateException), стрим не запускается")
    void restart_whenStorageRejects_doesNotStartStream() {
        when(storageService.markRestarting(TEST_ID))
                .thenThrow(new IllegalSubscriptionStateException("Cannot restart ACTIVE subscription"));

        assertThatThrownBy(() -> subscriptionService.restart(TEST_ID))
                .isInstanceOf(IllegalSubscriptionStateException.class)
                .hasMessage("Cannot restart ACTIVE subscription");

        verify(storageService).markRestarting(TEST_ID);
        verify(marketDataStreamManager, never()).subscribeTradeAsync(any(), any(), anyBoolean(), any(), any());
        verifyNoMoreInteractions(storageService, mapper, marketDataStreamManager);
    }

    // =========================================================================
    // Группа 4: Пакетное восстановление стримов (recover)
    // =========================================================================

    @Test
    @DisplayName("10. recover: при пустом списке подписок не производит сетевых вызовов")
    void recover_whenEmptyList_doesNothing() {
        List<SubscriptionEntity> emptyList = List.of();

        subscriptionService.recover(emptyList);

        verify(marketDataStreamManager, never()).subscribeTradesBatchAsync(any(), any(), anyBoolean(), any(), any());
        verifyNoMoreInteractions(storageService, mapper, marketDataStreamManager);
    }

    @Test
    @DisplayName("11. recover: отфильтровывает инструменты, которые уже есть в стриме")
    void recover_whenAllAlreadySubscribed_doesNotCallManager() {
        SubscriptionEntity entity = createEntity(Status.ACTIVE, 1);
        when(marketDataStreamManager.isSubscribedTrades(INSTRUMENT)).thenReturn(true);

        subscriptionService.recover(List.of(entity));

        verify(marketDataStreamManager).isSubscribedTrades(INSTRUMENT);
        verify(marketDataStreamManager, never()).subscribeTradesBatchAsync(any(), any(), anyBoolean(), any(), any());
        verifyNoMoreInteractions(storageService, mapper, marketDataStreamManager);
    }

    @Test
    @DisplayName("12. recover: при успешной батчевой подписке переводит все инструменты в ACTIVE")
    void recover_whenBatchSubscribedSuccessfully_marksAllActive() {
        SubscriptionEntity entity1 = createEntity(Status.CONNECTING, 1);
        SubscriptionEntity entity2 = new SubscriptionEntity(
                TEST_ID_2, EXCHANGE, CHANNEL, SYMBOL_2, Status.CONNECTING, null, TEST_TIMESTAMP, TEST_TIMESTAMP, 1
        );

        when(marketDataStreamManager.isSubscribedTrades(any())).thenReturn(false);

        var realisticResult = SubscriptionResultMapper.map(SubscribeTradesResponse.newBuilder()
                .addTradeSubscriptions(TradeSubscription.newBuilder()
                        .setFigi(SYMBOL)
                        .setInstrumentUid("test-uid-1")
                        .setTicker("SBER")
                        .setClassCode("TQBR")
                        .setSubscriptionStatus(ru.tinkoff.piapi.contract.v1.SubscriptionStatus.SUBSCRIPTION_STATUS_SUCCESS)
                        .build())
                .addTradeSubscriptions(TradeSubscription.newBuilder()
                        .setFigi(SYMBOL_2)
                        .setInstrumentUid("test-uid-2")
                        .setTicker("GAZP")
                        .setClassCode("TQBR")
                        .setSubscriptionStatus(ru.tinkoff.piapi.contract.v1.SubscriptionStatus.SUBSCRIPTION_STATUS_SUCCESS)
                        .build())
                .build());
        when(marketDataStreamManager.subscribeTradesBatchAsync(any(), any(), anyBoolean(), any(), any()))
                .thenReturn(CompletableFuture.completedFuture(realisticResult));

        subscriptionService.recover(List.of(entity1, entity2));

        verify(storageService).markActive(TEST_ID);
        verify(storageService).markActive(TEST_ID_2);
        verify(storageService, never()).markFailed(any(), any());
        verifyNoMoreInteractions(storageService, marketDataStreamManager);
    }

    @Test
    @DisplayName("13. recover: при частичном отказе биржи переводит каждый инструмент в соответствующий статус")
    void recover_whenPartialFailureInBatch_marksEachAccurately() {
        SubscriptionEntity entity1 = createEntity(Status.CONNECTING, 1);
        SubscriptionEntity entity2 = new SubscriptionEntity(
                TEST_ID_2, EXCHANGE, CHANNEL, SYMBOL_2, Status.CONNECTING, null, TEST_TIMESTAMP, TEST_TIMESTAMP, 1
        );

        when(marketDataStreamManager.isSubscribedTrades(any())).thenReturn(false);

        var mockResult = new MarketDataSubscriptionResult(
                RequestAction.SUBSCRIBE,
                MarketDataResponseType.TRADE,
                Map.of(
                        INSTRUMENT, SubscriptionStatus.OK,
                        INSTRUMENT_2, SubscriptionStatus.ERROR
                )
        );
        when(marketDataStreamManager.subscribeTradesBatchAsync(any(), any(), anyBoolean(), any(), any()))
                .thenReturn(CompletableFuture.completedFuture(mockResult));

        subscriptionService.recover(List.of(entity1, entity2));

        verify(storageService).markActive(TEST_ID);
        verify(storageService).markFailed(TEST_ID_2, REJECTED_BY_EXCHANGE);
        verifyNoMoreInteractions(storageService, marketDataStreamManager);
    }

    @Test
    @DisplayName("14. recover: при сетевом сбое всего батча переводит все восстанавливаемые подписки в FAILED")
    void recover_whenBatchStreamFailsExceptionally_marksAllFailed() {
        SubscriptionEntity entity1 = createEntity(Status.CONNECTING, 1);
        SubscriptionEntity entity2 = new SubscriptionEntity(
                TEST_ID_2, EXCHANGE, CHANNEL, SYMBOL_2, Status.CONNECTING, null, TEST_TIMESTAMP, TEST_TIMESTAMP, 1
        );

        when(marketDataStreamManager.isSubscribedTrades(any())).thenReturn(false);
        when(marketDataStreamManager.subscribeTradesBatchAsync(any(), any(), anyBoolean(), any(), any()))
                .thenReturn(CompletableFuture.failedFuture(new RuntimeException(ERROR_MESSAGE)));

        subscriptionService.recover(List.of(entity1, entity2));

        verify(storageService).markFailed(TEST_ID, ERROR_MESSAGE);
        verify(storageService).markFailed(TEST_ID_2, ERROR_MESSAGE);
        verify(storageService, never()).markActive(any());
        verifyNoMoreInteractions(storageService, marketDataStreamManager);
    }

    // =========================================================================
    // Группа 5: Чтение данных (findById и findAll)
    // =========================================================================

    @Test
    @DisplayName("15. findById: делегирует запрос в хранилище и конвертирует сущность в DTO")
    void findById_delegatesToStorageAndMapper() {
        SubscriptionEntity entity = createEntity(Status.ACTIVE, 1);
        SubscriptionResponse expectedResponse = createResponse(Status.ACTIVE);

        when(storageService.findById(TEST_ID)).thenReturn(entity);
        when(mapper.toDto(entity)).thenReturn(expectedResponse);

        SubscriptionResponse actual = subscriptionService.findById(TEST_ID);

        assertThat(actual).isEqualTo(expectedResponse);
        verify(storageService).findById(TEST_ID);
        verify(mapper).toDto(entity);
        verifyNoMoreInteractions(storageService, mapper, marketDataStreamManager);
    }

    @Test
    @DisplayName("16. findAll: делегирует запрос с фильтрами в хранилище и конвертирует список в DTO")
    void findAll_delegatesToStorageAndMapper() {
        SubscriptionEntity entity = createEntity(Status.ACTIVE, 1);
        SubscriptionResponse expectedResponse = createResponse(Status.ACTIVE);

        when(storageService.findAll(EXCHANGE, CHANNEL, Status.ACTIVE)).thenReturn(List.of(entity));
        when(mapper.toDto(entity)).thenReturn(expectedResponse);

        List<SubscriptionResponse> actual = subscriptionService.findAll(EXCHANGE, CHANNEL, Status.ACTIVE);

        assertThat(actual).containsExactly(expectedResponse);
        verify(storageService).findAll(EXCHANGE, CHANNEL, Status.ACTIVE);
        verify(mapper).toDto(entity);
        verifyNoMoreInteractions(storageService, mapper, marketDataStreamManager);
    }

    // =========================================================================
    // Группа 6: Граничные случаи (дедупликация и частичная подписка при recover)
    // =========================================================================

    @Test
    @DisplayName("17. recover: дедуплицирует повторяющиеся инструменты перед вызовом менеджера")
    void recover_whenDuplicateInstrumentsInBatch_deduplicatesBeforeCallingManager() {
        SubscriptionEntity entity1 = createEntity(Status.CONNECTING, 1);
        SubscriptionEntity entity1Dup = new SubscriptionEntity(
                999L, EXCHANGE, CHANNEL, SYMBOL, Status.CONNECTING, null, TEST_TIMESTAMP, TEST_TIMESTAMP, 1
        );

        when(marketDataStreamManager.isSubscribedTrades(INSTRUMENT)).thenReturn(false);
        var mockResult = new MarketDataSubscriptionResult(
                RequestAction.SUBSCRIBE,
                MarketDataResponseType.TRADE,
                Map.of(INSTRUMENT, SubscriptionStatus.OK)
        );
        when(marketDataStreamManager.subscribeTradesBatchAsync(any(), any(), anyBoolean(), any(), any()))
                .thenReturn(CompletableFuture.completedFuture(mockResult));

        subscriptionService.recover(List.of(entity1, entity1Dup));

        verify(marketDataStreamManager).subscribeTradesBatchAsync(
                eq(Set.of(INSTRUMENT)),
                eq(TradeSourceType.TRADE_SOURCE_ALL),
                eq(false),
                any(),
                any()
        );
        verify(storageService).markActive(TEST_ID);
        verifyNoMoreInteractions(storageService, marketDataStreamManager);
    }

    @Test
    @DisplayName("18. recover: отправляет в стрим только те инструменты, которые еще не подписаны")
    void recover_whenPartiallySubscribed_onlySubscribesNewInstruments() {
        SubscriptionEntity entity1 = createEntity(Status.ACTIVE, 1);
        SubscriptionEntity entity2 = new SubscriptionEntity(
                TEST_ID_2, EXCHANGE, CHANNEL, SYMBOL_2, Status.CONNECTING, null, TEST_TIMESTAMP, TEST_TIMESTAMP, 1
        );

        when(marketDataStreamManager.isSubscribedTrades(INSTRUMENT)).thenReturn(true);
        when(marketDataStreamManager.isSubscribedTrades(INSTRUMENT_2)).thenReturn(false);

        var mockResult = new MarketDataSubscriptionResult(
                RequestAction.SUBSCRIBE,
                MarketDataResponseType.TRADE,
                Map.of(INSTRUMENT_2, SubscriptionStatus.OK)
        );
        when(marketDataStreamManager.subscribeTradesBatchAsync(any(), any(), anyBoolean(), any(), any()))
                .thenReturn(CompletableFuture.completedFuture(mockResult));

        subscriptionService.recover(List.of(entity1, entity2));

        verify(marketDataStreamManager).isSubscribedTrades(INSTRUMENT);
        verify(marketDataStreamManager).isSubscribedTrades(INSTRUMENT_2);
        verify(marketDataStreamManager).subscribeTradesBatchAsync(
                eq(Set.of(INSTRUMENT_2)),
                eq(TradeSourceType.TRADE_SOURCE_ALL),
                eq(false),
                any(),
                any()
        );
        verify(storageService).markActive(TEST_ID_2);
        verifyNoMoreInteractions(storageService, marketDataStreamManager);
    }

    @Test
    @DisplayName("19. subscribe: при завершении фьючера с null-результатом без ошибки переводит подписку в FAILED")
    void subscribe_whenStreamReturnsNullResult_marksFailedInStorage() {
        var request = new CreateSubscriptionRequest(EXCHANGE, SYMBOL, CHANNEL);
        SubscriptionEntity entity = createEntity(Status.CONNECTING, 1);

        when(storageService.getOrCreate(request)).thenReturn(entity);
        when(marketDataStreamManager.subscribeTradeAsync(any(), any(), anyBoolean(), any(), any()))
                .thenReturn(CompletableFuture.completedFuture(null));

        subscriptionService.subscribe(request);

        verify(storageService).getOrCreate(request);
        verify(storageService).markFailed(TEST_ID, REJECTED_BY_EXCHANGE);
        verify(storageService, never()).markActive(any());
        verify(mapper).toDto(entity);
        verifyNoMoreInteractions(storageService, mapper, marketDataStreamManager);
    }

    @Test
    @DisplayName("20. subscribe: зарегистрированные tradeListener и openInterestListener успешно обрабатывают поступающие события")
    void subscribe_listeners_handleIncomingEventsWithoutException() {
        var request = new CreateSubscriptionRequest(EXCHANGE, SYMBOL, CHANNEL);
        SubscriptionEntity entity = createEntity(Status.CONNECTING, 1);

        when(storageService.getOrCreate(request)).thenReturn(entity);
        when(marketDataStreamManager.subscribeTradeAsync(any(), any(), anyBoolean(), any(), any()))
                .thenReturn(new CompletableFuture<>());

        subscriptionService.subscribe(request);

        @SuppressWarnings("unchecked")
        ArgumentCaptor<OnNextListener<TradeWrapper>> tradeCaptor = ArgumentCaptor.forClass(OnNextListener.class);
        @SuppressWarnings("unchecked")
        ArgumentCaptor<OnNextListener<OpenInterestWrapper>> oiCaptor = ArgumentCaptor.forClass(OnNextListener.class);

        verify(marketDataStreamManager).subscribeTradeAsync(
                eq(INSTRUMENT),
                eq(TradeSourceType.TRADE_SOURCE_ALL),
                eq(false),
                tradeCaptor.capture(),
                oiCaptor.capture()
        );

        Trade trade = Trade.newBuilder().setInstrumentUid(SYMBOL).build();
        TradeWrapper tradeWrapper = mock(TradeWrapper.class);
        when(tradeWrapper.getOriginal()).thenReturn(trade);
        when(tradeWrapper.getPrice()).thenReturn(java.math.BigDecimal.valueOf(100.0));
        when(tradeWrapper.getQuantity()).thenReturn(10L);

        tradeCaptor.getValue().onNext(tradeWrapper);
        oiCaptor.getValue().onNext(mock(OpenInterestWrapper.class));
    }

    @Test
    @DisplayName("21. subscribe: после полной отписки (TERMINATED) повторный запрос успешно поднимает подписку в ACTIVE")
    void subscribe_afterUnsubscribe_successfullyReactivatesStream() {
        var request = new CreateSubscriptionRequest(EXCHANGE, SYMBOL, CHANNEL);
        SubscriptionEntity activeEntity = createEntity(Status.ACTIVE, 1);
        SubscriptionEntity terminatedEntity = createEntity(Status.TERMINATED, 0);
        SubscriptionEntity connectingEntity = createEntity(Status.CONNECTING, 1);

        when(storageService.getOrCreate(request)).thenReturn(activeEntity);
        when(marketDataStreamManager.subscribeTradeAsync(any(), any(), anyBoolean(), any(), any()))
                .thenReturn(CompletableFuture.completedFuture(SubscriptionStatus.OK));

        subscriptionService.subscribe(request);
        verify(storageService).markActive(TEST_ID);

        when(storageService.decrementRefCount(TEST_ID)).thenReturn(terminatedEntity);
        subscriptionService.unsubscribe(TEST_ID);
        verify(marketDataStreamManager).unsubscribeTrades(Set.of(INSTRUMENT), false, TradeSourceType.TRADE_SOURCE_ALL);

        when(storageService.getOrCreate(request)).thenReturn(connectingEntity);
        subscriptionService.subscribe(request);

        verify(storageService, times(2)).markActive(TEST_ID);
        verify(storageService, never()).markFailed(any(), any());
    }

    // =========================================================================
    // Группа 7: Резолв инструментов через TinvestService
    // =========================================================================

    @Test
    @DisplayName("22. subscribe: при создании подписки с тикером SBER резолвит символ через tinvestService.resolve() и передает UID в стрим и хранилище")
    void subscribe_whenTickerProvided_resolvesSymbolAndPassesUidToStreamManagerAndStorage() {
        String ticker = "SBER";
        String uid = "uid-sber-999";
        var request = new CreateSubscriptionRequest(EXCHANGE, ticker, CHANNEL);
        var expectedStorageRequest = new CreateSubscriptionRequest(EXCHANGE, uid, CHANNEL);
        var entity = new SubscriptionEntity(
                TEST_ID,
                EXCHANGE,
                CHANNEL,
                uid,
                Status.CONNECTING,
                null,
                TEST_TIMESTAMP,
                TEST_TIMESTAMP,
                1
        );
        var expectedResponse = new SubscriptionResponse(
                TEST_ID,
                EXCHANGE,
                CHANNEL,
                uid,
                Status.CONNECTING.name(),
                null,
                TEST_TIMESTAMP,
                TEST_TIMESTAMP
        );
        var resolved = new InstrumentEntity(
                uid,
                ticker,
                "TQBR",
                "share",
                "Сбербанк",
                10,
                "RUB",
                true,
                TEST_TIMESTAMP
        );

        when(tinvestService.resolve(ticker)).thenReturn(resolved);
        when(storageService.getOrCreate(expectedStorageRequest)).thenReturn(entity);
        when(mapper.toDto(entity)).thenReturn(expectedResponse);
        when(marketDataStreamManager.subscribeTradeAsync(any(), any(), anyBoolean(), any(), any()))
                .thenReturn(new CompletableFuture<>());

        SubscriptionResponse actual = subscriptionService.subscribe(request);

        assertThat(actual).isEqualTo(expectedResponse);
        verify(tinvestService).resolve(ticker);
        verify(storageService).getOrCreate(expectedStorageRequest);
        verify(marketDataStreamManager).subscribeTradeAsync(
                eq(new Instrument(uid)),
                eq(TradeSourceType.TRADE_SOURCE_ALL),
                eq(false),
                any(),
                any()
        );
    }

    @Test
    @DisplayName("23. subscribe: если resolve бросает SymbolNotFoundException, подписка прерывается и стрим не создается")
    void subscribe_whenSymbolNotFound_abortsAndDoesNotCreateStream() {
        String ticker = "SBER";
        var request = new CreateSubscriptionRequest(EXCHANGE, ticker, CHANNEL);

        when(tinvestService.resolve(ticker)).thenThrow(new SymbolNotFoundException("Инструмент не найден: " + ticker));

        assertThatThrownBy(() -> subscriptionService.subscribe(request))
                .isInstanceOf(SymbolNotFoundException.class)
                .hasMessageContaining("SBER");

        verify(tinvestService).resolve(ticker);
        verifyNoInteractions(marketDataStreamManager);
        verify(storageService, never()).getOrCreate(any());
    }
}
