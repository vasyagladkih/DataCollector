package ru.datacollector.streaming;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import ru.tinkoff.piapi.contract.v1.MarketDataRequest;
import ru.tinkoff.piapi.contract.v1.Trade;
import ru.tinkoff.piapi.contract.v1.TradeSourceType;
import ru.ttech.piapi.core.connector.ConnectorConfiguration;
import ru.ttech.piapi.core.connector.ServiceStubFactory;
import ru.ttech.piapi.core.connector.streaming.StreamServiceStubFactory;
import ru.ttech.piapi.core.connector.streaming.listeners.OnNextListener;
import ru.ttech.piapi.core.impl.marketdata.MarketDataResponseType;
import ru.ttech.piapi.core.impl.marketdata.subscription.Instrument;
import ru.ttech.piapi.core.impl.marketdata.subscription.MarketDataSubscriptionResult;
import ru.ttech.piapi.core.impl.marketdata.subscription.RequestAction;
import ru.ttech.piapi.core.impl.marketdata.subscription.SubscriptionStatus;
import ru.ttech.piapi.core.impl.marketdata.wrapper.OpenInterestWrapper;
import ru.ttech.piapi.core.impl.marketdata.wrapper.TradeWrapper;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class DataCollectorMarketDataStreamManagerTest {

    private static final String FIGI = "BBG004730N88";
    private static final Instrument INSTRUMENT = new Instrument(FIGI);
    private static final Instrument INSTRUMENT_2 = new Instrument("SBER");

    private static final OnNextListener<TradeWrapper> NOOP_TRADE_LISTENER = Objects::requireNonNull;
    private static final OnNextListener<OpenInterestWrapper> NOOP_OI_LISTENER = Objects::requireNonNull;

    @Mock
    private StreamServiceStubFactory streamFactory;

    @Mock
    private ServiceStubFactory serviceStubFactory;

    @Mock
    private ConnectorConfiguration connectorConfiguration;

    @Mock
    private ExecutorService executorService;

    @Mock
    private ScheduledExecutorService scheduledExecutorService;

    private StubMarketDataStreamManager manager;

    @BeforeEach
    void setUp() {
        doReturn(serviceStubFactory).when(streamFactory).getServiceStubFactory();
        doReturn(connectorConfiguration).when(serviceStubFactory).getConfiguration();

        manager = new StubMarketDataStreamManager(
                streamFactory,
                executorService,
                scheduledExecutorService
        );
    }

    // =========================================================================
    // Группа 1: Управление жизненным циклом (SmartLifecycle)
    // =========================================================================

    @Test
    @DisplayName("1. start() переводит статус в running, а stop() останавливает менеджер")
    void startAndStop_shouldManageRunningState() {
        assertThat(manager.isRunning()).isFalse();

        manager.start();
        assertThat(manager.isRunning()).isTrue();

        manager.stop();
        assertThat(manager.isRunning()).isFalse();
    }

    @Test
    @DisplayName("2. Повторный вызов start() игнорируется (идемпотентность)")
    void start_shouldBeIdempotent() {
        manager.start();
        assertThat(manager.isRunning()).isTrue();

        manager.start();

        assertThat(manager.isRunning()).isTrue();
        verify(executorService, times(6)).submit(any(Runnable.class));
    }

    @Test
    @DisplayName("3. Повторный вызов stop() игнорируется (идемпотентность)")
    void stop_shouldBeIdempotent() {
        manager.start();
        assertThat(manager.isRunning()).isTrue();

        manager.stop();
        assertThat(manager.isRunning()).isFalse();

        manager.stop();

        assertThat(manager.isRunning()).isFalse();
    }

    // =========================================================================
    // Группа 2: Бизнес-логика пакетных подписок (subscribeTradesBatchAsync)
    // =========================================================================

    @Test
    @DisplayName("4. Если инструмент уже в стриме, возвращается немедленный OK без сетевого вызова")
    void subscribeTradesBatchAsync_whenAlreadySubscribed_shouldReturnImmediateOk() {
        var spyManager = spy(manager);
        doReturn(true).when(spyManager).isSubscribedTrades(INSTRUMENT);

        CompletableFuture<MarketDataSubscriptionResult> future = spyManager.subscribeTradesBatchAsync(
                Set.of(INSTRUMENT),
                TradeSourceType.TRADE_SOURCE_ALL,
                false,
                NOOP_TRADE_LISTENER,
                NOOP_OI_LISTENER
        );

        assertThat(future).isCompleted();
        MarketDataSubscriptionResult result = future.join();
        assertThat(result).isNotNull();
        assertThat(result.getResponseType()).isEqualTo(MarketDataResponseType.TRADE);
        assertThat(result.getSubscriptionStatusMap()).containsExactlyEntriesOf(Map.of(INSTRUMENT, SubscriptionStatus.OK));
        assertThat(spyManager.getTradeListeners()).hasSize(1);
        assertThat(spyManager.getOpenInterestListeners()).hasSize(1);
    }

    @Test
    @DisplayName("5. Если инструмент новый, слушатели регистрируются в коллбэке успешной сетевой подписки")
    void subscribeTradesBatchAsync_whenNewInstrument_shouldRegisterListenersOnSuccess() {
        var spyManager = spy(manager);
        doReturn(false).when(spyManager).isSubscribedTrades(INSTRUMENT);

        var mockResult = new MarketDataSubscriptionResult(
                RequestAction.SUBSCRIBE,
                MarketDataResponseType.TRADE,
                Map.of(INSTRUMENT, SubscriptionStatus.OK)
        );
        doReturn(CompletableFuture.completedFuture(mockResult))
                .when(spyManager).subscribe(any(), any(), any());

        CompletableFuture<MarketDataSubscriptionResult> future = spyManager.subscribeTradesBatchAsync(
                Set.of(INSTRUMENT),
                TradeSourceType.TRADE_SOURCE_ALL,
                false,
                NOOP_TRADE_LISTENER,
                NOOP_OI_LISTENER
        );

        assertThat(future).isCompleted();
        MarketDataSubscriptionResult result = future.join();
        assertThat(result).isNotNull();
        assertThat(result.getResponseType()).isEqualTo(MarketDataResponseType.TRADE);
        assertThat(result.getSubscriptionStatusMap()).containsExactlyEntriesOf(Map.of(INSTRUMENT, SubscriptionStatus.OK));
        assertThat(spyManager.getTradeListeners()).hasSize(1);
        assertThat(spyManager.getOpenInterestListeners()).hasSize(1);
    }

    @Test
    @DisplayName("6. Ошибка gRPC: при сбое сетевого стрима фьючер завершается с ошибкой, а слушатели не сохраняются")
    void subscribeTradesBatchAsync_whenGrpcFails_shouldNotRegisterListenersAndFailFuture() {
        var spyManager = spy(manager);
        doReturn(false).when(spyManager).isSubscribedTrades(INSTRUMENT);

        doReturn(CompletableFuture.failedFuture(new RuntimeException("gRPC network error")))
                .when(spyManager).subscribe(any(), any(), any());

        CompletableFuture<MarketDataSubscriptionResult> future = spyManager.subscribeTradesBatchAsync(
                Set.of(INSTRUMENT),
                TradeSourceType.TRADE_SOURCE_ALL,
                false,
                NOOP_TRADE_LISTENER,
                NOOP_OI_LISTENER
        );

        assertThat(future).failsWithin(1, TimeUnit.SECONDS)
                .withThrowableOfType(ExecutionException.class)
                .withCauseInstanceOf(RuntimeException.class)
                .withMessageContaining("gRPC network error");
        assertThat(spyManager.getTradeListeners()).isEmpty();
        assertThat(spyManager.getOpenInterestListeners()).isEmpty();
    }

    @Test
    @DisplayName("7. Идемпотентность регистрации: повторная подписка на уже активный инструмент не должна дублировать слушателей")
    void subscribeTradesBatchAsync_whenAlreadySubscribed_shouldNotDuplicateListeners() {
        var spyManager = spy(manager);
        doReturn(true).when(spyManager).isSubscribedTrades(INSTRUMENT);

        for (int i = 0; i < 3; i++) {
            CompletableFuture<MarketDataSubscriptionResult> future = spyManager.subscribeTradesBatchAsync(
                    Set.of(INSTRUMENT),
                    TradeSourceType.TRADE_SOURCE_ALL,
                    false,
                    NOOP_TRADE_LISTENER,
                    NOOP_OI_LISTENER
            );
            assertThat(future).isCompleted();
        }

        assertThat(spyManager.getTradeListeners())
                .as("Повторные вызовы subscribeTradesBatchAsync не должны приводить к дублированию слушателей одного стрима")
                .hasSize(1);
        assertThat(spyManager.getOpenInterestListeners())
                .as("Повторные вызовы subscribeTradesBatchAsync не должны приводить к дублированию слушателей открытого интереса")
                .hasSize(1);
    }

    // =========================================================================
    // Группа 3: Многопоточность и дефект #25 (ConcurrentModificationException)
    // =========================================================================

    @Test
    @DisplayName("8. Потокобезопасность: параллельное добавление слушателей во время итерации воркера не приводит к ConcurrentModificationException")
    void concurrentListenerRegistration_underActiveIteration_shouldNotThrowException()
            throws InterruptedException, ExecutionException, TimeoutException {
        var spyManager = spy(manager);
        doReturn(true).when(spyManager).isSubscribedTrades(any(Instrument.class));

        List<OnNextListener<TradeWrapper>> tradeListeners = spyManager.getTradeListeners();

        for (int i = 0; i < 20; i++) {
            tradeListeners.add(NOOP_TRADE_LISTENER);
        }

        Trade realTrade = Trade.newBuilder()
                .setFigi(FIGI)
                .setInstrumentUid(FIGI)
                .build();
        TradeWrapper mockTrade = mock(TradeWrapper.class);
        doReturn(realTrade).when(mockTrade).getOriginal();

        AtomicReference<RuntimeException> iterationException = new AtomicReference<>();
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch stopLatch = new CountDownLatch(1);
        int threadCount = 10;
        CountDownLatch clientDoneLatch = new CountDownLatch(threadCount * 50);

        try (ExecutorService pool = Executors.newFixedThreadPool(threadCount + 1)) {
            Future<?> workerFuture = pool.submit(() -> {
                try {
                    if (!startLatch.await(5, TimeUnit.SECONDS)) {
                        return;
                    }
                    while (stopLatch.getCount() > 0) {
                        tradeListeners.forEach(l -> l.onNext(mockTrade));
                    }
                } catch (InterruptedException _) {
                    Thread.currentThread().interrupt();
                } catch (RuntimeException e) {
                    iterationException.set(e);
                }
            });

            startLatch.countDown();

            for (int i = 0; i < threadCount * 50; i++) {
                pool.execute(() -> {
                    try {
                        spyManager.subscribeTradesBatchAsync(
                                Set.of(INSTRUMENT),
                                TradeSourceType.TRADE_SOURCE_ALL,
                                false,
                                NOOP_TRADE_LISTENER,
                                NOOP_OI_LISTENER
                        );
                    } finally {
                        clientDoneLatch.countDown();
                    }
                });
            }

            boolean clientsCompleted = clientDoneLatch.await(5, TimeUnit.SECONDS);
            assertThat(clientsCompleted).as("Все клиенты должны успешно завершить регистрацию").isTrue();
            stopLatch.countDown();
            workerFuture.get(2, TimeUnit.SECONDS);
        }

        assertThat(iterationException.get())
                .as("Воркер SDK должен работать стабильно без выброса исключений при конкурентной модификации")
                .isNull();
    }

    @Test
    @DisplayName("9. Потокобезопасность (детерминированная): добавление слушателя блокируется до окончания обхода forEach воркером")
    void concurrentRegistration_blockedDuringWorkerIteration_provesMutualExclusion()
            throws InterruptedException, ExecutionException, TimeoutException {
        var spyManager = spy(manager);
        doReturn(true).when(spyManager).isSubscribedTrades(any(Instrument.class));

        List<OnNextListener<TradeWrapper>> tradeListeners = spyManager.getTradeListeners();

        CountDownLatch insideIterationLatch = new CountDownLatch(1);
        CountDownLatch releaseIterationLatch = new CountDownLatch(1);

        tradeListeners.add(trade -> {
            if (trade != null) {
                insideIterationLatch.countDown();
            }
            try {
                if (!releaseIterationLatch.await(2, TimeUnit.SECONDS)) {
                    Thread.currentThread().interrupt();
                }
            } catch (InterruptedException _) {
                Thread.currentThread().interrupt();
            }
        });

        TradeWrapper mockTrade = mock(TradeWrapper.class);

        try (ExecutorService pool = Executors.newFixedThreadPool(2)) {
            Future<?> workerFuture = pool.submit(() -> tradeListeners.forEach(l -> l.onNext(mockTrade)));

            boolean entered = insideIterationLatch.await(1, TimeUnit.SECONDS);
            assertThat(entered).isTrue();

            Future<?> clientFuture = pool.submit(() -> spyManager.subscribeTradesBatchAsync(
                    Set.of(INSTRUMENT),
                    TradeSourceType.TRADE_SOURCE_ALL,
                    false,
                    NOOP_TRADE_LISTENER,
                    NOOP_OI_LISTENER
            ));

            assertThat(clientFuture).isNotDone();

            releaseIterationLatch.countDown();

            workerFuture.get(2, TimeUnit.SECONDS);
            clientFuture.get(2, TimeUnit.SECONDS);

            assertThat(clientFuture).isDone();
        }
    }

    // =========================================================================
    // Группа 4: Новые контракты Live (single) и Recovery (batch) подписок
    // =========================================================================

    @Test
    @DisplayName("10. subscribeTradeAsync: при уже активной подписке возвращает немедленный OK")
    void subscribeTradeAsync_whenAlreadySubscribed_shouldReturnImmediateOk() {
        var spyManager = spy(manager);
        doReturn(true).when(spyManager).isSubscribedTrades(INSTRUMENT);

        CompletableFuture<SubscriptionStatus> future = spyManager.subscribeTradeAsync(
                INSTRUMENT,
                TradeSourceType.TRADE_SOURCE_ALL,
                false,
                NOOP_TRADE_LISTENER,
                NOOP_OI_LISTENER
        );

        assertThat(future).isCompleted();
        assertThat(future.join()).isEqualTo(SubscriptionStatus.OK);
        assertThat(spyManager.getTradeListeners()).hasSize(1);
        assertThat(spyManager.getOpenInterestListeners()).hasSize(1);
    }

    @Test
    @DisplayName("11. subscribeTradeAsync: для нового инструмента запрашивает стрим и возвращает статус биржи")
    void subscribeTradeAsync_whenNewInstrument_shouldReturnStatusFromNetwork() {
        var spyManager = spy(manager);
        doReturn(false).when(spyManager).isSubscribedTrades(INSTRUMENT);

        var mockResult = new MarketDataSubscriptionResult(
                RequestAction.SUBSCRIBE,
                MarketDataResponseType.TRADE,
                Map.of(INSTRUMENT, SubscriptionStatus.OK)
        );
        doReturn(CompletableFuture.completedFuture(mockResult))
                .when(spyManager).subscribe(any(), any(), any());

        CompletableFuture<SubscriptionStatus> future = spyManager.subscribeTradeAsync(
                INSTRUMENT,
                TradeSourceType.TRADE_SOURCE_ALL,
                false,
                NOOP_TRADE_LISTENER,
                NOOP_OI_LISTENER
        );

        assertThat(future).isCompleted();
        assertThat(future.join()).isEqualTo(SubscriptionStatus.OK);
        assertThat(spyManager.getTradeListeners()).hasSize(1);
        assertThat(spyManager.getOpenInterestListeners()).hasSize(1);
    }

    @Test
    @DisplayName("12. subscribeTradesBatchAsync: для смешанного набора объединяет активные и новые инструменты без ложного FAILED")
    void subscribeTradesBatchAsync_whenMixedInstruments_shouldMergeActiveInstrumentsWithOkStatus() {
        var spyManager = spy(manager);
        doReturn(true).when(spyManager).isSubscribedTrades(INSTRUMENT);
        doReturn(false).when(spyManager).isSubscribedTrades(INSTRUMENT_2);

        var mockNetworkResult = new MarketDataSubscriptionResult(
                RequestAction.SUBSCRIBE,
                MarketDataResponseType.TRADE,
                Map.of(INSTRUMENT_2, SubscriptionStatus.OK)
        );
        doReturn(CompletableFuture.completedFuture(mockNetworkResult))
                .when(spyManager).subscribe(any(), any(), any());

        CompletableFuture<MarketDataSubscriptionResult> future = spyManager.subscribeTradesBatchAsync(
                Set.of(INSTRUMENT, INSTRUMENT_2),
                TradeSourceType.TRADE_SOURCE_ALL,
                false,
                NOOP_TRADE_LISTENER,
                NOOP_OI_LISTENER
        );

        assertThat(future).isCompleted();
        MarketDataSubscriptionResult result = future.join();
        assertThat(result).isNotNull();
        assertThat(result.getSubscriptionStatusMap()).containsExactlyInAnyOrderEntriesOf(Map.of(
                INSTRUMENT, SubscriptionStatus.OK,
                INSTRUMENT_2, SubscriptionStatus.OK
        ));
        assertThat(spyManager.getTradeListeners()).hasSize(1);
        assertThat(spyManager.getOpenInterestListeners()).hasSize(1);
    }

    @Test
    @DisplayName("13. subscribeTradesBatchAsync: если все инструменты уже подписаны, возвращает OK без обращения в сеть")
    void subscribeTradesBatchAsync_whenAllAlreadySubscribed_shouldReturnImmediateOk() {
        var spyManager = spy(manager);
        doReturn(true).when(spyManager).isSubscribedTrades(INSTRUMENT);
        doReturn(true).when(spyManager).isSubscribedTrades(INSTRUMENT_2);

        CompletableFuture<MarketDataSubscriptionResult> future = spyManager.subscribeTradesBatchAsync(
                Set.of(INSTRUMENT, INSTRUMENT_2),
                TradeSourceType.TRADE_SOURCE_ALL,
                false,
                NOOP_TRADE_LISTENER,
                NOOP_OI_LISTENER
        );

        assertThat(future).isCompleted();
        MarketDataSubscriptionResult result = future.join();
        assertThat(result).isNotNull();
        assertThat(result.getSubscriptionStatusMap()).containsExactlyInAnyOrderEntriesOf(Map.of(
                INSTRUMENT, SubscriptionStatus.OK,
                INSTRUMENT_2, SubscriptionStatus.OK
        ));
        assertThat(spyManager.getTradeListeners()).hasSize(1);
        assertThat(spyManager.getOpenInterestListeners()).hasSize(1);
    }

    /**
     * Тестовый наследник для прямого доступа к унаследованному полю context родительского SDK
     * и открытия доступа к protected-методам для шпионажа в тестах.
     */
    static class StubMarketDataStreamManager extends DataCollectorMarketDataStreamManager {
        StubMarketDataStreamManager(StreamServiceStubFactory streamFactory,
                                   ExecutorService executorService,
                                   ScheduledExecutorService scheduledExecutorService) {
            super(streamFactory, executorService, scheduledExecutorService);
        }

        List<OnNextListener<TradeWrapper>> getTradeListeners() {
            return this.context.getOnTradeListeners();
        }

        List<OnNextListener<OpenInterestWrapper>> getOpenInterestListeners() {
            return this.context.getOnOpenInterestListeners();
        }

        @Override
        public CompletableFuture<MarketDataSubscriptionResult> subscribe(
                MarketDataResponseType type,
                Set<Instrument> instruments,
                Function<List<Instrument>, MarketDataRequest> requestBuilder) {
            return super.subscribe(type, instruments, requestBuilder);
        }
    }
}
