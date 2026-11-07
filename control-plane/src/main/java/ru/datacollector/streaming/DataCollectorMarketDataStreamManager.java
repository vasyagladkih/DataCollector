package ru.datacollector.streaming;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;
import ru.tinkoff.piapi.contract.v1.MarketDataRequest;
import ru.tinkoff.piapi.contract.v1.SubscriptionAction;
import ru.tinkoff.piapi.contract.v1.TradeSourceType;
import ru.ttech.piapi.core.connector.streaming.StreamServiceStubFactory;
import ru.ttech.piapi.core.connector.streaming.listeners.OnNextListener;
import ru.ttech.piapi.core.impl.marketdata.MarketDataResponseType;
import ru.ttech.piapi.core.impl.marketdata.MarketDataStreamManager;
import ru.ttech.piapi.core.impl.marketdata.subscription.Instrument;
import ru.ttech.piapi.core.impl.marketdata.subscription.InstrumentMapper;
import ru.ttech.piapi.core.impl.marketdata.subscription.MarketDataSubscriptionResult;
import ru.ttech.piapi.core.impl.marketdata.subscription.RequestAction;
import ru.ttech.piapi.core.impl.marketdata.subscription.SubscriptionStatus;
import ru.ttech.piapi.core.impl.marketdata.util.MarketDataRequestBuilder;
import ru.ttech.piapi.core.impl.marketdata.wrapper.OpenInterestWrapper;
import ru.ttech.piapi.core.impl.marketdata.wrapper.TradeWrapper;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Кастомный менеджер стримов рыночных данных, расширяющий базовый функционал T-Invest SDK.
 * Предоставляет асинхронный доступ к результатам подписки через {@link CompletableFuture}
 * и управляет своим жизненным циклом в контексте Spring через {@link SmartLifecycle}.
 */
@Slf4j
@Component
public class DataCollectorMarketDataStreamManager
        extends MarketDataStreamManager
        implements SmartLifecycle {

    private final AtomicBoolean running = new AtomicBoolean(false);

    public DataCollectorMarketDataStreamManager(
            StreamServiceStubFactory streamFactory,
            @Qualifier("marketDataStreamManagerExecutorService") ExecutorService executorService,
            @Qualifier("streamHealthCheckScheduledExecutorService") ScheduledExecutorService scheduledExecutorService
    ) {
        super(streamFactory, executorService, scheduledExecutorService);
    }

    @Override
    public void start() {
        if (running.compareAndSet(false, true)) {
            log.info("Запуск DataCollectorMarketDataStreamManager и очередей обработки данных...");
            super.start();
        }
    }

    @Override
    public void stop() {
        if (running.compareAndSet(true, false)) {
            log.info("Остановка DataCollectorMarketDataStreamManager и закрытие gRPC-стримов...");
            try {
                super.shutdown();
            } catch (Exception e) {
                log.error("Ошибка при остановке DataCollectorMarketDataStreamManager", e);
            }
        }
    }

    @Override
    public boolean isRunning() {
        return running.get();
    }

    /**
     * Асинхронная подписка на сделки с возвратом {@link CompletableFuture}.
     *
     * @param instruments             список инструментов
     * @param tradeSourceType         тип источника сделок
     * @param withOpenInterest        флаг открытого интереса
     * @param onTradeListener         слушатель сделок
     * @param onOpenInterestListener  слушатель открытого интереса
     * @return фьючер с детальным результатом подписки от сервера биржи
     */
    // TODO: Разделить recovery (батчевая подписка на коллекцию при рестарте) и live (единичная подписка на один инструмент),
    //       чтобы исключить уязвимость контракта при передаче смешанного набора (уже подписанные + новые инструменты).
    public CompletableFuture<MarketDataSubscriptionResult> subscribeTradesAsync(
            Set<Instrument> instruments,
            TradeSourceType tradeSourceType,
            boolean withOpenInterest,
            OnNextListener<TradeWrapper> onTradeListener,
            OnNextListener<OpenInterestWrapper> onOpenInterestListener
    ) {
        var wrappedTradeListener = filterByInstrumentId(
                instruments, trade -> InstrumentMapper.map(trade.getOriginal()), onTradeListener
        );
        var wrappedOpenInterestListener = filterByInstrumentId(
                instruments, openInterest -> InstrumentMapper.map(openInterest.getOriginal()), onOpenInterestListener
        );

        var filteredInstruments = instruments.stream()
                .filter(instrument -> !isSubscribedTrades(instrument))
                .collect(Collectors.toUnmodifiableSet());

        if (filteredInstruments.isEmpty()) {
            // TODO #25: Потенциальный ConcurrentModificationException при добавлении слушателя в несинхронизированный forEach воркера SDK
            addWrapperListener(context.getOnTradeListeners(), wrappedTradeListener);
            addWrapperListener(context.getOnOpenInterestListeners(), wrappedOpenInterestListener);

            return CompletableFuture.completedFuture(new MarketDataSubscriptionResult(
                    RequestAction.SUBSCRIBE,
                    MarketDataResponseType.TRADE,
                    instruments.stream().collect(Collectors.toMap(
                            i -> i,
                            _ -> SubscriptionStatus.OK
                    ))
            ));
        }

        Function<List<Instrument>, MarketDataRequest> requestBuilder = instrumentsSublist ->
                MarketDataRequestBuilder.buildTradesRequest(
                        instrumentsSublist,
                        tradeSourceType,
                        withOpenInterest,
                        SubscriptionAction.SUBSCRIPTION_ACTION_SUBSCRIBE
                );

        return subscribe(MarketDataResponseType.TRADE, filteredInstruments, requestBuilder)
                .whenComplete((subscriptionResult, throwable) -> {
                    if (throwable != null) {
                        log.error("Ошибка при подписке на сделки для инструментов: {}", filteredInstruments, throwable);
                        return;
                    }
                    if (subscriptionResult != null) {
                        addWrapperListener(context.getOnTradeListeners(), wrappedTradeListener);
                        addWrapperListener(context.getOnOpenInterestListeners(), wrappedOpenInterestListener);
                    }
                });
    }
}