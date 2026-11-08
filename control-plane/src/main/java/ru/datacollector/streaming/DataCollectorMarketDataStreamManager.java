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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
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
    private final Set<Instrument> activeTradeInstruments = ConcurrentHashMap.newKeySet();
    private final Set<Instrument> activeOiInstruments = ConcurrentHashMap.newKeySet();

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
            } finally {
                activeTradeInstruments.clear();
                activeOiInstruments.clear();
            }
        }
    }

    @Override
    public boolean isRunning() {
        return running.get();
    }

    /**
     * Асинхронная единичная подписка на сделки для одного инструмента (Live-режим).
     *
     * @param instrument              инструмент
     * @param tradeSourceType         тип источника сделок
     * @param withOpenInterest        флаг открытого интереса
     * @param onTradeListener         слушатель сделок
     * @param onOpenInterestListener  слушатель открытого интереса
     * @return фьючер со статусом подписки (OK, ERROR и т.д.)
     */
    public CompletableFuture<SubscriptionStatus> subscribeTradeAsync(
            Instrument instrument,
            TradeSourceType tradeSourceType,
            boolean withOpenInterest,
            OnNextListener<TradeWrapper> onTradeListener,
            OnNextListener<OpenInterestWrapper> onOpenInterestListener
    ) {
        var instruments = Set.of(instrument);
        var wrappedTradeListener = filterByInstrumentId(
                instruments, trade -> InstrumentMapper.map(trade.getOriginal()), onTradeListener
        );
        var wrappedOpenInterestListener = filterByInstrumentId(
                instruments, openInterest -> InstrumentMapper.map(openInterest.getOriginal()), onOpenInterestListener
        );

        if (isSubscribedTrades(instrument)) {
            registerTradeListenerIfAbsent(instruments, wrappedTradeListener);
            registerOiListenerIfAbsent(instruments, wrappedOpenInterestListener);
            return CompletableFuture.completedFuture(SubscriptionStatus.OK);
        }

        Function<List<Instrument>, MarketDataRequest> requestBuilder = instrumentsSublist ->
                MarketDataRequestBuilder.buildTradesRequest(
                        instrumentsSublist,
                        tradeSourceType,
                        withOpenInterest,
                        SubscriptionAction.SUBSCRIPTION_ACTION_SUBSCRIBE
                );

        return subscribe(MarketDataResponseType.TRADE, instruments, requestBuilder)
                .whenComplete((subscriptionResult, throwable) -> {
                    if (throwable != null) {
                        log.error("Ошибка при подписке на сделки для инструмента: {}", instrument, throwable);
                        return;
                    }
                    if (subscriptionResult != null) {
                        registerTradeListenerIfAbsent(instruments, wrappedTradeListener);
                        registerOiListenerIfAbsent(instruments, wrappedOpenInterestListener);
                    }
                })
                .thenApply(result -> {
                    if (result == null || result.getSubscriptionStatusMap() == null) return SubscriptionStatus.ERROR;

                    return result.getSubscriptionStatusMap().entrySet().stream()
                            .filter(entry -> entry.getKey().sharesIdWith(instrument))
                            .map(Map.Entry::getValue)
                            .findFirst()
                            .orElse(SubscriptionStatus.ERROR);
                });
    }

    /**
     * Асинхронная пакетная подписка на сделки для набора инструментов (Crash Recovery режим).
     * Если часть инструментов уже активна в стриме, сетевой запрос отправляется только для новых,
     * а их статус OK объединяется с ответом биржи, предотвращая потерю подписок.
     *
     * @param instruments             набор инструментов
     * @param tradeSourceType         тип источника сделок
     * @param withOpenInterest        флаг открытого интереса
     * @param onTradeListener         слушатель сделок
     * @param onOpenInterestListener  слушатель открытого интереса
     * @return фьючер с детальным объединенным результатом подписки для всех запрошенных инструментов
     */
    public CompletableFuture<MarketDataSubscriptionResult> subscribeTradesBatchAsync(
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

        var alreadySubscribed = instruments.stream()
                .filter(this::isSubscribedTrades)
                .collect(Collectors.toUnmodifiableSet());

        var needNetworkSubscription = instruments.stream()
                .filter(instrument -> !alreadySubscribed.contains(instrument))
                .collect(Collectors.toUnmodifiableSet());

        if (needNetworkSubscription.isEmpty()) {
            registerTradeListenerIfAbsent(instruments, wrappedTradeListener);
            registerOiListenerIfAbsent(instruments, wrappedOpenInterestListener);

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

        return subscribe(MarketDataResponseType.TRADE, needNetworkSubscription, requestBuilder)
                .whenComplete((subscriptionResult, throwable) -> {
                    if (throwable != null) {
                        log.error("Ошибка при пакетной подписке на сделки для инструментов: {}", needNetworkSubscription, throwable);
                        return;
                    }
                    if (subscriptionResult != null) {
                        registerTradeListenerIfAbsent(instruments, wrappedTradeListener);
                        registerOiListenerIfAbsent(instruments, wrappedOpenInterestListener);
                    }
                })
                .thenApply(networkResult -> {
                    if (alreadySubscribed.isEmpty() || networkResult == null) {
                        return networkResult;
                    }
                    var fullStatusMap = new HashMap<>(networkResult.getSubscriptionStatusMap());
                    for (Instrument subscribed : alreadySubscribed) {
                        fullStatusMap.put(subscribed, SubscriptionStatus.OK);
                    }
                    return new MarketDataSubscriptionResult(
                            networkResult.getSubscriptionAction(),
                            networkResult.getResponseType(),
                            fullStatusMap
                    );
                });
    }

    @Override
    public void unsubscribeTrades(
            Set<Instrument> instruments,
            boolean withOpenInterest,
            TradeSourceType tradeSourceType
    ) {
        super.unsubscribeTrades(instruments, withOpenInterest, tradeSourceType);
        if (instruments != null && !instruments.isEmpty()) {
            activeTradeInstruments.removeIf(active -> instruments.stream().anyMatch(active::sharesIdWith));
            if (withOpenInterest) {
                activeOiInstruments.removeIf(active -> instruments.stream().anyMatch(active::sharesIdWith));
            }
        }
    }

    private void registerTradeListenerIfAbsent(Set<Instrument> instruments, OnNextListener<TradeWrapper> listener) {
        boolean isNew = false;
        for (Instrument instrument : instruments) {
            if (activeTradeInstruments.add(instrument)) isNew = true;
        }
        if (isNew) addWrapperListener(context.getOnTradeListeners(), listener);
    }

    private void registerOiListenerIfAbsent(Set<Instrument> instruments, OnNextListener<OpenInterestWrapper> listener) {
        boolean isNew = false;
        for (Instrument instrument : instruments) {
            if (activeOiInstruments.add(instrument)) isNew = true;
        }
        if (isNew) addWrapperListener(context.getOnOpenInterestListeners(), listener);
    }
}