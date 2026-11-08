package ru.datacollector.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import ru.datacollector.dto.CreateSubscriptionRequest;
import ru.datacollector.dto.SubscriptionResponse;
import ru.datacollector.mapper.SubscriptionMapper;
import ru.datacollector.model.Status;
import ru.datacollector.repository.entity.InstrumentEntity;
import ru.datacollector.repository.entity.SubscriptionEntity;
import ru.datacollector.streaming.DataCollectorMarketDataStreamManager;
import ru.tinkoff.piapi.contract.v1.Trade;
import ru.tinkoff.piapi.contract.v1.TradeSourceType;
import ru.ttech.piapi.core.connector.streaming.listeners.OnNextListener;
import ru.ttech.piapi.core.impl.marketdata.subscription.Instrument;
import ru.ttech.piapi.core.impl.marketdata.subscription.MarketDataSubscriptionResult;
import ru.ttech.piapi.core.impl.marketdata.subscription.SubscriptionStatus;
import ru.ttech.piapi.core.impl.marketdata.wrapper.OpenInterestWrapper;
import ru.ttech.piapi.core.impl.marketdata.wrapper.TradeWrapper;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

@Slf4j
@RequiredArgsConstructor
@Service
public class SubscriptionService {

    @Value("${kafka.topics.raw}")
    private String rawTopic;

    private final Map<String, String> partitionKeyCache = new ConcurrentHashMap<>();

    private final KafkaDataProducerService kafkaProducer;
    private final SubscriptionStorageService storageService;
    private final SubscriptionMapper mapper;
    private final DataCollectorMarketDataStreamManager marketDataStreamManager;
    private final TinvestService tinvestService;
    private static final OnNextListener<OpenInterestWrapper> openInterestStubListener = _ -> {};

    private final OnNextListener<TradeWrapper> tradeListener = this::onTrade;

    private void onTrade(TradeWrapper tradeWrapper) {
        Trade trade = tradeWrapper.getOriginal();
        String uid = trade.getInstrumentUid();
        String partitionKey = partitionKeyCache.computeIfAbsent(uid, _ ->
                buildPartitionKey(trade.getClassCode(), trade.getTicker())
        );
        kafkaProducer.send(rawTopic, partitionKey, trade.toByteArray());
    }

    private String buildPartitionKey(String classCode, String ticker) {
        return resolveExchange(classCode) + ":TRADE:" + ticker;
    }

    private String resolveExchange(String classCode) {
        if (classCode == null || classCode.isBlank()) return "UNKNOWN";
        if ("SPBFUT".equals(classCode) || classCode.startsWith("TQ") || "CETS".equals(classCode)) return "MOEX";
        if (classCode.startsWith("SPB") || classCode.startsWith("SPHK")) return "SPB";
        return "UNKNOWN";
    }

    public SubscriptionResponse subscribe(CreateSubscriptionRequest request) {
        InstrumentEntity instrument = tinvestService.resolve(request.symbol());

        partitionKeyCache.putIfAbsent(instrument.instrumentUid(), buildPartitionKey(instrument.classCode(), instrument.ticker()));

        CreateSubscriptionRequest targetRequest = new CreateSubscriptionRequest(
                request.exchange(),
                instrument.instrumentUid(),
                request.channel()
        );
        SubscriptionEntity entity = storageService.getOrCreate(targetRequest);
        if (entity.refCount() == 1) startStreamSubscription(entity);
        return mapper.toDto(entity);
    }

    /**
     * Батчевое восстановление стримов при Crash Recovery (пакетная отправка в gRPC).
     *
     * @param entities список незавершенных подписок из БД
     */
    public void recover(List<SubscriptionEntity> entities) {
        Map<Instrument, SubscriptionEntity> targets = entities.stream()
                .filter(e -> !marketDataStreamManager.isSubscribedTrades(new Instrument(e.symbol())))
                .collect(Collectors.toMap(e -> new Instrument(e.symbol()), e -> e, (existing, _) -> existing));

        if (targets.isEmpty()) return;

        for (SubscriptionEntity entity : targets.values()) {
            try {
                InstrumentEntity inst = tinvestService.resolve(entity.symbol());
                partitionKeyCache.putIfAbsent(inst.instrumentUid(), buildPartitionKey(inst.classCode(), inst.ticker()));
            } catch (Exception e) {
                log.warn("Не удалось восстановить ключ партиционирования для {}: {}", entity.symbol(), e.getMessage());
            }
        }

        marketDataStreamManager.subscribeTradesBatchAsync(
                targets.keySet(),
                TradeSourceType.TRADE_SOURCE_ALL,
                false,
                tradeListener,
                openInterestStubListener
        ).whenComplete((result, error) -> handleBatchSubscriptionResult(targets, result, error));
    }

    public SubscriptionResponse unsubscribe(Long id) {
        SubscriptionEntity entity = storageService.decrementRefCount(id);
        if (entity.refCount() <= 0) {
            var instrument = new Instrument(entity.symbol());
            marketDataStreamManager.unsubscribeTrades(
                    Set.of(instrument),
                    false,
                    TradeSourceType.TRADE_SOURCE_ALL
            );
            partitionKeyCache.remove(entity.symbol());
        }
        return mapper.toDto(entity);
    }

    public SubscriptionResponse restart(Long id) {
        SubscriptionEntity entity = storageService.markRestarting(id);
        try {
            InstrumentEntity inst = tinvestService.resolve(entity.symbol());
            partitionKeyCache.putIfAbsent(inst.instrumentUid(), buildPartitionKey(inst.classCode(), inst.ticker()));
        } catch (Exception e) {
            log.warn("Не удалось восстановить ключ партиционирования для {}: {}", entity.symbol(), e.getMessage());
        }
        startStreamSubscription(entity);
        return mapper.toDto(entity);
    }

    public SubscriptionResponse findById(Long id) {
        return mapper.toDto(storageService.findById(id));
    }

    public List<SubscriptionResponse> findAll(String exchange, String channel, Status status) {
        return storageService.findAll(exchange, channel, status)
                .stream()
                .map(mapper::toDto)
                .toList();
    }

    private void startStreamSubscription(SubscriptionEntity entity) {
        var instrument = new Instrument(entity.symbol());

        marketDataStreamManager.subscribeTradeAsync(
                instrument,
                TradeSourceType.TRADE_SOURCE_ALL,
                false,
                tradeListener,
                openInterestStubListener
        ).whenComplete((status, error) -> {
            if (error != null) {
                storageService.markFailed(entity.id(), error.getMessage());
            } else if (status == SubscriptionStatus.OK) {
                storageService.markActive(entity.id());
            } else {
                storageService.markFailed(entity.id(), "Subscription rejected by exchange");
            }
        });
    }

    private void handleBatchSubscriptionResult(Map<Instrument, SubscriptionEntity> targets,
                                              MarketDataSubscriptionResult result,
                                              Throwable error) {
        if (error != null) {
            targets.values().forEach(e -> storageService.markFailed(e.id(), error.getMessage()));
            return;
        }

        var statusMap = (result != null) ? result.getSubscriptionStatusMap() : Map.<Instrument, SubscriptionStatus>of();
        targets.forEach((instrument, entity) -> {

            SubscriptionStatus status = statusMap.entrySet().stream()
                    .filter(entry -> entry.getKey().sharesIdWith(instrument))
                    .map(Map.Entry::getValue)
                    .findFirst()
                    .orElse(SubscriptionStatus.ERROR);

            if (status == SubscriptionStatus.OK) {
                storageService.markActive(entity.id());
            } else {
                storageService.markFailed(entity.id(), "Subscription rejected by exchange");
            }
        });
    }
}
