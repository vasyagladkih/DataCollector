package ru.datacollector.ingestor.consumer;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Timestamp;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import ru.datacollector.ingestor.model.MarketTradeRecord;
import ru.datacollector.ingestor.repository.MarketTradeRepository;
import ru.tinkoff.piapi.contract.v1.Quotation;
import ru.tinkoff.piapi.contract.v1.Trade;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * Батчевый консьюмер сырых рыночных сделок из топика Kafka и сохранения в TimescaleDB.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class TradeBatchConsumer {

    private final MarketTradeRepository tradeRepository;

    /**
     * Обрабатывает пакет входящих сообщений Kafka со сделками.
     *
     * @param records пачка записей из топика Kafka
     */
    @KafkaListener(
            topics = "${kafka.topics.raw:market.raw}",
            groupId = "${spring.kafka.consumer.group-id:market-trades-timescale-sink}",
            batch = "true"
    )
    public void consumeBatch(List<ConsumerRecord<String, byte[]>> records) {
        if (records == null || records.isEmpty()) {
            return;
        }

        List<MarketTradeRecord> batch = new ArrayList<>(records.size());
        for (ConsumerRecord<String, byte[]> record : records) {
            byte[] payload = record.value();
            if (payload == null || payload.length == 0) {
                continue;
            }

            try {
                Trade trade = Trade.parseFrom(payload);
                batch.add(mapToRecord(record.key(), trade));
            } catch (InvalidProtocolBufferException e) {
                log.error("Ошибка парсинга Protobuf Trade (топик: {}, offset: {}): {}",
                        record.topic(), record.offset(), e.getMessage());
            }
        }

        if (!batch.isEmpty()) {
            tradeRepository.saveAll(batch);
            log.info("Обработана пачка из {} сообщений Kafka, записано {} сделок в TimescaleDB",
                    records.size(), batch.size());
        }
    }

    private MarketTradeRecord mapToRecord(String key, Trade trade) {
        String exchange = extractExchange(key);
        String symbol = resolveSymbol(trade);
        String ticker = resolveTicker(key, trade, symbol);
        BigDecimal price = convertPrice(trade.getPrice());
        long quantity = trade.getQuantity();
        Instant tradeTime = convertTime(trade.getTime());

        return new MarketTradeRecord(exchange, symbol, ticker, price, quantity, tradeTime);
    }

    private String extractExchange(String key) {
        if (key == null || key.isBlank()) {
            return "UNKNOWN";
        }
        int colonIdx = key.indexOf(':');
        if (colonIdx > 0) {
            return key.substring(0, colonIdx);
        }
        return key;
    }

    private String resolveSymbol(Trade trade) {
        if (!trade.getInstrumentUid().isBlank()) {
            return trade.getInstrumentUid();
        }
        if (!trade.getFigi().isBlank()) {
            return trade.getFigi();
        }
        if (!trade.getTicker().isBlank()) {
            return trade.getTicker();
        }
        return "UNKNOWN";
    }

    private String resolveTicker(String key, Trade trade, String fallbackSymbol) {
        if (!trade.getTicker().isBlank()) {
            return trade.getTicker();
        }
        String tickerFromKey = extractTickerFromKey(key);
        if (!tickerFromKey.isBlank()) {
            return tickerFromKey;
        }
        return fallbackSymbol;
    }

    private String extractTickerFromKey(String key) {
        if (key == null) {
            return "";
        }
        int lastColon = key.lastIndexOf(':');
        if (lastColon >= 0 && lastColon + 1 < key.length()) {
            return key.substring(lastColon + 1);
        }
        return "";
    }

    private BigDecimal convertPrice(Quotation quotation) {
        if (quotation == null) {
            return BigDecimal.ZERO;
        }
        return BigDecimal.valueOf(quotation.getUnits())
                .add(BigDecimal.valueOf(quotation.getNano(), 9));
    }

    private Instant convertTime(Timestamp timestamp) {
        if (timestamp == null || (timestamp.getSeconds() == 0 && timestamp.getNanos() == 0)) {
            return Instant.now();
        }
        return Instant.ofEpochSecond(timestamp.getSeconds(), timestamp.getNanos());
    }
}
