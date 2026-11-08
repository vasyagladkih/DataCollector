package ru.datacollector.ingestor.consumer;

import com.google.protobuf.Timestamp;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import ru.datacollector.ingestor.model.MarketTradeRecord;
import ru.datacollector.ingestor.repository.MarketTradeRepository;
import ru.tinkoff.piapi.contract.v1.Quotation;
import ru.tinkoff.piapi.contract.v1.Trade;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

@ExtendWith(MockitoExtension.class)
class TradeBatchConsumerTest {

    @Mock
    private MarketTradeRepository tradeRepository;

    @InjectMocks
    private TradeBatchConsumer consumer;

    @Test
    @DisplayName("Игнорирует пустой или null список записей")
    void shouldDoNothingOnNullOrEmptyRecords() {
        consumer.consumeBatch(null);
        consumer.consumeBatch(Collections.emptyList());

        verifyNoInteractions(tradeRepository);
    }

    @Test
    @DisplayName("Пропускает записи с null или пустым payload")
    void shouldSkipEmptyOrNullPayloads() {
        ConsumerRecord<String, byte[]> emptyRecord = new ConsumerRecord<>("market.raw", 0, 0L, "key", new byte[0]);
        ConsumerRecord<String, byte[]> nullRecord = new ConsumerRecord<>("market.raw", 0, 1L, "key", null);

        consumer.consumeBatch(List.of(emptyRecord, nullRecord));

        verifyNoInteractions(tradeRepository);
    }

    @Test
    @DisplayName("Пропускает поврежденный Protobuf payload, не прерывая выполнение")
    void shouldSkipCorruptedProtobufPayload() {
        ConsumerRecord<String, byte[]> corruptRecord = new ConsumerRecord<>(
                "market.raw", 0, 0L, "MOEX:TRADE:SBER", new byte[]{0x01, 0x02, 0x03}
        );

        consumer.consumeBatch(List.of(corruptRecord));

        verifyNoInteractions(tradeRepository);
    }

    @Test
    @DisplayName("Успешно парсит валидную сделку с Instrument UID и тикером")
    void shouldProcessValidTradeWithUidAndTicker() {
        Trade trade = Trade.newBuilder()
                .setInstrumentUid("test-uid-456")
                .setTicker("SBER")
                .setFigi("BBG004730N88")
                .setPrice(Quotation.newBuilder().setUnits(280).setNano(500_000_000).build())
                .setQuantity(25)
                .setTime(Timestamp.newBuilder().setSeconds(1700000000).setNanos(500_000_000).build())
                .build();

        ConsumerRecord<String, byte[]> record = new ConsumerRecord<>(
                "market.raw", 0, 10L, "MOEX:TRADE:SBER", trade.toByteArray()
        );

        consumer.consumeBatch(List.of(record));

        @SuppressWarnings("unchecked")
        ArgumentCaptor<List<MarketTradeRecord>> captor = ArgumentCaptor.forClass(List.class);
        verify(tradeRepository).saveAll(captor.capture());

        List<MarketTradeRecord> saved = captor.getValue();
        assertThat(saved).hasSize(1);

        MarketTradeRecord item = saved.getFirst();
        assertThat(item.exchange()).isEqualTo("MOEX");
        assertThat(item.symbol()).isEqualTo("test-uid-456");
        assertThat(item.ticker()).isEqualTo("SBER");
        assertThat(item.price()).isEqualByComparingTo(new BigDecimal("280.5"));
        assertThat(item.quantity()).isEqualTo(25L);
        assertThat(item.tradeTime()).isEqualTo(Instant.ofEpochSecond(1700000000, 500_000_000));
    }

    @Test
    @DisplayName("Использует FIGI и тикер из ключа, если UID и тикер отсутствуют в Protobuf Trade")
    void shouldFallbackToFigiAndKeyTicker() {
        Trade trade = Trade.newBuilder()
                .setFigi("BBG000B9XRY4")
                .setPrice(Quotation.newBuilder().setUnits(100).setNano(0).build())
                .setQuantity(5)
                .setTime(Timestamp.newBuilder().setSeconds(1700000100).build())
                .build();

        ConsumerRecord<String, byte[]> record = new ConsumerRecord<>(
                "market.raw", 0, 11L, "SPB:TRADE:AAPL", trade.toByteArray()
        );

        consumer.consumeBatch(List.of(record));

        @SuppressWarnings("unchecked")
        ArgumentCaptor<List<MarketTradeRecord>> captor = ArgumentCaptor.forClass(List.class);
        verify(tradeRepository).saveAll(captor.capture());

        MarketTradeRecord item = captor.getValue().getFirst();
        assertThat(item.exchange()).isEqualTo("SPB");
        assertThat(item.symbol()).isEqualTo("BBG000B9XRY4");
        assertThat(item.ticker()).isEqualTo("AAPL");
    }

    @Test
    @DisplayName("Корректно определяет биржу из различных форматов ключа Kafka")
    void shouldExtractExchangeCorrectlyFromVariousKeyFormats() {
        Trade trade = Trade.newBuilder()
                .setInstrumentUid("uid-1")
                .setPrice(Quotation.newBuilder().setUnits(10).build())
                .build();

        ConsumerRecord<String, byte[]> rec1 = new ConsumerRecord<>("market.raw", 0, 1L, "MOEX:TRADE:A", trade.toByteArray());
        ConsumerRecord<String, byte[]> rec2 = new ConsumerRecord<>("market.raw", 0, 2L, "SPB:TRADE:B", trade.toByteArray());
        ConsumerRecord<String, byte[]> rec3 = new ConsumerRecord<>("market.raw", 0, 3L, "UNKNOWN:TRADE:C", trade.toByteArray());
        ConsumerRecord<String, byte[]> rec4 = new ConsumerRecord<>("market.raw", 0, 4L, null, trade.toByteArray());
        ConsumerRecord<String, byte[]> rec5 = new ConsumerRecord<>("market.raw", 0, 5L, "JUSTKEY", trade.toByteArray());

        consumer.consumeBatch(List.of(rec1, rec2, rec3, rec4, rec5));

        @SuppressWarnings("unchecked")
        ArgumentCaptor<List<MarketTradeRecord>> captor = ArgumentCaptor.forClass(List.class);
        verify(tradeRepository).saveAll(captor.capture());

        List<MarketTradeRecord> saved = captor.getValue();
        assertThat(saved).hasSize(5);
        assertThat(saved.get(0).exchange()).isEqualTo("MOEX");
        assertThat(saved.get(1).exchange()).isEqualTo("SPB");
        assertThat(saved.get(2).exchange()).isEqualTo("UNKNOWN");
        assertThat(saved.get(3).exchange()).isEqualTo("UNKNOWN");
        assertThat(saved.get(4).exchange()).isEqualTo("JUSTKEY");
    }

    @Test
    @DisplayName("Обрабатывает сделку без цены и времени с безопасными значениями по умолчанию")
    void shouldHandleMissingPriceAndTime() {
        Trade trade = Trade.newBuilder()
                .setTicker("GAZP")
                .setQuantity(1)
                .build();

        ConsumerRecord<String, byte[]> record = new ConsumerRecord<>(
                "market.raw", 0, 15L, "MOEX:TRADE:GAZP", trade.toByteArray()
        );

        consumer.consumeBatch(List.of(record));

        @SuppressWarnings("unchecked")
        ArgumentCaptor<List<MarketTradeRecord>> captor = ArgumentCaptor.forClass(List.class);
        verify(tradeRepository).saveAll(captor.capture());

        MarketTradeRecord item = captor.getValue().getFirst();
        assertThat(item.price()).isEqualByComparingTo(BigDecimal.ZERO);
        assertThat(item.tradeTime()).isNotNull();
    }
}
