package ru.datacollector.ingestor.repository;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ParameterizedPreparedStatementSetter;
import ru.datacollector.ingestor.model.MarketTradeRecord;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class MarketTradeRepositoryTest {

    @Mock
    private JdbcTemplate jdbcTemplate;

    @InjectMocks
    private MarketTradeRepository repository;

    @Test
    @DisplayName("Возвращает 0 и не вызывает jdbcTemplate для пустого или null списка")
    void shouldReturnZeroForEmptyOrNullList() {
        assertThat(repository.saveAll(null)).isZero();
        assertThat(repository.saveAll(Collections.emptyList())).isZero();

        verifyNoInteractions(jdbcTemplate);
    }

    @Test
    @DisplayName("Корректно передает батч сделок и заполняет параметры PreparedStatement")
    @SuppressWarnings("unchecked")
    void shouldSaveAllTradesSuccessfully() throws SQLException {
        Instant now = Instant.parse("2026-11-08T18:00:00Z");
        MarketTradeRecord record = new MarketTradeRecord(
                "MOEX",
                "uid-123",
                "SBER",
                new BigDecimal("280.50"),
                10L,
                now
        );
        List<MarketTradeRecord> trades = List.of(record);

        int[][] mockCounts = new int[][]{{1}};
        when(jdbcTemplate.batchUpdate(
                anyString(),
                anyList(),
                anyInt(),
                any(ParameterizedPreparedStatementSetter.class)
        )).thenReturn(mockCounts);

        int saved = repository.saveAll(trades);
        assertThat(saved).isEqualTo(1);

        ArgumentCaptor<ParameterizedPreparedStatementSetter<MarketTradeRecord>> setterCaptor =
                ArgumentCaptor.forClass(ParameterizedPreparedStatementSetter.class);

        verify(jdbcTemplate).batchUpdate(
                eq("""
            INSERT INTO market_trades (exchange, symbol, ticker, price, quantity, trade_time)
            VALUES (?, ?, ?, ?, ?, ?)
            """),
                eq(trades),
                eq(1),
                setterCaptor.capture()
        );

        PreparedStatement ps = mock(PreparedStatement.class);
        setterCaptor.getValue().setValues(ps, record);

        verify(ps).setString(1, "MOEX");
        verify(ps).setString(2, "uid-123");
        verify(ps).setString(3, "SBER");
        verify(ps).setBigDecimal(4, new BigDecimal("280.50"));
        verify(ps).setLong(5, 10L);
        verify(ps).setTimestamp(6, Timestamp.from(now));
    }
}
