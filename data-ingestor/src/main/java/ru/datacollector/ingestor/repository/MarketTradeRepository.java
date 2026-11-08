package ru.datacollector.ingestor.repository;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;
import ru.datacollector.ingestor.model.MarketTradeRecord;

import java.sql.Timestamp;
import java.util.List;

/**
 * Репозиторий батчевой вставки рыночных сделок в TimescaleDB.
 */
@Slf4j
@Repository
@RequiredArgsConstructor
public class MarketTradeRepository {

    @SuppressWarnings("SqlResolve")
    private static final String INSERT_SQL = """
            INSERT INTO market_trades (exchange, symbol, ticker, price, quantity, trade_time)
            VALUES (?, ?, ?, ?, ?, ?)
            """;

    private final JdbcTemplate jdbcTemplate;

    /**
     * Выполняет пакетную вставку сделок в гипертаблицу TimescaleDB.
     *
     * @param trades список сделок для сохранения
     * @return количество сохраненных записей
     */
    @Transactional
    public int saveAll(List<MarketTradeRecord> trades) {
        if (trades == null || trades.isEmpty()) {
            return 0;
        }

        int[][] updateCounts = jdbcTemplate.batchUpdate(
                INSERT_SQL,
                trades,
                trades.size(),
                (ps, trade) -> {
                    ps.setString(1, trade.exchange());
                    ps.setString(2, trade.symbol());
                    ps.setString(3, trade.ticker());
                    ps.setBigDecimal(4, trade.price());
                    ps.setLong(5, trade.quantity());
                    ps.setTimestamp(6, Timestamp.from(trade.tradeTime()));
                }
        );

        int totalSaved = 0;
        for (int[] batch : updateCounts) {
            for (int count : batch) {
                if (count >= 0) {
                    totalSaved += count;
                }
            }
        }

        log.debug("Сохранено {} сделок в TimescaleDB", totalSaved);
        return totalSaved;
    }
}
