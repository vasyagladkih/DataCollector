package ru.datacollector.ingestor.model;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * Неизменяемая запись рыночной сделки для сохранения в гипертаблицу TimescaleDB.
 *
 * @param exchange  наименование биржи (MOEX, SPB, UNKNOWN)
 * @param symbol    идентификатор инструмента (instrument_uid или figi)
 * @param ticker    тикер инструмента
 * @param price     цена совершения сделки
 * @param quantity  объем сделки в лотах/штуках
 * @param tradeTime метка времени совершения сделки
 */
public record MarketTradeRecord(
        String exchange,
        String symbol,
        String ticker,
        BigDecimal price,
        long quantity,
        Instant tradeTime
) {}
