package ru.datacollector.mapper;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import ru.datacollector.repository.entity.InstrumentEntity;
import ru.tinkoff.piapi.contract.v1.Bond;
import ru.tinkoff.piapi.contract.v1.Currency;
import ru.tinkoff.piapi.contract.v1.Etf;
import ru.tinkoff.piapi.contract.v1.Future;
import ru.tinkoff.piapi.contract.v1.InstrumentShort;
import ru.tinkoff.piapi.contract.v1.Share;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

class InstrumentEntityMapperTest {

    private final InstrumentEntityMapper mapper = new InstrumentEntityMapper();
    private final Instant now = Instant.parse("2026-11-14T20:00:00Z");

    @Test
    @DisplayName("Корректно маппит Share в InstrumentEntity")
    void shouldMapShareToEntity() {
        Share share = Share.newBuilder()
                .setUid("uid-share-1")
                .setTicker("SBER")
                .setClassCode("TQBR")
                .setName("Сбербанк")
                .setLot(10)
                .setCurrency("rub")
                .setApiTradeAvailableFlag(true)
                .build();

        InstrumentEntity entity = mapper.toEntity(share, now);

        assertThat(entity.instrumentUid()).isEqualTo("uid-share-1");
        assertThat(entity.ticker()).isEqualTo("SBER");
        assertThat(entity.classCode()).isEqualTo("TQBR");
        assertThat(entity.instrumentType()).isEqualTo("share");
        assertThat(entity.name()).isEqualTo("Сбербанк");
        assertThat(entity.lot()).isEqualTo(10);
        assertThat(entity.currency()).isEqualTo("rub");
        assertThat(entity.isTradable()).isTrue();
        assertThat(entity.updatedAt()).isEqualTo(now);
    }

    @Test
    @DisplayName("Корректно маппит Future в InstrumentEntity")
    void shouldMapFutureToEntity() {
        Future future = Future.newBuilder()
                .setUid("uid-future-1")
                .setTicker("SiZ4")
                .setClassCode("SPBFUT")
                .setName("Фьючерс Si")
                .setLot(1)
                .setCurrency("rub")
                .setApiTradeAvailableFlag(true)
                .build();

        InstrumentEntity entity = mapper.toEntity(future, now);

        assertThat(entity.instrumentUid()).isEqualTo("uid-future-1");
        assertThat(entity.ticker()).isEqualTo("SiZ4");
        assertThat(entity.classCode()).isEqualTo("SPBFUT");
        assertThat(entity.instrumentType()).isEqualTo("futures");
        assertThat(entity.name()).isEqualTo("Фьючерс Si");
        assertThat(entity.lot()).isEqualTo(1);
        assertThat(entity.currency()).isEqualTo("rub");
        assertThat(entity.isTradable()).isTrue();
        assertThat(entity.updatedAt()).isEqualTo(now);
    }

    @Test
    @DisplayName("Корректно маппит Bond в InstrumentEntity")
    void shouldMapBondToEntity() {
        Bond bond = Bond.newBuilder()
                .setUid("uid-bond-1")
                .setTicker("SU26238RMFS4")
                .setClassCode("TQOB")
                .setName("ОФЗ 26238")
                .setLot(1)
                .setCurrency("rub")
                .setApiTradeAvailableFlag(false)
                .build();

        InstrumentEntity entity = mapper.toEntity(bond, now);

        assertThat(entity.instrumentUid()).isEqualTo("uid-bond-1");
        assertThat(entity.ticker()).isEqualTo("SU26238RMFS4");
        assertThat(entity.classCode()).isEqualTo("TQOB");
        assertThat(entity.instrumentType()).isEqualTo("bond");
        assertThat(entity.isTradable()).isFalse();
    }

    @Test
    @DisplayName("Корректно маппит Currency в InstrumentEntity")
    void shouldMapCurrencyToEntity() {
        Currency currency = Currency.newBuilder()
                .setUid("uid-cur-1")
                .setTicker("USDRUB")
                .setClassCode("CETS")
                .setName("Доллар США")
                .setLot(1000)
                .setCurrency("rub")
                .setApiTradeAvailableFlag(true)
                .build();

        InstrumentEntity entity = mapper.toEntity(currency, now);

        assertThat(entity.instrumentUid()).isEqualTo("uid-cur-1");
        assertThat(entity.ticker()).isEqualTo("USDRUB");
        assertThat(entity.classCode()).isEqualTo("CETS");
        assertThat(entity.instrumentType()).isEqualTo("currency");
        assertThat(entity.lot()).isEqualTo(1000);
    }

    @Test
    @DisplayName("Корректно маппит Etf в InstrumentEntity")
    void shouldMapEtfToEntity() {
        Etf etf = Etf.newBuilder()
                .setUid("uid-etf-1")
                .setTicker("TMOS")
                .setClassCode("TQTF")
                .setName("Тинькофф iMOEX")
                .setLot(1)
                .setCurrency("rub")
                .setApiTradeAvailableFlag(true)
                .build();

        InstrumentEntity entity = mapper.toEntity(etf, now);

        assertThat(entity.instrumentUid()).isEqualTo("uid-etf-1");
        assertThat(entity.ticker()).isEqualTo("TMOS");
        assertThat(entity.classCode()).isEqualTo("TQTF");
        assertThat(entity.instrumentType()).isEqualTo("etf");
    }

    @Test
    @DisplayName("Корректно маппит InstrumentShort в InstrumentEntity")
    void shouldMapInstrumentShortToEntity() {
        InstrumentShort shortInst = InstrumentShort.newBuilder()
                .setUid("uid-short-1")
                .setTicker("GAZP")
                .setClassCode("TQBR")
                .setInstrumentType("share")
                .setName("Газпром")
                .setLot(10)
                .setApiTradeAvailableFlag(true)
                .build();

        InstrumentEntity entity = mapper.toEntity(shortInst, now);

        assertThat(entity.instrumentUid()).isEqualTo("uid-short-1");
        assertThat(entity.ticker()).isEqualTo("GAZP");
        assertThat(entity.classCode()).isEqualTo("TQBR");
        assertThat(entity.instrumentType()).isEqualTo("share");
        assertThat(entity.name()).isEqualTo("Газпром");
        assertThat(entity.currency()).isEqualTo("rub");
    }

    @Test
    @DisplayName("Компактный конструктор InstrumentEntity нормализует lot и updatedAt")
    void shouldNormalizeLotAndUpdatedAtInCompactConstructor() {
        Instant specificTime = Instant.parse("2026-01-01T00:00:00Z");

        InstrumentEntity entityWithDefaults = new InstrumentEntity(
                "uid-1",
                "TICKER",
                "TQBR",
                "share",
                "Name",
                -5,
                "rub",
                true,
                null
        );

        assertThat(entityWithDefaults.lot()).isEqualTo(1);
        assertThat(entityWithDefaults.updatedAt()).isNotNull();

        InstrumentEntity entityWithExplicitValues = new InstrumentEntity(
                "uid-2",
                "TICKER",
                "TQBR",
                "share",
                "Name",
                10,
                "usd",
                true,
                specificTime
        );

        assertThat(entityWithExplicitValues.lot()).isEqualTo(10);
        assertThat(entityWithExplicitValues.updatedAt()).isEqualTo(specificTime);
    }
}
