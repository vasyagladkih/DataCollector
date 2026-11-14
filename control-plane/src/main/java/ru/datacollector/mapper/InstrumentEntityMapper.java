package ru.datacollector.mapper;

import org.springframework.stereotype.Component;
import ru.datacollector.repository.entity.InstrumentEntity;
import ru.tinkoff.piapi.contract.v1.Bond;
import ru.tinkoff.piapi.contract.v1.Currency;
import ru.tinkoff.piapi.contract.v1.Etf;
import ru.tinkoff.piapi.contract.v1.Future;
import ru.tinkoff.piapi.contract.v1.InstrumentShort;
import ru.tinkoff.piapi.contract.v1.Share;

import java.time.Instant;

@Component
public class InstrumentEntityMapper {

    public InstrumentEntity toEntity(Share s, Instant now) {
        return new InstrumentEntity(
                s.getUid(),
                s.getTicker(),
                s.getClassCode(),
                "share",
                s.getName(),
                s.getLot(),
                s.getCurrency(),
                s.getApiTradeAvailableFlag(),
                now
        );
    }

    public InstrumentEntity toEntity(Future f, Instant now) {
        return new InstrumentEntity(
                f.getUid(),
                f.getTicker(),
                f.getClassCode(),
                "futures",
                f.getName(),
                f.getLot(),
                f.getCurrency(),
                f.getApiTradeAvailableFlag(),
                now
        );
    }

    public InstrumentEntity toEntity(Bond b, Instant now) {
        return new InstrumentEntity(
                b.getUid(),
                b.getTicker(),
                b.getClassCode(),
                "bond",
                b.getName(),
                b.getLot(),
                b.getCurrency(),
                b.getApiTradeAvailableFlag(),
                now
        );
    }

    public InstrumentEntity toEntity(Currency c, Instant now) {
        return new InstrumentEntity(
                c.getUid(),
                c.getTicker(),
                c.getClassCode(),
                "currency",
                c.getName(),
                c.getLot(),
                c.getCurrency(),
                c.getApiTradeAvailableFlag(),
                now
        );
    }

    public InstrumentEntity toEntity(Etf e, Instant now) {
        return new InstrumentEntity(
                e.getUid(),
                e.getTicker(),
                e.getClassCode(),
                "etf",
                e.getName(),
                e.getLot(),
                e.getCurrency(),
                e.getApiTradeAvailableFlag(),
                now
        );
    }

    public InstrumentEntity toEntity(InstrumentShort s, Instant now) {
        return new InstrumentEntity(
                s.getUid(),
                s.getTicker(),
                s.getClassCode(),
                s.getInstrumentType(),
                s.getName(),
                s.getLot(),
                "rub",
                s.getApiTradeAvailableFlag(),
                now
        );
    }
}
