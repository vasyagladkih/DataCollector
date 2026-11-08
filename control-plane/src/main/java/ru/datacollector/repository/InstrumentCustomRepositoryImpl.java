package ru.datacollector.repository;

import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.stereotype.Repository;
import ru.datacollector.repository.entity.InstrumentEntity;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Repository
@RequiredArgsConstructor
public class InstrumentCustomRepositoryImpl implements InstrumentCustomRepository {

    @SuppressWarnings("SqlResolve")
    private static final String UPSERT_SQL = """
        INSERT INTO instruments (instrument_uid, ticker, class_code, instrument_type, name, lot, currency, is_tradable, updated_at)
        VALUES (:instrumentUid, :ticker, :classCode, :instrumentType, :name, :lot, :currency, :isTradable, :updatedAt)
        ON CONFLICT (instrument_uid) DO UPDATE SET
            ticker = EXCLUDED.ticker,
            class_code = EXCLUDED.class_code,
            instrument_type = EXCLUDED.instrument_type,
            name = EXCLUDED.name,
            lot = EXCLUDED.lot,
            currency = EXCLUDED.currency,
            is_tradable = EXCLUDED.is_tradable,
            updated_at = EXCLUDED.updated_at
        """;

    private final NamedParameterJdbcTemplate jdbcTemplate;

    @Override
    public void upsert(InstrumentEntity entity) {
        jdbcTemplate.update(UPSERT_SQL, createParams(entity));
    }

    @Override
    public void upsertAll(List<InstrumentEntity> entities) {
        if (entities == null || entities.isEmpty()) {
            return;
        }

        Map<String, InstrumentEntity> unique = new LinkedHashMap<>();
        for (InstrumentEntity entity : entities) {
            if (entity != null && entity.instrumentUid() != null) {
                unique.put(entity.instrumentUid(), entity);
            }
        }

        SqlParameterSource[] batch = unique.values().stream()
                .map(this::createParams)
                .toArray(SqlParameterSource[]::new);

        jdbcTemplate.batchUpdate(UPSERT_SQL, batch);
    }

    private SqlParameterSource createParams(InstrumentEntity entity) {
        Instant updatedAt = entity.updatedAt() != null ? entity.updatedAt() : Instant.now();
        return new MapSqlParameterSource()
                .addValue("instrumentUid", entity.instrumentUid())
                .addValue("ticker", entity.ticker())
                .addValue("classCode", entity.classCode())
                .addValue("instrumentType", entity.instrumentType())
                .addValue("name", entity.name())
                .addValue("lot", entity.lot())
                .addValue("currency", entity.currency())
                .addValue("isTradable", entity.isTradable())
                .addValue("updatedAt", Timestamp.from(updatedAt));
    }
}
