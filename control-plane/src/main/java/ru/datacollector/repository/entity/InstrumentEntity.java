package ru.datacollector.repository.entity;

import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Column;
import org.springframework.data.relational.core.mapping.Table;

import java.time.Instant;

@Table("instruments")
public record InstrumentEntity(
        @Id
        @Column("instrument_uid")
        String instrumentUid,
        @Column("ticker")
        String ticker,
        @Column("class_code")
        String classCode,
        @Column("instrument_type")
        String instrumentType,
        @Column("name")
        String name,
        @Column("lot")
        int lot,
        @Column("currency")
        String currency,
        @Column("is_tradable")
        boolean isTradable,
        @Column("updated_at")
        Instant updatedAt
) {}
