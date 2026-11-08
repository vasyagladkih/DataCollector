package ru.datacollector.repository;

import ru.datacollector.repository.entity.InstrumentEntity;

import java.util.List;

public interface InstrumentCustomRepository {

    void upsert(InstrumentEntity entity);

    void upsertAll(List<InstrumentEntity> entities);
}
