package ru.datacollector.repository;

import org.jspecify.annotations.NullMarked;
import org.springframework.data.jdbc.repository.query.Query;
import org.springframework.data.repository.CrudRepository;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import ru.datacollector.repository.entity.InstrumentEntity;

import java.util.List;
import java.util.Optional;

@Repository
@NullMarked
public interface InstrumentRepository extends CrudRepository<InstrumentEntity, String>, InstrumentCustomRepository {

    @Override
    Optional<InstrumentEntity> findById(String instrumentUid);

    @SuppressWarnings("SqlResolve")
    @Query("SELECT * FROM instruments WHERE ticker = :ticker LIMIT 1")
    Optional<InstrumentEntity> findByTicker(@Param("ticker") String ticker);

    @Override
    List<InstrumentEntity> findAll();
}
