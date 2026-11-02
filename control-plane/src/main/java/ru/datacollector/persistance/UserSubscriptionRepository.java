package ru.datacollector.persistance;

import org.springframework.data.jdbc.repository.query.Query;
import org.springframework.data.repository.CrudRepository;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import ru.datacollector.model.Status;
import ru.datacollector.persistance.entity.SubscriptionEntity;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Repository
public interface UserSubscriptionRepository extends CrudRepository<SubscriptionEntity, UUID> {

    @Query("SELECT * FROM subscriptions WHERE exchange = :exchange AND action = :action AND symbol = :symbol")
    Optional<SubscriptionEntity> findByExchangeAndActionAndSymbol(
            @Param("exchange") String exchange,
            @Param("action") String action,
            @Param("symbol") String symbol
    );

    @Query("SELECT * FROM subscriptions WHERE " +
           "(CAST(:exchange AS text) IS NULL OR exchange = :exchange) AND " +
           "(CAST(:action AS text) IS NULL OR action = :action) AND " +
           "(CAST(:status AS text) IS NULL OR status = :status)")
    List<SubscriptionEntity> findAll(
            @Param("exchange") String exchange,
            @Param("action") String action,
            @Param("status") Status status
    );
}
