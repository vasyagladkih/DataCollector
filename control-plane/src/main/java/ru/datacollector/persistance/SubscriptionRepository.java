package ru.datacollector.persistance;

import org.springframework.data.jdbc.repository.query.Query;
import org.springframework.data.repository.CrudRepository;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import ru.datacollector.model.Status;
import ru.datacollector.persistance.entity.SubscriptionEntity;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Repository
public interface SubscriptionRepository extends CrudRepository<SubscriptionEntity, UUID> {

    @Query("SELECT * FROM subscriptions WHERE exchange = :exchange AND channel = :channel AND symbol = :symbol")
    Optional<SubscriptionEntity> findByExchangeAndChannelAndSymbol(
            @Param("exchange") String exchange,
            @Param("channel") String channel,
            @Param("symbol") String symbol
    );

    @Query("SELECT * FROM subscriptions WHERE " +
           "(CAST(:exchange AS text) IS NULL OR exchange = :exchange) AND " +
           "(CAST(:channel AS text) IS NULL OR channel = :channel) AND " +
           "(CAST(:status AS text) IS NULL OR status = :status)")
    List<SubscriptionEntity> findAll(
            @Param("exchange") String exchange,
            @Param("channel") String channel,
            @Param("status") Status status
    );

    @Query("SELECT * FROM subscriptions WHERE status IN (:statuses)")
    List<SubscriptionEntity> findByStatusIn(@Param("statuses") Collection<Status> statuses);
}
