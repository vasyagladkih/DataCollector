package ru.datacollector.repository;

import org.springframework.data.jdbc.repository.query.Query;
import org.springframework.data.repository.CrudRepository;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import ru.datacollector.model.Status;
import ru.datacollector.repository.entity.SubscriptionEntity;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

@Repository
public interface SubscriptionRepository extends CrudRepository<SubscriptionEntity, Long> {

    @Query("SELECT * FROM subscriptions WHERE " +
           "(CAST(:exchange AS text) IS NULL OR exchange = :exchange) AND " +
           "(CAST(:channel AS text) IS NULL OR channel = :channel) AND " +
           "(CAST(:status AS text) IS NULL OR status = :status)")
    List<SubscriptionEntity> findAll(
            @Param("exchange") String exchange,
            @Param("channel") String channel,
            @Param("status") Status status
    );

    @Query("SELECT * FROM subscriptions WHERE status IN (:statuses) AND ref_count > 0")
    List<SubscriptionEntity> findPendingRecovery(@Param("statuses") Collection<Status> statuses);

    @Query("SELECT * FROM subscriptions WHERE id = :id FOR UPDATE")
    Optional<SubscriptionEntity> findByIdForUpdate(Long id);

    @Query("""
        INSERT INTO subscriptions (
            exchange, channel, symbol, status, error_message, ref_count, created_at, updated_at
        ) VALUES (
            :exchange, :channel, :symbol, 'CONNECTING', NULL, 1, NOW(), NOW()
        )
        ON CONFLICT (exchange, channel, symbol)
        DO UPDATE SET
            ref_count = subscriptions.ref_count + 1,
            status = CASE
                WHEN subscriptions.ref_count = 0 OR subscriptions.status = 'TERMINATED' THEN 'CONNECTING'
                ELSE subscriptions.status
            END,
            error_message = CASE
                WHEN subscriptions.ref_count = 0 OR subscriptions.status = 'TERMINATED' THEN NULL
                ELSE subscriptions.error_message
            END,
            updated_at = NOW()
        RETURNING *
    """)
    SubscriptionEntity upsertAndIncrement(
            @Param("exchange") String exchange,
            @Param("channel") String channel,
            @Param("symbol") String symbol
    );
}
