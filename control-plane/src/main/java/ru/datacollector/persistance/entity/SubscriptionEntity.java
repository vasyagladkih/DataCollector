package ru.datacollector.persistance.entity;

import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Column;
import org.springframework.data.relational.core.mapping.Table;
import ru.datacollector.model.Status;

import java.time.Instant;
import java.util.UUID;

@Table("subscriptions")
public record SubscriptionEntity(
        @Id
        UUID id,
        String exchange,
        @Column("channel")
        String channel,
        String symbol,
        Status status,
        @Column("error_message")
        String errorMessage,
        @Column("created_at")
        Instant createdAt,
        @Column("updated_at")
        Instant updatedAt
) {
    public SubscriptionEntity withStatus(Status newStatus) {
        return new SubscriptionEntity(
                this.id,
                this.exchange,
                this.channel,
                this.symbol,
                newStatus,
                this.errorMessage,
                this.createdAt,
                Instant.now()
        );
    }
}
