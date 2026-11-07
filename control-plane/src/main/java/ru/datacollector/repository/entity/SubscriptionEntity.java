package ru.datacollector.repository.entity;

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
        Instant updatedAt,
        @Column("ref_count")
        int refCount
) {

    public SubscriptionEntity withDecrementedRef() {
        int updatedRefCount = Math.max(0, this.refCount - 1);
        Status newStatus = (updatedRefCount == 0) ? Status.TERMINATED : this.status;
        return new SubscriptionEntity(
                this.id,
                this.exchange,
                this.channel,
                this.symbol,
                newStatus,
                null,
                this.createdAt,
                Instant.now(),
                updatedRefCount
        );
    }

    public SubscriptionEntity withRestarting() {
        return new SubscriptionEntity(
                this.id,
                this.exchange,
                this.channel,
                this.symbol,
                Status.CONNECTING,
                null,
                this.createdAt,
                Instant.now(),
                this.refCount
        );
    }

    public SubscriptionEntity withStatus(Status newStatus) {
        return new SubscriptionEntity(
                this.id,
                this.exchange,
                this.channel,
                this.symbol,
                newStatus,
                this.errorMessage,
                this.createdAt,
                Instant.now(),
                this.refCount
        );
    }

    public SubscriptionEntity withActive() {
        return new SubscriptionEntity(
                this.id,
                this.exchange,
                this.channel,
                this.symbol,
                Status.ACTIVE,
                null,
                this.createdAt,
                Instant.now(),
                this.refCount
        );
    }

    public SubscriptionEntity withFailed(String errorMessage) {
        return new SubscriptionEntity(
                this.id,
                this.exchange,
                this.channel,
                this.symbol,
                Status.FAILED,
                errorMessage,
                this.createdAt,
                Instant.now(),
                this.refCount
        );
    }
}
