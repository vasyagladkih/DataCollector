package ru.datacollector.dto;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.time.Instant;
import java.util.UUID;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record SubscriptionResponse(
        UUID id,
        String exchange,
        String channel,
        String symbol,
        String status,
        String errorMessage,
        Instant createdAt,
        Instant updatedAt
) {}
