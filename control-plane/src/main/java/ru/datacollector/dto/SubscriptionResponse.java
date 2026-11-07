package ru.datacollector.dto;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.time.Instant;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record SubscriptionResponse(
        Long id,
        String exchange,
        String channel,
        String symbol,
        String status,
        String errorMessage,
        Instant createdAt,
        Instant updatedAt
) {}
