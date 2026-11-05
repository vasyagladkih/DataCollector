package ru.datacollector.dto;

import ru.datacollector.common.model.StreamKey;
import ru.datacollector.service.ExchangeConnection;

import java.util.UUID;

public record Subscription(
        UUID id,
        StreamKey streamKey,
        String symbol,
        ExchangeConnection connection
) {}