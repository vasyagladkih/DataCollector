package ru.datacollector.dto;

import jakarta.validation.constraints.NotNull;

public record CreateSubscriptionRequest(
        @NotNull(message = "Значение биржи не должно быть пустым")
        String exchange,
        @NotNull(message = "Значение валюты не должно быть пустым")
        String symbol,
        @NotNull(message = "Значение типа собираемых данных не должно быть пустым")
        String channel
) {}