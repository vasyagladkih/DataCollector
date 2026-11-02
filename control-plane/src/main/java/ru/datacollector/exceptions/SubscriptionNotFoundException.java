package ru.datacollector.exceptions;

import java.util.UUID;

public class SubscriptionNotFoundException extends RuntimeException {
    public SubscriptionNotFoundException(UUID id) {
        super("Подписка с идентификатором " + id + " не найдена");
    }

    public SubscriptionNotFoundException(String message) {
        super(message);
    }
}
