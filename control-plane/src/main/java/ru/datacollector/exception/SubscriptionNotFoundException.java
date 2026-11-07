package ru.datacollector.exception;

public class SubscriptionNotFoundException extends RuntimeException {
    public SubscriptionNotFoundException(Long id) {
        super("Подписка с идентификатором " + id + " не найдена");
    }
}
