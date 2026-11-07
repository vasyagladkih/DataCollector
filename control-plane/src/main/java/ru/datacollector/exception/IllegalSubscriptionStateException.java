package ru.datacollector.exception;

public class IllegalSubscriptionStateException extends RuntimeException {
    public IllegalSubscriptionStateException(String message) {
        super(message);
    }
}
