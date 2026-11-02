package ru.datacollector.exceptions;

public class IllegalSubscriptionStateException extends RuntimeException {
    public IllegalSubscriptionStateException(String message) {
        super(message);
    }
}
