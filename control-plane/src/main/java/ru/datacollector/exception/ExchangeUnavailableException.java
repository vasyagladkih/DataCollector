package ru.datacollector.exception;

public class ExchangeUnavailableException extends RuntimeException {

    @SuppressWarnings("unused")
    public ExchangeUnavailableException(String message) {
        super(message);
    }

    public ExchangeUnavailableException(String message, Throwable cause) {
        super(message, cause);
    }
}
