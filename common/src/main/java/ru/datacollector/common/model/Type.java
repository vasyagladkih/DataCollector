package ru.datacollector.common.model;

public enum     Type {
    TRADES, BOOK_TICKER, ORDER_BOOK;

    public static Type type(Command command) {
        if (command == null) return null;
        return switch (command) {
            case OrderBookCommand _ -> Type.ORDER_BOOK;
            case TradesCommand _ -> Type.TRADES;
            case BookTickerCommand _ -> Type.BOOK_TICKER;
            default -> throw new IllegalArgumentException("Unknown command: " + command);
        };
    }
}
