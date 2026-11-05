package ru.datacollector.service;

import ru.datacollector.common.model.Action;
import ru.datacollector.common.model.StreamKey;

import java.net.http.WebSocket;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

public class ExchangeConnection {
    private final StreamKey streamKey;
    private final int capacity;
    private final AtomicInteger load = new AtomicInteger(0);
    private final Consumer<ExchangeConnection> reconnectCallback;
    private WebSocket ws;

    public ExchangeConnection(StreamKey streamKey, int capacity, Consumer<ExchangeConnection> reconnectCallback) {
        this.streamKey = streamKey;
        this.capacity = capacity;
        this.reconnectCallback = reconnectCallback;
    }

    public boolean tryAcquireSlot() {
        return load.getAndUpdate(cur -> cur < capacity ? cur + 1 : cur) < capacity;
    }

    public int releaseSlot() {
        return load.decrementAndGet();
    }

    public void send(String symbol, Action command) {
        // ws.sendText(...) с командой SUBSCRIBE / UNSUBSCRIBE под symbol
    }

    public void close() {
        // ws.sendClose(...)
    }

    public boolean isAlive() {
        return ws != null && !ws.isInputClosed() && !ws.isOutputClosed();
    }

    private void onWsTerminated() {
        // reconnect...
        // Сообщаем сервису: "Я (this) упал и поднялся, восстанови мои подписки"
        reconnectCallback.accept(this);
    }
}
