package ru.connector.exchange;

import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.WebSocketSession;
import reactor.core.publisher.Flux;

public interface ExchangeAdapter {
    Flux<WebSocketMessage> createPingStream(WebSocketSession session);
    boolean isPayload(DataBuffer buffer);
    void handleServiceMsg(DataBuffer buffer);
    boolean isRecoverable(Throwable error);

    /**
     * Максимальное количество активных подписок на одно WebSocket-соединение биржи.
     * По умолчанию возвращает 100.
     */
    default int getMaxSubscriptionsPerConnection() {
        return 100;
    }
}