package ru.connector.exchange.kukoin;

import lombok.extern.slf4j.Slf4j;
import org.springframework.core.NestedExceptionUtils;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.WebSocketSession;
import reactor.core.publisher.Flux;
import ru.connector.exceptions.ExchangeConnectionException;
import ru.connector.exchange.ExchangeAdapter;
import tools.jackson.core.JsonParser;
import tools.jackson.core.JsonToken;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.JsonNodeFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.TimeoutException;

/**
 * Адаптер биржи KuCoin для обработки служебных сообщений (ping/pong, welcome, ack).
 */
@Slf4j
public class KucoinAdapter implements ExchangeAdapter {

    private static final int PING_INTERVAL = 18;
    private static final int MAX_SUBSCRIPTIONS = 100;

    private static final String PING_JSON = JsonNodeFactory.instance.objectNode()
            .put("type", "ping").toString();

    private final JsonMapper jsonMapper;

    public KucoinAdapter() {
        this(JsonMapper.builder().build());
    }

    public KucoinAdapter(JsonMapper jsonMapper) {this.jsonMapper = jsonMapper;}

    @Override
    public Flux<WebSocketMessage> createPingStream(WebSocketSession session) {
        return Flux.interval(Duration.ofSeconds(PING_INTERVAL))
                .map(seq -> session.textMessage(PING_JSON));
    }

    @Override
    public boolean isPayload(DataBuffer buffer) {
        if (buffer == null || buffer.readableByteCount() == 0) return false;

        int startPos = buffer.readPosition();
        try (JsonParser parser = jsonMapper.createParser(buffer.asInputStream())) {
            if (parser.nextToken() != JsonToken.START_OBJECT) return false;

            while (parser.nextToken() == JsonToken.PROPERTY_NAME) {
                String fieldName = parser.currentName();
                if ("T".equals(fieldName)) return true;
                parser.nextToken();
                parser.skipChildren();
            }
            return false;
        } catch (Exception _) {
            return false;
        } finally {
            buffer.readPosition(startPos);
        }
    }

    @Override
    public void handleServiceMsg(DataBuffer buffer) {
        try {
            byte[] bytes = new byte[buffer.readableByteCount()];
            buffer.read(bytes);
            JsonNode root = jsonMapper.readTree(bytes);

            if (isNormalServiceMsg(root)) {
                log.debug("[KUCOIN-SERVICE] {}", root);
            } else {
                log.warn("[KUCOIN-WARN] Нештатное или ошибочное сервисное сообщение: {}", root);
            }
        } catch (Exception e) {
            log.error("[KUCOIN-ERROR] Ошибка обработки сервисного сообщения", e);
        }
    }

    public boolean isNormalServiceMsg(JsonNode node) {
        if (node == null || !node.isObject()) return false;

        String op = node.path("op").asString();
        String type = node.path("type").asString();
        if ("pong".equalsIgnoreCase(op) || "pong".equalsIgnoreCase(type)
                || "welcome".equalsIgnoreCase(type) || "ack".equalsIgnoreCase(type)) {
            log.debug("[WS] Получен текстовый PONG фрейм");
            return true;
        }

        String message = node.path("message").asString();
        String data = node.path("data").asString();
        if ("welcome".equalsIgnoreCase(message) || "welcome".equalsIgnoreCase(data)) {
            log.debug("[WS] Получен текстовый WELCOME фрейм");
            return true;
        }

        JsonNode resultNode = node.get("result");
        if (resultNode != null) {
            log.debug("[WS] Получен текстовый ACK фрейм");
            return resultNode.asBoolean(false) || "true".equalsIgnoreCase(resultNode.asString());
        }
        return false;
    }

    @Override
    public boolean isRecoverable(Throwable error) {
        if (error == null) return false;

        Throwable root = NestedExceptionUtils.getMostSpecificCause(error);
        return root instanceof IOException
               || root instanceof TimeoutException
               || root instanceof ExchangeConnectionException;
    }

    @Override
    public int getMaxSubscriptionsPerConnection() {
        return MAX_SUBSCRIPTIONS;
    }
}