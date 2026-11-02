package ru.connector.exchange.kukoin;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.WebSocketSession;
import reactor.test.StepVerifier;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.JsonNodeFactory;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DefaultDataBufferFactory;

import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@ExtendWith(OutputCaptureExtension.class)
class KucoinAdapterTest {

    private KucoinAdapter adapter;
    private WebSocketSession session;
    private static final String PING_JSON = JsonNodeFactory.instance.objectNode()
            .put("type", "ping").toString();
    private final DefaultDataBufferFactory bufferFactory = new DefaultDataBufferFactory();

    @BeforeEach
    void setUp() {
        adapter = new KucoinAdapter();
        session = mock(WebSocketSession.class);

        when(session.textMessage(anyString())).thenAnswer(invocation -> {
            String text = invocation.getArgument(0);
            WebSocketMessage msg = mock(WebSocketMessage.class);
            when(msg.getPayloadAsText()).thenReturn(text);
            return msg;
        });
    }

    private DataBuffer loadBuffer(String path) throws IOException {
        byte[] bytes = new ClassPathResource(path).getContentAsByteArray();
        return bufferFactory.wrap(bytes);
    }

    @Test
    @DisplayName("createPingStream: отправляет ping каждые 18 секунд по виртуальному времени")
    void shouldEmitPingEvery18Seconds() {
        StepVerifier.withVirtualTime(() -> adapter.createPingStream(session))
                .expectSubscription()
                .expectNoEvent(Duration.ofSeconds(17))
                .thenAwait(Duration.ofSeconds(1))
                .assertNext(msg -> assertEquals(PING_JSON, msg.getPayloadAsText()))
                .thenAwait(Duration.ofSeconds(18))
                .expectNextCount(1)
                .thenAwait(Duration.ofSeconds(36))
                .expectNextCount(2)
                .thenCancel()
                .verify();
    }

    @Test
    @DisplayName("isPayload: возвращает true для реального фрейма спотовых сделок KuCoin UTA")
    void shouldReturnTrueForSpotTradePayload() throws IOException {
        DataBuffer buffer = loadBuffer("fixtures/kucoin/trade-spot.json");
        assertTrue(adapter.isPayload(buffer), "Спотовая сделка UTA должна распознаваться как полезная нагрузка");
    }

    @Test
    @DisplayName("isPayload: возвращает true для реального фрейма фьючерсных сделок KuCoin UTA")
    void shouldReturnTrueForFuturesTradePayload() throws IOException {
        DataBuffer buffer = loadBuffer("fixtures/kucoin/trade-futures.json");
        assertTrue(adapter.isPayload(buffer), "Фьючерсная сделка UTA должна распознаваться как полезная нагрузка");
    }

    @ParameterizedTest(name = "Служебный фрейм не должен быть payload: {0}")
    @ValueSource(strings = {
            "fixtures/kucoin/service-welcome.json",
            "fixtures/kucoin/service-pong.json",
            "fixtures/kucoin/service-ack.json",
            "fixtures/kucoin/service-error.json",
            "fixtures/kucoin/service-auth-failed.json",
            "fixtures/kucoin/service-error-topic.json"
    })
    @DisplayName("isPayload: возвращает false для служебных фреймов и ошибок")
    void shouldReturnFalseForServiceMessages(String fixturePath) throws IOException {
        DataBuffer buffer = loadBuffer(fixturePath);
        assertFalse(adapter.isPayload(buffer), "Служебный фрейм не должен распознаваться как полезная нагрузка: " + fixturePath);
    }

    @ParameterizedTest(name = "Штатный сервисный фрейм обязан распознаваться как normal: {0}")
    @ValueSource(strings = {
            "fixtures/kucoin/service-welcome.json",
            "fixtures/kucoin/service-pong.json",
            "fixtures/kucoin/service-ack.json"
    })

    @DisplayName("isNormalServiceMsg: возвращает true для штатных фреймов (welcome, pong, ack)")
    void shouldIdentifyNormalServiceMessages(String fixturePath) throws IOException {
        byte[] bytes = new ClassPathResource(fixturePath).getContentAsByteArray();
        JsonNode node = JsonMapper.builder().build().readTree(bytes);
        assertTrue(adapter.isNormalServiceMsg(node), "Фрейм должен быть распознан как штатный: " + fixturePath);
    }

    @ParameterizedTest(name = "Нештатный сервисный фрейм или ошибка не должны распознаваться как normal: {0}")
    @ValueSource(strings = {
            "fixtures/kucoin/service-error.json",
            "fixtures/kucoin/service-auth-failed.json",
            "fixtures/kucoin/service-error-topic.json"
    })
    @DisplayName("isNormalServiceMsg: возвращает false для ошибок биржи (error, code!=200, result:false)")
    void shouldIdentifyErrorServiceMessages(String fixturePath) throws IOException {
        byte[] bytes = new ClassPathResource(fixturePath).getContentAsByteArray();
        JsonNode node = JsonMapper.builder().build().readTree(bytes);
        assertFalse(adapter.isNormalServiceMsg(node), "Фрейм ошибки не должен считаться штатным: " + fixturePath);
    }

    @Test
    @DisplayName("isNormalServiceMsg: возвращает false для null и не-объектов")
    void shouldReturnFalseForInvalidNodes() {
        assertFalse(adapter.isNormalServiceMsg(null));
        assertFalse(adapter.isNormalServiceMsg(tools.jackson.databind.node.JsonNodeFactory.instance.stringNode("just a string")));
        assertFalse(adapter.isNormalServiceMsg(tools.jackson.databind.node.JsonNodeFactory.instance.arrayNode()));
    }

    @Test
    @DisplayName("handleServiceMsg: корректно вычитывает буфер штатного сообщения и не падает с ошибкой")
    void shouldHandleNormalServiceMsgWithoutError() throws IOException {
        DataBuffer buffer = loadBuffer("fixtures/kucoin/service-pong.json");
        int initialCount = buffer.readableByteCount();
        assertTrue(initialCount > 0);

        assertDoesNotThrow(() -> adapter.handleServiceMsg(buffer));
        assertEquals(0, buffer.readableByteCount(), "Буфер должен быть полностью вычитан");
    }

    @Test
    @DisplayName("handleServiceMsg: корректно вычитывает буфер сообщения об ошибке и не выбрасывает исключений")
    void shouldHandleErrorServiceMsgWithoutError() throws IOException {
        DataBuffer buffer = loadBuffer("fixtures/kucoin/service-error.json");
        int initialCount = buffer.readableByteCount();
        assertTrue(initialCount > 0);

        assertDoesNotThrow(() -> adapter.handleServiceMsg(buffer));
        assertEquals(0, buffer.readableByteCount(), "Буфер ошибки должен быть полностью вычитан");
    }

    @Test
    @DisplayName("handleServiceMsg: безопасно обрабатывает поврежденный не-JSON буфер")
    void shouldHandleCorruptedBufferSafely() {
        DataBuffer buffer = bufferFactory.wrap("corrupted {not valid json".getBytes(StandardCharsets.UTF_8));
        assertDoesNotThrow(() -> adapter.handleServiceMsg(buffer));
        assertEquals(0, buffer.readableByteCount(), "Буфер должен быть вычитан даже при ошибке парсинга");
    }

    @Test
    @DisplayName("isPayload: возвращает true для сделки с другим порядком полей (поле T не первое)")
    void shouldReturnTrueForReorderedTradePayload() throws IOException {
        DataBuffer buffer = loadBuffer("fixtures/kucoin/trade-reordered.json");
        assertTrue(adapter.isPayload(buffer), "Сделка с другим порядком полей должна распознаваться как полезная нагрузка");
    }

    @Test
    @DisplayName("isPayload: возвращает true для сделки с пробелами после фигурной скобки")
    void shouldReturnTrueForSpacedTradePayload() throws IOException {
        DataBuffer buffer = loadBuffer("fixtures/kucoin/trade-spaced.json");
        assertTrue(adapter.isPayload(buffer), "Сделка с пробелами должна распознаваться как полезная нагрузка");
    }

    @Test
    @DisplayName("isPayload: возвращает false для ошибки биржи, содержащей строку топика в data (защита от False Positive)")
    void shouldReturnFalseForErrorContainingTopic() throws IOException {
        DataBuffer buffer = loadBuffer("fixtures/kucoin/service-error-with-topic.json");
        assertFalse(adapter.isPayload(buffer), "Ошибка с цитатой топика в поле data НЕ должна распознаваться как сделка");
    }

    @Test
    @DisplayName("isPayload: возвращает false для пустого буфера и буфера короче 5 байт")
    void shouldReturnFalseForEmptyOrShortBuffer() {
        DataBuffer empty = bufferFactory.wrap(new byte[0]);
        assertFalse(adapter.isPayload(empty), "Пустой буфер не должен распознаваться как payload");

        DataBuffer shortBuffer = bufferFactory.wrap("{}".getBytes(StandardCharsets.UTF_8));
        assertFalse(adapter.isPayload(shortBuffer), "Буфер короче 5 байт не должен распознаваться как payload");
    }

    @Test
    @DisplayName("isNormalServiceMsg: возвращает true для type == ack и type == welcome")
    void shouldReturnTrueForTypeAckAndWelcome() {
        JsonNode ackNode = JsonMapper.builder().build().createObjectNode()
                .put("id", "123").put("type", "ack");
        assertTrue(adapter.isNormalServiceMsg(ackNode), "type: ack должен распознаваться как штатный");

        JsonNode welcomeNode = JsonMapper.builder().build().createObjectNode()
                .put("type", "welcome");
        assertTrue(adapter.isNormalServiceMsg(welcomeNode), "type: welcome должен распознаваться как штатный");
    }

    @Test
    @DisplayName("handleServiceMsg: логирует штатное сообщение в DEBUG и не логирует в WARN")
    void shouldLogNormalServiceMsgToDebug(CapturedOutput output) throws IOException {
        DataBuffer buffer = loadBuffer("fixtures/kucoin/service-pong.json");
        adapter.handleServiceMsg(buffer);
        assertFalse(output.getOut().contains("[KUCOIN-WARN]"), "Штатное сообщение не должно попадать в WARN");
    }

    @Test
    @DisplayName("handleServiceMsg: логирует сообщение об ошибке биржи в WARN")
    void shouldLogErrorServiceMsgToWarn(CapturedOutput output) throws IOException {
        DataBuffer buffer = loadBuffer("fixtures/kucoin/service-error.json");
        adapter.handleServiceMsg(buffer);
        assertTrue(output.getOut().contains("[KUCOIN-WARN]"), "Сообщение об ошибке обязано логироваться в WARN");
    }

    @Test
    @DisplayName("handleServiceMsg: логирует битый не-JSON буфер в ERROR")
    void shouldLogCorruptedBufferToError(CapturedOutput output) {
        DataBuffer buffer = bufferFactory.wrap("corrupted {not valid json".getBytes(StandardCharsets.UTF_8));
        adapter.handleServiceMsg(buffer);
        assertTrue(output.getOut().contains("[KUCOIN-ERROR]"), "Битый буфер обязан логироваться в ERROR");
    }

    @Test
    @DisplayName("isPayload: не смещает readPosition и не уменьшает readableByteCount буфера")
    void shouldPreserveBufferReadPositionAndReadableBytes() throws IOException {
        DataBuffer buffer = loadBuffer("fixtures/kucoin/trade-spot.json");
        int initialPos = buffer.readPosition();
        int initialBytes = buffer.readableByteCount();

        boolean result = adapter.isPayload(buffer);

        assertTrue(result, "Спотовая сделка должна быть распознана как payload");
        assertEquals(initialPos, buffer.readPosition(), "readPosition не должен изменяться после вызова isPayload");
        assertEquals(initialBytes, buffer.readableByteCount(), "readableByteCount не должен уменьшаться после вызова isPayload");

        byte[] bytes = new byte[buffer.readableByteCount()];
        buffer.read(bytes);
        String json = new String(bytes, StandardCharsets.UTF_8);
        assertTrue(json.startsWith("{\"T\":"), "Содержимое буфера не должно быть повреждено или урезано");
    }

    @Test
    @DisplayName("isPayload: возвращает false для JSON-массива вместо объекта")
    void shouldReturnFalseForArrayJson() {
        DataBuffer buffer = bufferFactory.wrap("[{\"T\":\"trade.SPOT\"}]".getBytes(StandardCharsets.UTF_8));
        assertFalse(adapter.isPayload(buffer), "JSON-массив не должен распознаваться как полезная нагрузка");
    }

    @Test
    @DisplayName("isPayload: возвращает false если поле T находится внутри вложенного объекта")
    void shouldReturnFalseForNestedTField() {
        DataBuffer buffer = bufferFactory.wrap("{\"meta\":{\"T\":\"trade.SPOT\"},\"id\":\"123\"}".getBytes(StandardCharsets.UTF_8));
        assertFalse(adapter.isPayload(buffer), "Вложенное поле T не должно распознаваться как полезная нагрузка");
    }

    @Test
    @DisplayName("isPayload: возвращает false для скалярных значений JSON (строка, число)")
    void shouldReturnFalseForScalarJson() {
        DataBuffer stringBuffer = bufferFactory.wrap("\"just a string\"".getBytes(StandardCharsets.UTF_8));
        assertFalse(adapter.isPayload(stringBuffer), "Скалярная строка не должна распознаваться как полезная нагрузка");

        DataBuffer numberBuffer = bufferFactory.wrap("123456789".getBytes(StandardCharsets.UTF_8));
        assertFalse(adapter.isPayload(numberBuffer), "Скалярное число не должно распознаваться как полезная нагрузка");
    }

    @Test
    @DisplayName("isRecoverable: возвращает true для сетевых ошибок, таймаутов и сбоев подключения")
    void shouldReturnTrueForNetworkErrors() {
        assertTrue(adapter.isRecoverable(new java.io.IOException("Connection reset")));
        assertTrue(adapter.isRecoverable(new java.net.SocketException("Broken pipe")));
        assertTrue(adapter.isRecoverable(new java.util.concurrent.TimeoutException("Read timed out")));
        assertTrue(adapter.isRecoverable(new ru.connector.exceptions.ExchangeConnectionException("Connection refused")));
        assertTrue(adapter.isRecoverable(new RuntimeException("Wrapped", new java.io.IOException("Inner IO"))));
    }

    @Test
    @DisplayName("isRecoverable: возвращает false для null, логических ошибок и NPE")
    void shouldReturnFalseForNonNetworkErrors() {
        assertFalse(adapter.isRecoverable(null));
        assertFalse(adapter.isRecoverable(new NullPointerException("NPE")));
        assertFalse(adapter.isRecoverable(new IllegalArgumentException("Invalid argument")));
        assertFalse(adapter.isRecoverable(new IllegalStateException("Illegal state")));
    }
}