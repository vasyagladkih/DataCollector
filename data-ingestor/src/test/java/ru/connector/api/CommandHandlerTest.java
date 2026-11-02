package ru.connector.api;

import tools.jackson.databind.json.JsonMapper;
import org.springframework.http.codec.json.JacksonJsonDecoder;
import org.springframework.http.codec.json.JacksonJsonEncoder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.reactivestreams.Publisher;
import org.springframework.core.io.ClassPathResource;
import org.springframework.http.MediaType;
import org.springframework.test.web.reactive.server.WebTestClient;
import org.springframework.web.reactive.function.BodyInserters;
import org.springframework.web.reactive.socket.WebSocketHandler;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.WebSocketSession;
import org.springframework.web.reactive.socket.client.WebSocketClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import ru.datacollector.common.model.CreateSubscriptionRequest;
import org.mapstruct.factory.Mappers;
import ru.connector.exceptions.ExchangeConnectionException;
import ru.connector.exchange.kukoin.KucoinManager;
import ru.connector.kafka.KafkaRawDataPublisher;
import ru.connector.kafka.KafkaSubscriptionPublisher;
import ru.connector.kafka.SubscriptionConsumer;
import ru.connector.mapper.IngestorCommandMapper;
import ru.connector.service.ExchangeService;
import ru.datacollector.common.mapper.SubscriptionResponseMapper;
import ru.datacollector.common.model.StreamKey;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@Deprecated
@ExtendWith(MockitoExtension.class)
class CommandHandlerTest {

    private WebTestClient client;

    @Mock
    private WebSocketClient mockWsClient;

    @Mock
    private KafkaSubscriptionPublisher mockKafkaPublisher;

    @Mock
    private KafkaRawDataPublisher mockRawPublisher;

    private List<String> sentWsFrames;
    private SubscriptionConsumer subscriptionConsumer;
    @BeforeEach
    void setUp() {
        sentWsFrames = new CopyOnWriteArrayList<>();

        lenient().when(mockWsClient.execute(any(URI.class), any(WebSocketHandler.class))).thenAnswer(invocation -> {
            WebSocketHandler handler = invocation.getArgument(1);
            WebSocketSession session = mock(WebSocketSession.class);

            when(session.receive()).thenReturn(Flux.never());
            when(session.send(any())).thenAnswer(sendInv -> {
                Publisher<WebSocketMessage> publisher = sendInv.getArgument(0);
                return Flux.from(publisher)
                        .doOnNext(msg -> sentWsFrames.add(msg.getPayloadAsText()))
                        .then();
            });
            when(session.textMessage(anyString())).thenAnswer(textInv -> {
                WebSocketMessage msg = mock(WebSocketMessage.class);
                when(msg.getPayloadAsText()).thenReturn(textInv.getArgument(0));
                return msg;
            });

            return handler.handle(session);
        });

        lenient().when(mockKafkaPublisher.publish(any(), any())).thenReturn(Mono.empty());
        lenient().when(mockKafkaPublisher.publishTombstone(any())).thenReturn(Mono.empty());

        // Сборка реального стека приложения
        JsonMapper jsonMapper = JsonMapper.builder().build();
        KucoinManager kucoinManager = new KucoinManager(mockWsClient, mockRawPublisher, jsonMapper);
        IngestorCommandMapper commandMapper = Mappers.getMapper(IngestorCommandMapper.class);
        SubscriptionResponseMapper responseMapper = Mappers.getMapper(SubscriptionResponseMapper.class);
        ExchangeService exchangeService = new ExchangeService(mockKafkaPublisher, Map.of("KUCOIN", kucoinManager), commandMapper, responseMapper);
        subscriptionConsumer = new SubscriptionConsumer(exchangeService);

        JsonMapper testJsonMapper = ru.connector.config.KafkaConfig.createJsonMapper();

        client = WebTestClient.bindToController(new CommandHandler(exchangeService))
                .controllerAdvice(new GlobalExceptionHandler())
                .httpMessageCodecs(configurer -> {
                    configurer.defaultCodecs().jacksonJsonDecoder(new JacksonJsonDecoder(testJsonMapper));
                    configurer.defaultCodecs().jacksonJsonEncoder(new JacksonJsonEncoder(testJsonMapper));
                })
                .build();
    }

    /**
     * Что проверяем: Успешное создание подписки на сделки BTC-USDT.
     * Что ждем:
     * - HTTP 202 Accepted и JSON с ID=StreamKey;
     * - Отправку события в Kafka (publish);
     * - Прием консьюмером и отправку кадра subscribe в сокет биржи;
     * - Появление записи в GET /api/subscriptions.
     */
    @Test
    @DisplayName("POST /api/subscriptions: Успешная регистрация и отправка фрейма в WS через Consumer")
    void shouldSubscribeSuccessfully() throws Exception {
        long expectedId = (long) ("KUCOIN:SPOT:BTC-USDT:TRADES".hashCode() & 0x7FFFFFFF);
        postFixture("fixtures/requests/subscribe-spot-btc.json")
                .expectStatus().isAccepted()
                .expectBody()
                .jsonPath("$.id").isEqualTo(expectedId)
                .jsonPath("$.exchange").isEqualTo("KUCOIN")
                .jsonPath("$.market").isEqualTo("SPOT")
                .jsonPath("$.symbol").isEqualTo("BTC-USDT");

        verify(mockKafkaPublisher, times(1)).publish(eq("KUCOIN:SPOT:BTC-USDT:TRADES"), any());

        simulateKafkaSubscription("fixtures/requests/subscribe-spot-btc.json");
        assertWsFrameSent("subscribe");

        getActiveSubscriptions()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.length()").isEqualTo(1)
                .jsonPath("$[0].id").isEqualTo(expectedId);
    }

    /**
     * Что проверяем: Повторный запрос той же подписки (идемпотентность).
     * Что ждем:
     * - HTTP 202 с тем же ID=StreamKey;
     * - БЕЗ повторной отправки фрейма в сокет при повторной вычитке из Kafka.
     */
    @Test
    @DisplayName("POST /api/subscriptions: Идемпотентность — повторный запрос возвращает тот же ID")
    void shouldBeIdempotentOnDuplicateSubscription() throws Exception {
        long expectedId = (long) ("KUCOIN:SPOT:BTC-USDT:TRADES".hashCode() & 0x7FFFFFFF);
        // Первый вызов
        postFixture("fixtures/requests/subscribe-spot-btc.json")
                .expectStatus().isAccepted()
                .expectBody().jsonPath("$.id").isEqualTo(expectedId);

        simulateKafkaSubscription("fixtures/requests/subscribe-spot-btc.json");

        // Повторный вызов
        postFixture("fixtures/requests/subscribe-spot-btc.json")
                .expectStatus().isAccepted()
                .expectBody().jsonPath("$.id").isEqualTo(expectedId);

        simulateKafkaSubscription("fixtures/requests/subscribe-spot-btc.json");

        assertEquals(1, sentWsFrames.size(), "WebSocket frame must not be duplicated");
    }

    /**
     * Что проверяем: Отписку от существующего потока по ID.
     * Что ждем:
     * - HTTP 204 No Content;
     * - Запись tombstone в Kafka;
     * - Отправку кадра unsubscribe в сокет при получении tombstone консьюмером;
     * - Удаление из списка активных подписок.
     */
    @Test
    @DisplayName("DELETE /api/subscriptions/{id}: Успешная отписка и очистка состояния")
    void shouldUnsubscribeSuccessfully() throws Exception {
        // Создаем подписку
        postFixture("fixtures/requests/subscribe-spot-btc.json")
                .expectStatus().isAccepted();

        simulateKafkaSubscription("fixtures/requests/subscribe-spot-btc.json");

        // Отписываемся
        deleteSubscription("KUCOIN:SPOT:BTC-USDT:TRADES")
                .expectStatus().isNoContent();

        verify(mockKafkaPublisher, times(1)).publishTombstone(eq("KUCOIN:SPOT:BTC-USDT:TRADES"));

        simulateKafkaTombstone("KUCOIN:SPOT:BTC-USDT:TRADES");
        assertWsFrameSent("unsubscribe");

        getActiveSubscriptions()
                .expectStatus().isOk()
                .expectBody().jsonPath("$.length()").isEqualTo(0);
    }

    /**
     * Что проверяем: Попытку отписки с некорректным форматом ключа.
     * Что ждем: HTTP 400 Bad Request.
     */
    @Test
    @DisplayName("DELETE /api/subscriptions/{id}: 400 Bad Request для некорректного формата ключа")
    void shouldReturnBadRequestOnMalformedKey() {
        deleteSubscription("999")
                .expectStatus().isBadRequest()
                .expectBody()
                .jsonPath("$.status").isEqualTo(400);
    }

    /**
     * Что проверяем: Запрос подписки на неподдерживаемую биржу (BINANCE).
     * Что ждем: HTTP 404 Not Found ("Exchange not supported: BINANCE").
     */
    @Test
    @DisplayName("POST /api/subscriptions: 404 Not Found для неподдерживаемой биржи")
    void shouldReturnNotFoundForUnsupportedExchange() {
        postFixture("fixtures/requests/subscribe-unknown-exchange.json")
                .expectStatus().isNotFound()
                .expectBody()
                .jsonPath("$.status").isEqualTo(404)
                .jsonPath("$.message").isEqualTo("Exchange not supported: BINANCE");
    }

    /**
     * Что проверяем: Валидацию некорректных входных DTO.
     * Что ждем: HTTP 400 Bad Request для каждого случая нарушения контракта.
     */
    @Test
    @DisplayName("POST /api/subscriptions: 400 Bad Request при невалидном теле запроса")
    void shouldReturnBadRequestOnValidationErrors() {
        // 1. Пустое имя биржи
        postFixture("fixtures/requests/invalid-blank-exchange.json")
                .expectStatus().isBadRequest();

        // 2. Некорректный формат символа
        postFixture("fixtures/requests/invalid-symbol.json")
                .expectStatus().isBadRequest();

        // 3. Отсутствие обязательных полей
        postFixture("fixtures/requests/invalid-missing-fields.json")
                .expectStatus().isBadRequest();

        // 4. Синтаксически битый JSON
        postFixture("fixtures/requests/invalid-malformed.json")
                .expectStatus().isBadRequest();
    }

    /**
     * Что проверяем: Сбой подключения в WebSocketClient при обработке в консьюмере.
     * Что ждем:
     * - Консьюмер ловит ошибку, не падает;
     * - Tombstone в Kafka НЕ отправляется (намерение сохраняется для восстановления).
     */
    @Test
    @DisplayName("SubscriptionConsumer: Обработка сбоя подключения сокета")
    void shouldHandleWebSocketFailureInConsumer() throws Exception {
        reset(mockWsClient);
        when(mockWsClient.execute(any(URI.class), any(WebSocketHandler.class)))
                .thenReturn(Mono.error(new ExchangeConnectionException("WebSocket connection refused")));

        postFixture("fixtures/requests/subscribe-spot-btc.json")
                .expectStatus().isAccepted();

        simulateKafkaSubscription("fixtures/requests/subscribe-spot-btc.json");

        verify(mockKafkaPublisher, never()).publishTombstone(any());
    }

    private void simulateKafkaSubscription(String fixturePath) throws Exception {
        byte[] bytes = new ClassPathResource(fixturePath).getInputStream().readAllBytes();
        JsonMapper mapper = ru.connector.config.KafkaConfig.createJsonMapper();
        CreateSubscriptionRequest dto = mapper.readValue(bytes, CreateSubscriptionRequest.class);
        String key = StreamKey.from(dto).toString();
        subscriptionConsumer.onMessage(new ConsumerRecord<>("market.subscriptions", 0, 0L, key, dto));
    }

    private void simulateKafkaTombstone(String key) {
        subscriptionConsumer.onMessage(new ConsumerRecord<>("market.subscriptions", 0, 1L, key, null));
    }

    private WebTestClient.ResponseSpec postFixture(String fixturePath) {
        return client.post()
                .uri("/api/subscriptions")
                .contentType(MediaType.APPLICATION_JSON)
                .body(BodyInserters.fromResource(new ClassPathResource(fixturePath)))
                .exchange();
    }

    private WebTestClient.ResponseSpec deleteSubscription(String id) {
        return client.delete()
                .uri("/api/subscriptions/" + id)
                .exchange();
    }

    private WebTestClient.ResponseSpec getActiveSubscriptions() {
        return client.get()
                .uri("/api/subscriptions")
                .exchange();
    }

    private void assertWsFrameSent(String expectedAction) {
        boolean found = sentWsFrames.stream()
                .anyMatch(f -> f.contains("\"action\":\"" + expectedAction + "\"")
                               && f.contains("\"symbol\":\"" + "BTC-USDT" + "\""));
        assertTrue(found, () -> "Expected frame with action=" + expectedAction + " and symbol=" + "BTC-USDT" + ", but was: " + sentWsFrames);
    }
}