package ru.connector.exchange.actor;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.buffer.DefaultDataBufferFactory;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.util.StreamUtils;
import org.springframework.web.reactive.socket.WebSocketHandler;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.WebSocketSession;
import org.springframework.web.reactive.socket.client.WebSocketClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.test.StepVerifier;
import ru.connector.exchange.ExchangeAdapter;
import ru.connector.exchange.kukoin.KucoinAdapter;
import ru.datacollector.common.model.StreamKey;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ConnectionActorTest {

    private static final StreamKey BTC_KEY = streamKey("BTC-USDT");
    private static final StreamKey ETH_KEY = streamKey("ETH-USDT");
    private static final StreamKey SOL_KEY = streamKey("SOL-USDT");

    private ConnectionActor actor;
    private ExchangeAdapter adapter;

    private List<byte[]> kafka;
    private List<Throwable> fatalErrors;
    private Sinks.Many<WebSocketMessage> inboundSink;
    private List<String> exchange;
    private final DefaultDataBufferFactory bufferFactory = new DefaultDataBufferFactory();
    private final AtomicInteger sessionCounter = new AtomicInteger(0);
    private final List<Sinks.Many<WebSocketMessage>> sessionInboundSinks = new CopyOnWriteArrayList<>();

    ConnectionActorTest() {}

    @BeforeEach
    void setUp() {
        WebSocketClient wsClient = mock(WebSocketClient.class);
        adapter = spy(new KucoinAdapter());

        kafka = new CopyOnWriteArrayList<>();
        fatalErrors = new CopyOnWriteArrayList<>();
        exchange = new CopyOnWriteArrayList<>();
        sessionCounter.set(0);
        sessionInboundSinks.clear();

        inboundSink = Sinks.many().multicast().onBackpressureBuffer();

        // Spring WebSocketClient.execute возвращает холодный Mono (Mono.defer),
        // создающий новую сессию при каждом повторном подключении (retryWhen).
        lenient().when(wsClient.execute(any(), any())).thenAnswer(inv -> {
            WebSocketHandler handler = inv.getArgument(1);
            return Mono.defer(() -> {
                int idx = sessionCounter.incrementAndGet();
                Sinks.Many<WebSocketMessage> sink = (idx == 1)
                        ? inboundSink
                        : Sinks.many().multicast().onBackpressureBuffer();
                sessionInboundSinks.add(sink);

                WebSocketSession freshSession = createMockSession(exchange);
                lenient().when(freshSession.receive()).thenReturn(sink.asFlux());

                return handler.handle(freshSession);
            });
        });

        lenient().doReturn(Flux.empty()).when(adapter).createPingStream(any());

        actor = ConnectionActor.builder()
                .id(1)
                .exchangeUrl(URI.create("wss://test.com"))
                .adapter(adapter)
                .wsClient(wsClient)
                .dataConsumer(kafka::add)
                .fatalErrorConsumer(fatalErrors::add)
                .reconnectDelay(Duration.ofMillis(50))
                .build();
    }

    @AfterEach
    void tearDown() {
        if (actor != null) {
            actor.close();
        }
    }

    // ==========================================
    // Вспомогательные методы и утилиты
    // ==========================================

    private static StreamKey streamKey(String symbol) {
        return StreamKey.parse("KUCOIN:SPOT:" + symbol + ":TRADES");
    }

    private WebSocketSession createMockSession(List<String> exchangeSink) {
        WebSocketSession mockSession = mock(WebSocketSession.class);
        lenient().when(mockSession.receive()).thenReturn(Flux.never());
        lenient().when(mockSession.send(any())).thenAnswer(inv -> {
            Flux<WebSocketMessage> stream = inv.getArgument(0);
            return stream.doOnNext(msg -> exchangeSink.add(msg.getPayloadAsText())).then();
        });
        lenient().when(mockSession.textMessage(anyString())).thenAnswer(inv -> {
            WebSocketMessage msg = mock(WebSocketMessage.class);
            when(msg.getPayloadAsText()).thenReturn(inv.getArgument(0));
            return msg;
        });
        return mockSession;
    }

    private String loadJson(String path) {
        ClassPathResource resource = new ClassPathResource(path);
        try (InputStream inputStream = resource.getInputStream()) {
            return StreamUtils.copyToString(inputStream, StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private String loadJson(String path, Map<String, String> placeholders) {
        String template = loadJson(path);
        for (Map.Entry<String, String> entry : placeholders.entrySet()) {
            template = template.replace("${" + entry.getKey() + "}", entry.getValue());
        }
        return template;
    }

    private WebSocketMessage toTextMessage(String json) {
        byte[] bytes = json.getBytes(StandardCharsets.UTF_8);
        return new WebSocketMessage(WebSocketMessage.Type.TEXT, bufferFactory.wrap(bytes));
    }

    @SuppressWarnings("unchecked")
    private int getActiveCount() {
        Map<StreamKey, String> map = (Map<StreamKey, String>) ReflectionTestUtils.getField(actor, "activeSubscriptions");
        return map != null ? map.size() : 0;
    }

    @SuppressWarnings("unchecked")
    private boolean hasSubscription(StreamKey key) {
        Map<StreamKey, String> map = (Map<StreamKey, String>) ReflectionTestUtils.getField(actor, "activeSubscriptions");
        return map != null && map.containsKey(key);
    }

    @FunctionalInterface
    private interface ConcurrentAction {
        void execute(int index) throws Exception;
    }

    /**
     * Выполняет действие конкурентно в заданном количестве потоков с одновременным стартом по защелке.
     */
    private void runConcurrently(int threadCount, ConcurrentAction action) throws InterruptedException {
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        try {
            CountDownLatch readyLatch = new CountDownLatch(threadCount);
            CountDownLatch startLatch = new CountDownLatch(1);
            List<CompletableFuture<Void>> futures = new ArrayList<>(threadCount);
            List<Throwable> errors = new CopyOnWriteArrayList<>();

            for (int i = 0; i < threadCount; i++) {
                final int index = i;
                futures.add(CompletableFuture.runAsync(() -> {
                    readyLatch.countDown();
                    try {
                        startLatch.await();
                        action.execute(index);
                    } catch (Throwable e) {
                        if (e instanceof InterruptedException) {
                            Thread.currentThread().interrupt();
                        }
                        errors.add(e);
                    }
                }, executor));
            }

            assertTrue(readyLatch.await(5, TimeUnit.SECONDS), "Превышен таймаут готовности потоков");
            startLatch.countDown();
            try {
                CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new)).get(15, TimeUnit.SECONDS);
            } catch (TimeoutException e) {
                errors.add(new AssertionError("Превышен таймаут выполнения параллельных потоков (15с)", e));
            } catch (Exception e) {
                errors.add(e);
            }

            if (!errors.isEmpty()) {
                throw new AssertionError("Ошибки в параллельных потоках (" + errors.size() + " из " + threadCount + "): "
                        + errors.getFirst().getMessage(), errors.getFirst());
            }
        } finally {
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS), "Пул потоков не завершился штатно");
        }
    }

    // ==========================================
    // БЛОК 1: Базовые функциональные тесты
    // ==========================================

    @Test
    @DisplayName("subscribe & payload: отправляет подписку на биржу и маршрутизирует входящие рыночные данные в Kafka")
    void shouldSubscribeAndRoutePayloadToKafka() {
        actor.start();

        String subFrame = loadJson("fixtures/kucoin/subscribe-spot.json");
        StepVerifier.create(actor.subscribe(BTC_KEY, subFrame))
                .verifyComplete();

        assertEquals(1, exchange.size(), "Фрейм подписки должен быть отправлен в сокет биржи");
        assertEquals(subFrame, exchange.getFirst(), "Содержимое отправленного фрейма подписки должно совпадать");
        assertEquals(1, getActiveCount(), "Подписка должна быть зарегистрирована в activeSubscriptions");
        assertTrue(hasSubscription(BTC_KEY), "Ключ BTC-USDT должен присутствовать в activeSubscriptions");

        String payloadJson = loadJson("fixtures/kucoin/trade-spot.json");
        inboundSink.tryEmitNext(toTextMessage(payloadJson));

        assertEquals(1, kafka.size(), "Полезный фрейм сделки должен быть передан в Kafka");
        assertArrayEquals(payloadJson.getBytes(StandardCharsets.UTF_8), kafka.getFirst(),
                "Содержимое переданного в Kafka фрейма должно совпадать с исходным JSON");
        verify(adapter, times(1)).isPayload(any());
        verify(adapter, never()).handleServiceMsg(any());
        assertTrue(fatalErrors.isEmpty(), "Не должно возникать фатальных ошибок");
    }

    @Test
    @DisplayName("inbound: направляет сервисный фрейм биржи в адаптер и не отправляет его в Kafka")
    void shouldRouteServiceMsgToAdapter() {
        actor.start();

        String serviceJson = loadJson("fixtures/kucoin/service-welcome.json");
        inboundSink.tryEmitNext(toTextMessage(serviceJson));

        assertTrue(kafka.isEmpty(), "Сервисный фрейм не должен попадать в Kafka");
        verify(adapter, times(1)).isPayload(any());
        verify(adapter, times(1)).handleServiceMsg(any());
        assertTrue(fatalErrors.isEmpty(), "Не должно возникать фатальных ошибок");
    }

    @Test
    @DisplayName("inbound: отсекает транспортный PONG фрейм (опкод 0xA) без вызова адаптера и передачи в Kafka")
    void shouldDropTransportPongOpcode() {
        actor.start();

        WebSocketMessage pongMsg = new WebSocketMessage(WebSocketMessage.Type.PONG, bufferFactory.allocateBuffer(0));
        inboundSink.tryEmitNext(pongMsg);

        assertTrue(kafka.isEmpty(), "Транспортный PONG не должен попадать в Kafka");
        verify(adapter, never()).isPayload(any());
        verify(adapter, never()).handleServiceMsg(any());
        assertTrue(fatalErrors.isEmpty(), "Не должно возникать фатальных ошибок");
    }

    @Test
    @DisplayName("unsubscribe: штатно отписывается и не шлет повторный фрейм для несуществующего ключа")
    void shouldHandleUnsubscribeAndIgnoreNonExistentKey() {
        actor.start();

        String subFrame = loadJson("fixtures/kucoin/subscribe-spot.json");
        String unsubFrame = loadJson("fixtures/kucoin/unsubscribe-spot.json");

        StepVerifier.create(actor.subscribe(BTC_KEY, subFrame))
                .verifyComplete();
        assertEquals(1, exchange.size(), "Подписка должна быть отправлена");
        assertEquals(1, getActiveCount(), "Подписка должна быть в activeSubscriptions");
        assertTrue(hasSubscription(BTC_KEY), "Ключ BTC должен присутствовать");

        StepVerifier.create(actor.unsubscribe(BTC_KEY, unsubFrame))
                .verifyComplete();
        assertEquals(2, exchange.size(), "Фрейм отписки должен быть отправлен в биржу");
        assertEquals(unsubFrame, exchange.get(1), "Содержимое фрейма отписки должно совпадать");
        assertEquals(0, getActiveCount(), "После отписки активных подписок в памяти остаться не должно");
        assertFalse(hasSubscription(BTC_KEY), "Ключ BTC должен быть удален из activeSubscriptions");

        StreamKey nonExistentKey = streamKey("NON-EXISTENT");
        StepVerifier.create(actor.unsubscribe(nonExistentKey, unsubFrame))
                .verifyComplete();
        assertEquals(2, exchange.size(), "Для несуществующего ключа фрейм отписки не должен отправляться в биржу");
        assertEquals(0, getActiveCount(), "Количество активных подписок по-прежнему 0");
        assertTrue(fatalErrors.isEmpty(), "Не должно возникать фатальных ошибок");
    }

    @Test
    @DisplayName("lifecycle: отклоняет команды после закрытия соединения, close() идемпотентен")
    void shouldRejectCommandsAndBeIdempotentWhenClosed() {
        actor.start();
        actor.close();

        assertDoesNotThrow(() -> actor.close(), "Повторный вызов close() обязан быть идемпотентным");

        String subFrame = loadJson("fixtures/kucoin/subscribe-spot.json");
        String unsubFrame = loadJson("fixtures/kucoin/unsubscribe-spot.json");

        StepVerifier.create(actor.subscribe(BTC_KEY, subFrame))
                .expectErrorMatches(err -> err instanceof IllegalStateException && err.getMessage().contains("закрыто"))
                .verify();

        StepVerifier.create(actor.unsubscribe(BTC_KEY, unsubFrame))
                .expectErrorMatches(err -> err instanceof IllegalStateException && err.getMessage().contains("закрыто"))
                .verify();

        assertTrue(exchange.isEmpty(), "В закрытый сокет не должны отправляться команды");
    }

    @Test
    @DisplayName("reconnect: автоматически отправляет все активные подписки при переподключении сокета")
    void shouldResubscribeActiveSubscriptionsOnReconnect() {
        actor.start();

        String subBtc = loadJson("fixtures/kucoin/subscribe-spot.json");
        String subEth = loadJson("fixtures/kucoin/subscribe-spot-eth.json");

        StepVerifier.create(actor.subscribe(BTC_KEY, subBtc)).verifyComplete();
        StepVerifier.create(actor.subscribe(ETH_KEY, subEth)).verifyComplete();
        assertEquals(2, exchange.size(), "В первой сессии должно быть отправлено 2 подписки");

        // Эмулируем обрыв сети первой сессии
        inboundSink.tryEmitError(new IOException("Simulated network drop"));

        // Актор автоматически выполняет retryWhen и переподключается ко 2-й сессии
        org.awaitility.Awaitility.await()
                .atMost(Duration.ofSeconds(3))
                .untilAsserted(() -> {
                    assertEquals(4, exchange.size(), "При реконнекте должны повторно отправиться 2 активные подписки");
                    assertEquals(2, getActiveCount(), "Количество активных подписок в памяти должно оставаться 2");
                    List<String> session2Frames = exchange.subList(2, 4);
                    assertTrue(session2Frames.contains(subBtc), "Сессия 2 должна получить подписку BTC");
                    assertTrue(session2Frames.contains(subEth), "Сессия 2 должна получить подписку ETH");
                });

        assertTrue(fatalErrors.isEmpty(), "Не должно возникать фатальных ошибок");
    }

    @Test
    @DisplayName("reconnect: отклоняет подписки во время обрыва связи и восстанавливает только ранее активные")
    void shouldResubscribeAndSendNewSubscriptionsDuringReconnect() {
        actor.start();

        String subBtc = loadJson("fixtures/kucoin/subscribe-spot.json");
        String subEth = loadJson("fixtures/kucoin/subscribe-spot-eth.json");

        StepVerifier.create(actor.subscribe(BTC_KEY, subBtc)).verifyComplete();
        StepVerifier.create(actor.subscribe(ETH_KEY, subEth)).verifyComplete();
        assertEquals(2, exchange.size(), "В первой сессии должно быть отправлено 2 подписки");

        // Эмулируем обрыв первой сессии
        inboundSink.tryEmitError(new IOException("Simulated network drop"));

        // Подписка во время обрыва обязана отклоняться ошибкой IllegalStateException
        String subSol = loadJson("fixtures/kucoin/subscribe-spot-sol.json");
        StepVerifier.create(actor.subscribe(SOL_KEY, subSol))
                .expectError(IllegalStateException.class)
                .verify(Duration.ofSeconds(2));

        assertEquals(2, getActiveCount(), "Подписка SOL не должна была добавиться в activeSubscriptions");
        assertFalse(hasSubscription(SOL_KEY), "Ключ SOL обязан отсутствовать в activeSubscriptions");

        // Проверяем, что актор сам переподключился и во 2-й сессии оказались ровно 2 старые подписки (BTC, ETH)
        org.awaitility.Awaitility.await()
                .atMost(Duration.ofSeconds(3))
                .untilAsserted(() -> {
                    assertEquals(4, exchange.size(), "Всего в сокет должно уйти 2 фрейма (сессия 1) + 2 фрейма (сессия 2)");
                    assertEquals(2, getActiveCount(), "В activeSubscriptions должно оставаться ровно 2 подписки");
                    List<String> session2Frames = exchange.subList(2, 4);
                    assertEquals(2, session2Frames.size(), "Во 2-ю сессию должно поступить ровно 2 фрейма (только старые подписки)");
                    assertTrue(session2Frames.contains(subBtc), "Сессия 2 должна содержать BTC");
                    assertTrue(session2Frames.contains(subEth), "Сессия 2 должна содержать ETH");
                    assertFalse(exchange.stream().anyMatch(f -> f.contains("SOL-USDT")), "Подписка SOL не должна была попасть ни в одну сессию");
                });

        assertTrue(fatalErrors.isEmpty(), "Не должно возникать фатальных ошибок");
    }

    // ==========================================
    // БЛОК 2: Многопоточные стресс-тесты
    // ==========================================

    @Test
    @DisplayName("multithread: одновременный шторм 50 подписок без потерь и сбоев очереди")
    void shouldHandleConcurrentSubscriptionsBurst() throws InterruptedException {
        actor.start();

        int threadsCount = 50;
        runConcurrently(threadsCount, i -> {
            String symbol = "TOKEN" + i + "-USDT";
            StreamKey key = streamKey(symbol);
            String frame = loadJson("fixtures/kucoin/subscribe-spot-template.json",
                    Map.of("id", String.valueOf(i), "symbol", symbol));
            actor.subscribe(key, frame).block(Duration.ofSeconds(10));
        });

        assertEquals(threadsCount, exchange.size(), "В сокет должно быть отправлено ровно 50 уникальных фреймов");
        assertEquals(threadsCount, getActiveCount(), "В activeSubscriptions должно быть ровно 50 записей");
        for (int i = 0; i < threadsCount; i++) {
            String symbol = "TOKEN" + i + "-USDT";
            assertTrue(hasSubscription(streamKey(symbol)), "Подписка для " + symbol + " обязана быть в activeSubscriptions");
            boolean frameFound = exchange.stream().anyMatch(f -> f.contains(symbol));
            assertTrue(frameFound, "В сокет биржи должен быть отправлен фрейм для " + symbol);
        }
        assertTrue(fatalErrors.isEmpty(), "Не должно возникать фатальных ошибок");
    }

    @Test
    @DisplayName("multithread: одновременная массовая отписка 50 потоков полностью очищает хранилище")
    void shouldHandleConcurrentUnsubscriptionsBurst() throws InterruptedException {
        actor.start();

        int threadsCount = 50;
        for (int i = 0; i < threadsCount; i++) {
            String symbol = "TOKEN" + i + "-USDT";
            StreamKey key = streamKey(symbol);
            String subFrame = loadJson("fixtures/kucoin/subscribe-spot-template.json",
                    Map.of("id", String.valueOf(i), "symbol", symbol));
            actor.subscribe(key, subFrame).block(Duration.ofSeconds(5));
        }
        assertEquals(threadsCount, exchange.size(), "50 подписок должны быть отправлены");
        assertEquals(threadsCount, getActiveCount(), "50 подписок должны быть сохранены");

        runConcurrently(threadsCount, i -> {
            String symbol = "TOKEN" + i + "-USDT";
            StreamKey key = streamKey(symbol);
            String unsubFrame = loadJson("fixtures/kucoin/unsubscribe-spot-template.json",
                    Map.of("id", String.valueOf(i), "symbol", symbol));
            actor.unsubscribe(key, unsubFrame).block(Duration.ofSeconds(10));
        });

        assertEquals(0, getActiveCount(), "После массовой отписки хранилище activeSubscriptions должно быть пустым");
        assertEquals(threadsCount * 2, exchange.size(), "В сокет должно быть отправлено ровно 50 подписок + 50 отписок");
        for (int i = 0; i < threadsCount; i++) {
            String symbol = "TOKEN" + i + "-USDT";
            boolean unsubFound = exchange.stream().anyMatch(f -> f.contains(symbol) && f.contains("\"unsubscribe\""));
            assertTrue(unsubFound, "В сокет биржи должен быть отправлен фрейм отписки для " + symbol);
        }
        assertTrue(fatalErrors.isEmpty(), "Не должно возникать фатальных ошибок");
    }

    @Test
    @DisplayName("multithread: 20 потоков одновременно подписываются на один ключ - ровно 1 запись в activeSubscriptions")
    void shouldHandleConcurrentDuplicateSubscriptions() throws InterruptedException {
        actor.start();

        String frame = loadJson("fixtures/kucoin/subscribe-spot.json");
        runConcurrently(20, _ -> actor.subscribe(BTC_KEY, frame).block(Duration.ofSeconds(10)));

        assertEquals(1, getActiveCount(), "В activeSubscriptions должна остаться ровно 1 запись для ключа");
        assertTrue(hasSubscription(BTC_KEY), "Запись должна соответствовать ключу BTC-USDT");
        assertEquals(1, exchange.size(), "В сокет биржи должен быть отправлен ровно 1 фрейм подписки (дубликаты должны быть отсечены)");
        assertTrue(fatalErrors.isEmpty(), "Не должно возникать фатальных ошибок");
    }

    @Test
    @DisplayName("multithread: 20 потоков одновременно отписываются от одного ключа - в сокет уходит ровно 1 фрейм")
    void shouldSendOnlyOneUnsubscribeFrameOnConcurrentDuplicateUnsubscribe() throws InterruptedException {
        actor.start();

        String subFrame = loadJson("fixtures/kucoin/subscribe-spot.json");
        String unsubFrame = loadJson("fixtures/kucoin/unsubscribe-spot.json");

        actor.subscribe(BTC_KEY, subFrame).block(Duration.ofSeconds(5));
        assertEquals(1, exchange.size(), "Подписка должна быть отправлена");
        assertEquals(1, getActiveCount(), "Подписка должна быть сохранена");

        runConcurrently(20, _ -> actor.unsubscribe(BTC_KEY, unsubFrame).block(Duration.ofSeconds(10)));

        assertEquals(0, getActiveCount(), "activeSubscriptions должно быть пустым после отписки");
        assertEquals(2, exchange.size(), "В сокет должно уйти ровно 2 фрейма: 1 подписка и ровно 1 отписка (дубликаты отсечены)");
        assertEquals(unsubFrame, exchange.get(1), "Второй фрейм должен быть фреймом отписки");
        assertTrue(fatalErrors.isEmpty(), "Не должно возникать фатальных ошибок");
    }

    @Test
    @DisplayName("multithread: подписки во время actor.close() завершаются без зависаний (0 дедлоков)")
    void shouldHandleConcurrentSubscriptionsDuringClose() throws InterruptedException {
        actor.start();

        int threadsCount = 50;
        AtomicInteger completedCount = new AtomicInteger();
        CountDownLatch inFlightLatch = new CountDownLatch(5);
        long startTime = System.currentTimeMillis();

        List<StreamKey> keys = new ArrayList<>(threadsCount);
        List<String> frames = new ArrayList<>(threadsCount);
        for (int i = 0; i < threadsCount; i++) {
            String symbol = "TOKEN" + i + "-USDT";
            keys.add(streamKey(symbol));
            frames.add(loadJson("fixtures/kucoin/subscribe-spot-template.json",
                    Map.of("id", String.valueOf(i), "symbol", symbol)));
        }

        runConcurrently(threadsCount + 1, i -> {
            if (i == threadsCount) {
                assertTrue(inFlightLatch.await(5, TimeUnit.SECONDS), "Превышен таймаут ожидания старта подписок перед закрытием");
                actor.close();
            } else {
                inFlightLatch.countDown();
                try {
                    actor.subscribe(keys.get(i), frames.get(i)).block(Duration.ofMillis(500));
                } catch (Exception e) {
                    // Ожидаемый отказ: только исключение о закрытом соединении, никаких TimeoutException
                    boolean isClosedError = e instanceof IllegalStateException
                            && (e.getMessage().contains("закрыто") || e.getMessage().contains("FAIL_TERMINATED"));
                    assertTrue(isClosedError, "Ожидалась ошибка закрытого соединения, но получено: " + e);
                } finally {
                    completedCount.incrementAndGet();
                }
            }
        });

        long duration = System.currentTimeMillis() - startTime;
        assertTrue(actor.isClosed(), "Актор должен быть закрыт");
        assertEquals(threadsCount, completedCount.get(),
                "Каждая из 50 операций обязана завершиться без зависаний");
        assertTrue(duration < 2500, "Операции закрытия и отбоя подписок должны выполняться без таймаутов (< 2.5с), заняло: " + duration + "ms");
    }

    @Test
    @DisplayName("multithread: отклоняет параллельные подписки во время обрыва связи и восстанавливает только ранее активные")
    void shouldNotLoseSubscriptionsDuringConcurrentReconnect() throws InterruptedException {
        actor.start();

        for (int i = 0; i < 10; i++) {
            String symbol = "INIT" + i + "-USDT";
            StreamKey key = streamKey(symbol);
            String subFrame = loadJson("fixtures/kucoin/subscribe-spot-template.json",
                    Map.of("id", String.valueOf(i), "symbol", symbol));
            actor.subscribe(key, subFrame).block(Duration.ofSeconds(5));
        }
        assertEquals(10, exchange.size(), "В первой сессии отправлено 10 подписок");
        assertEquals(10, getActiveCount(), "В activeSubscriptions сохранено 10 подписок");

        // Эмулируем обрыв первой сессии
        inboundSink.tryEmitError(new IOException("Simulated network drop"));

        int newSubsCount = 30;
        AtomicInteger rejectedCount = new AtomicInteger();

        // 30 параллельных потоков вызывают подписку во время обрыва: все вызовы обязаны быть отклонены ошибкой соединения
        runConcurrently(newSubsCount, i -> {
            int id = i + 100;
            String symbol = "NEW" + id + "-USDT";
            StreamKey key = streamKey(symbol);
            String frame = loadJson("fixtures/kucoin/subscribe-spot-template.json",
                    Map.of("id", String.valueOf(id), "symbol", symbol));
            assertThrows(Exception.class, () -> {
                actor.subscribe(key, frame).block(Duration.ofSeconds(5));

            }, "Подписка во время обрыва связи обязана отклоняться ошибкой");
        });

        assertEquals(newSubsCount, rejectedCount.get(), "Все 30 попыток подписки во время обрыва обязаны быть отклонены");

        // Проверяем, что актор автоматически восстановил соединение и отправил во 2-ю сессию ТОЛЬКО 10 старых подписок
        org.awaitility.Awaitility.await()
                .atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> {
                    assertEquals(10, getActiveCount(), "В activeSubscriptions должно остаться ровно 10 старых записей");
                    assertEquals(20, exchange.size(), "Всего в exchange должно быть ровно 20 фреймов (10 из сессии 1 + 10 из сессии 2)");
                    List<String> session2Frames = exchange.subList(10, exchange.size());
                    assertEquals(10, session2Frames.size(), "Во 2-ю сессию должны поступить ровно 10 ранее активных подписок");
                    for (int i = 0; i < 10; i++) {
                        String symbol = "INIT" + i + "-USDT";
                        boolean found = session2Frames.stream().anyMatch(f -> f.contains(symbol));
                        assertTrue(found, "Во 2-ю сессию должна быть восстановлена подписка " + symbol);
                    }
                    for (int i = 0; i < newSubsCount; i++) {
                        int id = i + 100;
                        String symbol = "NEW" + id + "-USDT";
                        boolean found = exchange.stream().anyMatch(f -> f.contains(symbol));
                        assertFalse(found, "Отклоненная подписка " + symbol + " не должна была попасть ни в одну из сессий");
                    }
                });

        assertTrue(fatalErrors.isEmpty(), "Не должно возникать фатальных ошибок");
    }
}