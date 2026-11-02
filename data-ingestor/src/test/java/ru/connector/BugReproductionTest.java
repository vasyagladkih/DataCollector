package ru.connector;

import tools.jackson.databind.json.JsonMapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.buffer.DefaultDataBufferFactory;
import org.springframework.web.reactive.socket.WebSocketHandler;
import org.springframework.web.reactive.socket.client.WebSocketClient;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import ru.connector.exceptions.ExchangeConnectionException;
import ru.connector.exchange.kukoin.KucoinAdapter;
import ru.connector.exchange.kukoin.KucoinManager;
import ru.connector.models.Command;
import ru.connector.models.SubscriptionDto;
import ru.datacollector.common.model.Action;
import ru.datacollector.common.model.MarketType;
import ru.datacollector.common.model.Symbol;

import java.net.URI;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Набор тестов для воспроизведения и эмпирической валидации выявленных дефектов.
 */
class BugReproductionTest {

    /**
     * Дефект 1: Symbol.parse() пропускает некорректные строки с пустым базовым активом.
     * <p>
     * Входные данные: "-USDT".
     * Ожидание: выбрасывание IllegalArgumentException, так как базовый актив пуст.
     * Реальность: создается объект Symbol("", "USDT"), нарушая инвариант @NotBlank.
     */
    @Test
    @DisplayName("Баг: Symbol.parse(\"-USDT\") создает объект с пустой базовой валютой вместо исключения")
    void shouldRejectSymbolWithEmptyBaseAsset() {
        assertThrows(IllegalArgumentException.class, () -> Symbol.parse("-USDT"),
                "Парсер символа обязан отклонять строку с пустым базовым активом '-USDT'");
    }

    /**
     * Дефект 2: Symbol.parse() пропускает некорректные строки с пустым котируемым активом.
     * <p>
     * Входные данные: "BTC--USDT".
     * Ожидание: выбрасывание IllegalArgumentException, так как котируемый актив пуст.
     * Реальность: создается объект Symbol("BTC", ""), нарушая инвариант @NotBlank.
     */
    @Test
    @DisplayName("Баг: Symbol.parse(\"BTC--USDT\") создает объект с пустой котируемой валютой")
    void shouldRejectSymbolWithEmptyQuoteAsset() {
        assertThrows(IllegalArgumentException.class, () -> Symbol.parse("BTC--USDT"),
                "Парсер символа обязан отклонять строку с пустым котируемым активом 'BTC--USDT'");
    }

    /**
     * Дефект 3: KucoinAdapter.isServiceMsg ложно классифицирует рыночные данные как сервисные кадры.
     * <p>
     * Входные данные: валидный JSON KuCoin с полем "id" (стандартный заголовок KuCoin WebSocket push):
     * {"id":"1545896669145","type":"message","topic":"/market/ticker:BTC-USDT","data":{"price":"64000.0"}}
     * Ожидание: isServiceMsg == false (кадр должен быть отправлен в Kafka).
     * Реальность: text.contains("\"id\":") возвращает true, сообщение отбрасывается как сервисное.
     */
    @Test
    @DisplayName("Баг: KucoinAdapter классифицирует рыночные сообщения с полем \"id\": как сервисные кадры")
    void shouldNotClassifyMarketDataWithIdAsServiceMessage() {
        KucoinAdapter adapter = new KucoinAdapter();
        String marketDataFrame = "{\"id\":\"1545896669145\",\"type\":\"message\",\"topic\":\"/market/ticker:BTC-USDT\",\"data\":{\"price\":\"64000.0\"}}";
        var buffer = new DefaultDataBufferFactory().wrap(marketDataFrame.getBytes(StandardCharsets.UTF_8));

        boolean isService = adapter.isPayload(buffer);

        assertFalse(isService, "Кадр рыночных данных с top-level полем 'id' не должен классифицироваться как служебный и отбрасываться");
    }

    /**
     * Дефект 4: Рассинхронизация ID при отписке в KucoinManager / GroupPoolActor / ConnectionActor.
     * <p>
     * Входные данные: CreateSubscriptionRequest с подпиской на SPOT BTC-USDT.
     * Механика:
     * - При подписке вызывается translate(sub, SUBSCRIBE), генерирующий id="X".
     * - ConnectionActor сохраняет в activeSubscriptions строку: {"id":"X", "action":"subscribe", ...}.
     * - При отписке GroupPoolActor повторно вызывает translate(sub, SUBSCRIBE) для сопоставления.
     * - KucoinManager.translate инкрементирует idSequence, генерируя id="X+1".
     * - ConnectionActor.handleUnsubscribe выполняет activeSubscriptions.remove(cmd.subscribeFrame()).
     * - Так как id="X" != id="X+1", remove() возвращает false.
     * - Фрейм отписки НИКОГДА не отправляется в WebSocket, а старая подписка не удаляется.
     */
    @Test
    @DisplayName("Баг: Повторный вызов translate(sub, SUBSCRIBE) при отписке меняет id фрейма, блокируя отписку")
    void shouldProduceIdenticalSubscribeFrameForUnsubscribeMatching() {
        KucoinManager manager = new KucoinManager(null, null, JsonMapper.builder().build());
        SubscriptionDto dto = new SubscriptionDto("KUCOIN", MarketType.SPOT, Symbol.parse("BTC-USDT"), new Command.Trades());

        String firstSubscribeFrame = manager.translate(dto, Action.SUBSCRIBE);
        String secondSubscribeFrame = manager.translate(dto, Action.SUBSCRIBE);

        assertEquals(firstSubscribeFrame, secondSubscribeFrame,
                "Фрейм подписки для сопоставления при отписке должен быть идентичен зарегистрированному, иначе activeSubscriptions.remove() вернет false");
    }

    /**
     * Дефект 5: Проглатывание ошибки сбоя соединения при подписке в KucoinManager / GroupPoolActor.
     * <p>
     * Входные данные: WebSocketClient, выбрасывающий сетевой сбой ExchangeConnectionException.
     * Ожидание: manager.subscribe() должен возвращать Mono с ошибкой ExchangeConnectionException,
     * чтобы ExchangeService выполнил откат (rollback) и контроллер вернул HTTP 502 Bad Gateway.
     * Реальность: ConnectionActor запускает подключение в фоне без связи с вызовом subscribe(),
     * поэтому manager.subscribe() возвращает успешный Mono.empty(), маскируя сбой сети (возвращается HTTP 202).
     */
    @Test
    @DisplayName("Баг: KucoinManager.subscribe() завершается успешно даже при сетевом сбое WebSocketClient")
    void shouldPropagateConnectionErrorOnSubscribe() {
        WebSocketClient mockClient = mock(WebSocketClient.class);
        when(mockClient.execute(any(URI.class), any(WebSocketHandler.class)))
                .thenReturn(Mono.error(new ExchangeConnectionException("WebSocket connection refused")));

        KucoinManager manager = new KucoinManager(mockClient, null, JsonMapper.builder().build());
        SubscriptionDto dto = new SubscriptionDto("KUCOIN", MarketType.SPOT, Symbol.parse("BTC-USDT"), new Command.Trades());

        StepVerifier.create(manager.subscribe(dto))
                .expectError(ExchangeConnectionException.class)
                .verify(java.time.Duration.ofSeconds(2));
    }
}
