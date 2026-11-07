package ru.datacollector.controller;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;
import ru.datacollector.dto.CreateSubscriptionRequest;
import ru.datacollector.dto.SubscriptionResponse;
import ru.datacollector.exception.IllegalSubscriptionStateException;
import ru.datacollector.exception.SubscriptionNotFoundException;
import ru.datacollector.exception.SymbolNotFoundException;
import ru.datacollector.model.Status;
import ru.datacollector.service.SubscriptionService;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(controllers = {SubscriptionController.class, GlobalExceptionHandler.class})
@AutoConfigureMockMvc(addFilters = false)
class SubscriptionControllerTest {

    private static final String BASE_URL = "/api/v1/subscriptions";
    private static final String RESTART_URL = "/api/v1/subscriptions/{id}/restart";
    private static final String BY_ID_URL = "/api/v1/subscriptions/{id}";

    private static final String JSON_VALID_REQUEST = "/json/requests/create-subscription-valid.json";
    private static final String JSON_INVALID_REQUEST = "/json/requests/create-subscription-invalid.json";
    private static final String JSON_EMPTY_REQUEST = "/json/requests/create-subscription-empty.json";

    private static final Long TEST_ID = 1L;
    private static final String EXCHANGE = "MOEX";
    private static final String CHANNEL = "trades";
    private static final String SYMBOL = "BBG004730N88";
    private static final Instant TEST_TIMESTAMP = Instant.parse("2026-01-01T10:00:00Z");

    @Autowired
    private MockMvc mockMvc;

    @MockitoBean
    private SubscriptionService subscriptionService;

    private String readJson(String path) {
        try (InputStream is = getClass().getResourceAsStream(path)) {
            if (is == null) {
                throw new IllegalArgumentException("Resource not found: " + path);
            }
            return new String(is.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private SubscriptionResponse createResponse(Status status) {
        return new SubscriptionResponse(
                TEST_ID,
                EXCHANGE,
                CHANNEL,
                SYMBOL,
                status.name(),
                null,
                TEST_TIMESTAMP,
                TEST_TIMESTAMP
        );
    }

    // =========================================================================
    // Группа 1: POST /api/v1/subscriptions
    // =========================================================================

    @Test
    @DisplayName("1. POST /api/v1/subscriptions: при валидном JSON из ресурсов возвращает 201 Created и DTO")
    void subscribe_whenValidRequest_returns201CreatedAndResponse() throws Exception {
        String jsonContent = readJson(JSON_VALID_REQUEST);
        SubscriptionResponse response = createResponse(Status.CONNECTING);
        var expectedRequest = new CreateSubscriptionRequest(EXCHANGE, SYMBOL, CHANNEL);

        when(subscriptionService.subscribe(expectedRequest)).thenReturn(response);

        mockMvc.perform(post(BASE_URL)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(jsonContent))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.id").value(TEST_ID))
                .andExpect(jsonPath("$.exchange").value(EXCHANGE))
                .andExpect(jsonPath("$.channel").value(CHANNEL))
                .andExpect(jsonPath("$.symbol").value(SYMBOL))
                .andExpect(jsonPath("$.status").value("CONNECTING"))
                .andExpect(jsonPath("$.errorMessage").doesNotExist())
                .andExpect(jsonPath("$.createdAt").value(TEST_TIMESTAMP.toString()))
                .andExpect(jsonPath("$.updatedAt").value(TEST_TIMESTAMP.toString()));

        verify(subscriptionService).subscribe(expectedRequest);
        verifyNoMoreInteractions(subscriptionService);
    }

    @Test
    @DisplayName("2. POST /api/v1/subscriptions: при null в обязательных полях возвращает 400 Bad Request и VALIDATION_FAILED")
    void subscribe_whenNullFields_returns400BadRequestValidationFailed() throws Exception {
        String jsonContent = readJson(JSON_INVALID_REQUEST);

        mockMvc.perform(post(BASE_URL)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(jsonContent))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.status").value(400))
                .andExpect(jsonPath("$.error").value("VALIDATION_FAILED"))
                .andExpect(jsonPath("$.message").isNotEmpty());

        verifyNoMoreInteractions(subscriptionService);
    }

    @Test
    @DisplayName("3. POST /api/v1/subscriptions: при пустом JSON {} возвращает 400 Bad Request")
    void subscribe_whenEmptyBody_returns400BadRequest() throws Exception {
        String jsonContent = readJson(JSON_EMPTY_REQUEST);

        mockMvc.perform(post(BASE_URL)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(jsonContent))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.status").value(400))
                .andExpect(jsonPath("$.error").value("VALIDATION_FAILED"))
                .andExpect(jsonPath("$.message").isNotEmpty());

        verifyNoMoreInteractions(subscriptionService);
    }

    @Test
    @DisplayName("4. POST /api/v1/subscriptions: при SymbolNotFoundException возвращает 400 и SYMBOL_NOT_FOUND")
    void subscribe_whenSymbolNotFound_returns400BadRequest() throws Exception {
        String jsonContent = readJson(JSON_VALID_REQUEST);
        var expectedRequest = new CreateSubscriptionRequest(EXCHANGE, SYMBOL, CHANNEL);
        when(subscriptionService.subscribe(expectedRequest))
                .thenThrow(new SymbolNotFoundException("Символ не найден"));

        mockMvc.perform(post(BASE_URL)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(jsonContent))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.status").value(400))
                .andExpect(jsonPath("$.error").value("SYMBOL_NOT_FOUND"))
                .andExpect(jsonPath("$.message").value("Символ не найден"));

        verify(subscriptionService).subscribe(expectedRequest);
        verifyNoMoreInteractions(subscriptionService);
    }

    // =========================================================================
    // Группа 2: DELETE /api/v1/subscriptions/{id}
    // =========================================================================

    @Test
    @DisplayName("5. DELETE /api/v1/subscriptions/{id}: при успешной отписке возвращает 200 OK")
    void unsubscribe_whenExists_returns200Ok() throws Exception {
        SubscriptionResponse response = createResponse(Status.TERMINATED);
        when(subscriptionService.unsubscribe(TEST_ID)).thenReturn(response);

        mockMvc.perform(delete(BY_ID_URL, TEST_ID))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.id").value(TEST_ID))
                .andExpect(jsonPath("$.exchange").value(EXCHANGE))
                .andExpect(jsonPath("$.channel").value(CHANNEL))
                .andExpect(jsonPath("$.symbol").value(SYMBOL))
                .andExpect(jsonPath("$.status").value("TERMINATED"))
                .andExpect(jsonPath("$.errorMessage").doesNotExist())
                .andExpect(jsonPath("$.createdAt").value(TEST_TIMESTAMP.toString()))
                .andExpect(jsonPath("$.updatedAt").value(TEST_TIMESTAMP.toString()));

        verify(subscriptionService).unsubscribe(TEST_ID);
        verifyNoMoreInteractions(subscriptionService);
    }

    @Test
    @DisplayName("6. DELETE /api/v1/subscriptions/{id}: при отсутствии подписки возвращает 404 SUBSCRIPTION_NOT_FOUND")
    void unsubscribe_whenNotFound_returns404NotFound() throws Exception {
        when(subscriptionService.unsubscribe(TEST_ID))
                .thenThrow(new SubscriptionNotFoundException(TEST_ID));

        mockMvc.perform(delete(BY_ID_URL, TEST_ID))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.status").value(404))
                .andExpect(jsonPath("$.error").value("SUBSCRIPTION_NOT_FOUND"))
                .andExpect(jsonPath("$.message").value("Подписка с идентификатором " + TEST_ID + " не найдена"));

        verify(subscriptionService).unsubscribe(TEST_ID);
        verifyNoMoreInteractions(subscriptionService);
    }

    // =========================================================================
    // Группа 3: GET /api/v1/subscriptions
    // =========================================================================

    @Test
    @DisplayName("7. GET /api/v1/subscriptions: без параметров возвращает 200 OK со списком всех подписок")
    void getSubscriptions_whenNoFilters_returns200OkWithList() throws Exception {
        SubscriptionResponse response = createResponse(Status.ACTIVE);
        when(subscriptionService.findAll(null, null, null)).thenReturn(List.of(response));

        mockMvc.perform(get(BASE_URL))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$").isArray())
                .andExpect(jsonPath("$.length()").value(1))
                .andExpect(jsonPath("$[0].id").value(TEST_ID.toString()))
                .andExpect(jsonPath("$[0].exchange").value(EXCHANGE))
                .andExpect(jsonPath("$[0].channel").value(CHANNEL))
                .andExpect(jsonPath("$[0].symbol").value(SYMBOL))
                .andExpect(jsonPath("$[0].status").value("ACTIVE"))
                .andExpect(jsonPath("$[0].createdAt").value(TEST_TIMESTAMP.toString()))
                .andExpect(jsonPath("$[0].updatedAt").value(TEST_TIMESTAMP.toString()));

        verify(subscriptionService).findAll(null, null, null);
        verifyNoMoreInteractions(subscriptionService);
    }

    @Test
    @DisplayName("8. GET /api/v1/subscriptions: с query-параметрами передает фильтры в сервис")
    void getSubscriptions_whenFiltered_passesFiltersAndReturnsList() throws Exception {
        SubscriptionResponse response = createResponse(Status.ACTIVE);
        when(subscriptionService.findAll(EXCHANGE, CHANNEL, Status.ACTIVE)).thenReturn(List.of(response));

        mockMvc.perform(get(BASE_URL)
                        .param("exchange", EXCHANGE)
                        .param("channel", CHANNEL)
                        .param("status", "ACTIVE"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$").isArray())
                .andExpect(jsonPath("$.length()").value(1))
                .andExpect(jsonPath("$[0].id").value(TEST_ID.toString()))
                .andExpect(jsonPath("$[0].exchange").value(EXCHANGE))
                .andExpect(jsonPath("$[0].channel").value(CHANNEL))
                .andExpect(jsonPath("$[0].symbol").value(SYMBOL))
                .andExpect(jsonPath("$[0].status").value("ACTIVE"))
                .andExpect(jsonPath("$[0].createdAt").value(TEST_TIMESTAMP.toString()))
                .andExpect(jsonPath("$[0].updatedAt").value(TEST_TIMESTAMP.toString()));

        verify(subscriptionService).findAll(EXCHANGE, CHANNEL, Status.ACTIVE);
        verifyNoMoreInteractions(subscriptionService);
    }

    // =========================================================================
    // Группа 4: GET /api/v1/subscriptions/{id}
    // =========================================================================

    @Test
    @DisplayName("9. GET /api/v1/subscriptions/{id}: при наличии записи возвращает 200 OK")
    void getById_whenExists_returns200Ok() throws Exception {
        SubscriptionResponse response = createResponse(Status.ACTIVE);
        when(subscriptionService.findById(TEST_ID)).thenReturn(response);

        mockMvc.perform(get(BY_ID_URL, TEST_ID))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.id").value(TEST_ID))
                .andExpect(jsonPath("$.exchange").value(EXCHANGE))
                .andExpect(jsonPath("$.channel").value(CHANNEL))
                .andExpect(jsonPath("$.symbol").value(SYMBOL))
                .andExpect(jsonPath("$.status").value("ACTIVE"))
                .andExpect(jsonPath("$.errorMessage").doesNotExist())
                .andExpect(jsonPath("$.createdAt").value(TEST_TIMESTAMP.toString()))
                .andExpect(jsonPath("$.updatedAt").value(TEST_TIMESTAMP.toString()));

        verify(subscriptionService).findById(TEST_ID);
        verifyNoMoreInteractions(subscriptionService);
    }

    @Test
    @DisplayName("10. GET /api/v1/subscriptions/{id}: при отсутствии возвращает 404 NOT_FOUND")
    void getById_whenNotFound_returns404NotFound() throws Exception {
        when(subscriptionService.findById(TEST_ID))
                .thenThrow(new SubscriptionNotFoundException(TEST_ID));

        mockMvc.perform(get(BY_ID_URL, TEST_ID))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.status").value(404))
                .andExpect(jsonPath("$.error").value("SUBSCRIPTION_NOT_FOUND"))
                .andExpect(jsonPath("$.message").value("Подписка с идентификатором " + TEST_ID + " не найдена"));

        verify(subscriptionService).findById(TEST_ID);
        verifyNoMoreInteractions(subscriptionService);
    }

    // =========================================================================
    // Группа 5: POST /api/v1/subscriptions/{id}/restart
    // =========================================================================

    @Test
    @DisplayName("11. POST /api/v1/subscriptions/{id}/restart: при валидном перезапуске возвращает 200 OK")
    void restart_whenValid_returns200Ok() throws Exception {
        SubscriptionResponse response = createResponse(Status.CONNECTING);
        when(subscriptionService.restart(TEST_ID)).thenReturn(response);

        mockMvc.perform(post(RESTART_URL, TEST_ID))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.id").value(TEST_ID))
                .andExpect(jsonPath("$.exchange").value(EXCHANGE))
                .andExpect(jsonPath("$.channel").value(CHANNEL))
                .andExpect(jsonPath("$.symbol").value(SYMBOL))
                .andExpect(jsonPath("$.status").value("CONNECTING"))
                .andExpect(jsonPath("$.errorMessage").doesNotExist())
                .andExpect(jsonPath("$.createdAt").value(TEST_TIMESTAMP.toString()))
                .andExpect(jsonPath("$.updatedAt").value(TEST_TIMESTAMP.toString()));

        verify(subscriptionService).restart(TEST_ID);
        verifyNoMoreInteractions(subscriptionService);
    }

    @Test
    @DisplayName("12. POST /api/v1/subscriptions/{id}/restart: при IllegalSubscriptionStateException возвращает 400 BAD_REQUEST")
    void restart_whenIllegalState_returns400BadRequest() throws Exception {
        when(subscriptionService.restart(TEST_ID))
                .thenThrow(new IllegalSubscriptionStateException("Перезапуск невозможен"));

        mockMvc.perform(post(RESTART_URL, TEST_ID))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.status").value(400))
                .andExpect(jsonPath("$.error").value("ILLEGAL_SUBSCRIPTION_STATE"))
                .andExpect(jsonPath("$.message").value("Перезапуск невозможен"));

        verify(subscriptionService).restart(TEST_ID);
        verifyNoMoreInteractions(subscriptionService);
    }

    // =========================================================================
    // Группа 6: Дополнительные граничные случаи
    // =========================================================================

    @Test
    @DisplayName("13. GET /api/v1/subscriptions: при отсутствии подписок возвращает 200 OK с пустым массивом []")
    void getSubscriptions_whenEmptyList_returns200OkWithEmptyArray() throws Exception {
        when(subscriptionService.findAll(null, null, null)).thenReturn(List.of());

        mockMvc.perform(get(BASE_URL))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$").isArray())
                .andExpect(jsonPath("$.length()").value(0));

        verify(subscriptionService).findAll(null, null, null);
        verifyNoMoreInteractions(subscriptionService);
    }
}
