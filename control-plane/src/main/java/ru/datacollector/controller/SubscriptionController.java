package ru.datacollector.controller;

import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import ru.datacollector.dto.CreateSubscriptionRequest;
import ru.datacollector.dto.SubscriptionResponse;
import ru.datacollector.model.Status;
import ru.datacollector.service.SubscriptionService;

import java.util.List;
import java.util.UUID;

@RequiredArgsConstructor
@RestController
@RequestMapping("/api/v1/subscriptions")
public class SubscriptionController {

    private final SubscriptionService subscriptionService;

    @PostMapping
    public ResponseEntity<SubscriptionResponse> subscribe(
            @Valid @RequestBody CreateSubscriptionRequest createSubscriptionRequest) {

        return ResponseEntity
                .status(HttpStatus.CREATED)
                .body(subscriptionService.subscribe(createSubscriptionRequest));
    }

    @DeleteMapping("/{id}")
    public ResponseEntity<SubscriptionResponse> unsubscribe(@PathVariable UUID id) {
        return ResponseEntity.ok(subscriptionService.unsubscribe(id));
    }

    @GetMapping
    public ResponseEntity<List<SubscriptionResponse>> getSubscriptions(
            @RequestParam(required = false) String exchange,
            @RequestParam(required = false) String channel,
            @RequestParam(required = false) Status status
    ) {
        List<SubscriptionResponse> subscriptions = subscriptionService.findAll(
                exchange,
                channel,
                status
        );
        return ResponseEntity.ok(subscriptions);
    }

    @GetMapping("/{id}")
    public ResponseEntity<SubscriptionResponse> getById(@PathVariable UUID id) {
        return ResponseEntity.ok(subscriptionService.findById(id));
    }

    @PostMapping("/{id}/restart")
    public ResponseEntity<SubscriptionResponse> restart(@PathVariable UUID id) {
        return ResponseEntity.ok(subscriptionService.restart(id));
    }
}
