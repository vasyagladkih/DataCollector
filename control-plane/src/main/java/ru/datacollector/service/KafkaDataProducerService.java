package ru.datacollector.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class KafkaDataProducerService {

    private final KafkaTemplate<String, byte[]> kafkaTemplate;

    public void send(String topic, String key, byte[] message) {
        if (message == null || message.length == 0) return;
        kafkaTemplate.send(topic, key, message).whenComplete((result, ex) -> {
            if (ex != null) {
                log.error("[KAFKA-RAW-ERROR] Ошибка отправки в топик {} с ключом {}: {}", topic, key, ex.getMessage(), ex);
            }
        });
    }
}