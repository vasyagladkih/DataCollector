package ru.connector.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;


@Component
public class KafkaRawDataPublisher {

    private static final Logger log = LoggerFactory.getLogger(KafkaRawDataPublisher.class);

    private final KafkaTemplate<String, byte[]> kafkaTemplate;
    private final String defaultTopic;

    public KafkaRawDataPublisher(
            KafkaTemplate<String, byte[]> kafkaTemplate,
            @Value("${kafka.topics.raw:market.data.raw}") String defaultTopic) {
        this.kafkaTemplate = kafkaTemplate;
        this.defaultTopic = defaultTopic;
    }

    public void publish(String key, byte[] payload) {
        publish(defaultTopic, key, payload);
    }

    public void publish(String topic, String key, byte[] payload) {
        if (payload == null || payload.length == 0) return;
        String targetTopic = (topic != null && !topic.isBlank()) ? topic : defaultTopic;

        kafkaTemplate.send(targetTopic, key, payload).whenComplete((_, ex) -> {
            if (ex != null) log.error("[KAFKA-RAW-ERROR] Failed to send message to topic={} key={}", targetTopic, key, ex);
        });
    }
}