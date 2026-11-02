package ru.connector.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import tools.jackson.core.JacksonException;
import tools.jackson.core.JsonParser;
import tools.jackson.databind.DeserializationContext;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ValueDeserializer;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.module.SimpleModule;
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.support.serializer.JacksonJsonSerializer;
import ru.datacollector.common.model.BookTickerCommand;
import ru.datacollector.common.model.Command;
import ru.datacollector.common.model.CreateSubscriptionRequest;
import ru.datacollector.common.model.OrderBookCommand;
import ru.datacollector.common.model.TradesCommand;

import java.util.Map;

@Configuration
public class KafkaConfig {

    @Bean
    @Primary
    public JsonMapper jsonMapper() {
        return createJsonMapper();
    }

    public static JsonMapper createJsonMapper() {
        SimpleModule module = new SimpleModule();
        module.addDeserializer(Command.class, new ValueDeserializer<>() {
            @Override
            public Command deserialize(JsonParser p, DeserializationContext ctxt) throws JacksonException {
                JsonNode node = ctxt.readTree(p);
                if (node == null || node.isNull()) return null;
                JsonNode typeNode = node.get("type");
                if (typeNode == null || typeNode.isNull()) return null;
                String type = typeNode.asString();
                return switch (type) {
                    case "trades" -> ctxt.readTreeAsValue(node, TradesCommand.class);
                    case "book_ticker", "ticker" -> ctxt.readTreeAsValue(node, BookTickerCommand.class);
                    case "orderbook" -> ctxt.readTreeAsValue(node, OrderBookCommand.class);
                    default -> throw new IllegalArgumentException("Unknown command type: " + type);
                };
            }
        });
        return JsonMapper.builder()
                .addModule(module)
                .build();
    }

    @Bean
    public NewTopic subscriptionStateTopic(@Value("${kafka.topics.subscription-state:market.subscriptions}") String topic) {
        return TopicBuilder.name(topic)
                .partitions(3)
                .replicas(1)
                .compact()
                .build();
    }

    @Bean
    public NewTopic rawDataTopic(@Value("${kafka.topics.raw:market.data.raw}") String topic) {
        return TopicBuilder.name(topic)
                .partitions(12)
                .replicas(1)
                .build();
    }

    @Bean
    public KafkaTemplate<String, byte[]> rawDataKafkaTemplate(ProducerFactory<Object, Object> producerFactory) {
        Map<String, Object> configs = producerFactory.getConfigurationProperties();
        return new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(configs, new StringSerializer(), new ByteArraySerializer()));
    }

    @Bean
    public KafkaTemplate<String, CreateSubscriptionRequest> subscriptionKafkaTemplate(
            ProducerFactory<Object, Object> producerFactory,
            JsonMapper jsonMapper) {
        Map<String, Object> configs = producerFactory.getConfigurationProperties();
        return new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(
                configs,
                new StringSerializer(),
                new JacksonJsonSerializer<>(jsonMapper))
        );
    }
}