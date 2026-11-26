package com.example.ehpoc;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.stereotype.Component;

/**
 * The "consumer" section of /api/diagnostics: the consumer group, the topic, whether the listener is running and
 * has partitions assigned, the buffer fill and the last consumer error (CLASS NAME only, never a message).
 * Facts come from the live listener container and the effective consumer configuration, not hard-coded values.
 */
@Component
public class ConsumerDiagnostics {

    private final KafkaListenerEndpointRegistry registry;
    private final ConsumerFactory<String, String> consumerFactory;
    private final EventBuffer buffer;
    private final EventStream stream;
    private final ConsumerStatus status;
    private final String topic;

    public ConsumerDiagnostics(KafkaListenerEndpointRegistry registry, ConsumerFactory<String, String> consumerFactory,
            EventBuffer buffer, EventStream stream, ConsumerStatus status,
            @Value("${ehpoc.eventhubs.topic}") String topic) {
        this.registry = registry;
        this.consumerFactory = consumerFactory;
        this.buffer = buffer;
        this.stream = stream;
        this.status = status;
        this.topic = topic;
    }

    public Map<String, Object> report() {
        Map<String, Object> config = consumerFactory.getConfigurationProperties();
        MessageListenerContainer container = registry.getListenerContainer(EventConsumer.LISTENER_ID);
        Collection<TopicPartition> assigned = container == null ? null : container.getAssignedPartitions();

        Map<String, Object> consumer = new LinkedHashMap<>();
        consumer.put("groupId", KafkaConfigValues.asString(config.get("group.id")));
        consumer.put("topic", topic);
        consumer.put("autoOffsetReset", KafkaConfigValues.asString(config.get("auto.offset.reset")));
        consumer.put("securityProtocol", KafkaConfigValues.asString(config.get("security.protocol")));
        consumer.put("saslMechanism", KafkaConfigValues.asString(config.get("sasl.mechanism")));
        consumer.put("listenerRunning", container != null && container.isRunning());
        // Greater than zero once the group has been joined; events sent before that are skipped (offset reset "latest").
        consumer.put("assignedPartitions", assigned == null ? 0 : assigned.size());
        consumer.put("bufferedCount", buffer.size());
        consumer.put("bufferCapacity", buffer.capacity());
        consumer.put("sseClients", stream.clientCount());
        consumer.put("lastConsumedAt", status.lastConsumedAt() == null ? null : status.lastConsumedAt().toString());
        consumer.put("lastErrorClass", status.lastErrorClass());
        consumer.put("lastErrorAt", status.lastErrorAt() == null ? null : status.lastErrorAt().toString());
        return consumer;
    }
}
