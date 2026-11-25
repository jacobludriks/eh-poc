package com.example.ehpoc;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import tools.jackson.databind.json.JsonMapper;

/**
 * Reads events back from the event hub through spring-kafka and puts them in the {@link EventBuffer} and on the
 * SSE stream. As with the publisher, authentication is entirely the Kafka client's (Spring Cloud Azure's
 * OAUTHBEARER callback handler with the workload identity); nothing here knows about credentials.
 *
 * <p>The listener is NOT auto-started: {@link ConsumerLifecycle} starts it after the app is up, and keeps retrying,
 * so a Kafka login or connection failure shows up in /api/diagnostics instead of crashing the pod.
 *
 * <p>Group id (dashboard), offset reset (latest) and acknowledgement come from the standard spring.kafka settings
 * in application.yml. A malformed message is logged (class and location only, never the payload) and skipped.
 */
@Component
public class EventConsumer {

    static final String LISTENER_ID = "dashboard-consumer";

    private static final Logger log = LoggerFactory.getLogger(EventConsumer.class);

    private final JsonMapper json;
    private final EventBuffer buffer;
    private final EventStream stream;
    private final ConsumerStatus status;

    public EventConsumer(JsonMapper json, EventBuffer buffer, EventStream stream, ConsumerStatus status) {
        this.json = json;
        this.buffer = buffer;
        this.stream = stream;
        this.status = status;
    }

    @KafkaListener(id = LISTENER_ID, idIsGroup = false, topics = "${ehpoc.eventhubs.topic}", autoStartup = "false")
    public void onMessage(ConsumerRecord<String, String> record) {
        Event event = parse(record);
        if (event == null) {
            return;
        }
        ConsumedEvent consumed = ConsumedEvent.of(event, record.key(), record.partition(), record.offset());
        buffer.add(consumed);
        status.consumed();
        stream.publishEvent(consumed);
    }

    private Event parse(ConsumerRecord<String, String> record) {
        try {
            Event event = record.value() == null ? null : json.readValue(record.value(), Event.class);
            if (event == null || event.id() == null || event.message() == null) {
                skip(record, "not an event", null);
                return null;
            }
            return event;
        }
        catch (RuntimeException e) {
            skip(record, "not valid event JSON", e);
            return null;
        }
    }

    private void skip(ConsumerRecord<String, String> record, String why, RuntimeException cause) {
        log.warn("Skipping malformed message ({}{}) from {}-{} at offset {}", why,
                cause == null ? "" : ": " + cause.getClass().getSimpleName(), record.topic(), record.partition(),
                record.offset());
    }
}
