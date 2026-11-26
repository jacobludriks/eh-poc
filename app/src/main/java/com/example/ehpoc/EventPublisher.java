package com.example.ehpoc;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import tools.jackson.databind.json.JsonMapper;

/**
 * Publishes events to the event hub through spring-kafka. Authentication is entirely the Kafka client's:
 * Spring Cloud Azure's auto-configured OAUTHBEARER callback handler obtains a workload identity token.
 * Nothing in this class knows about credentials.
 *
 * <p>Waits for the broker acknowledgement with a bounded timeout so an unreachable or unauthorised hub
 * becomes a quick, clear error rather than a hung request.
 */
@Service
public class EventPublisher {

    private static final Logger log = LoggerFactory.getLogger(EventPublisher.class);

    /** Upper bound for the whole send. The producer's own max.block.ms bounds the metadata/login wait inside it. */
    static final long ACK_TIMEOUT_SECONDS = 30;

    private final KafkaTemplate<String, String> kafka;
    private final JsonMapper json;
    private final String topic;

    public EventPublisher(KafkaTemplate<String, String> kafka, JsonMapper json,
            @Value("${ehpoc.eventhubs.topic}") String topic) {
        this.kafka = kafka;
        this.json = json;
        this.topic = topic;
    }

    public String topic() {
        return topic;
    }

    /** Creates the event record, publishes it keyed by its id, and returns it with the broker acknowledgement. */
    public PublishResult publish(String message, String source) {
        Event event = Event.create(message, source);
        String payload = json.writeValueAsString(event);
        try {
            SendResult<String, String> sent = kafka.send(topic, event.id(), payload).get(ACK_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            RecordMetadata metadata = sent.getRecordMetadata();
            return new PublishResult(event, topic, metadata.partition(), metadata.offset());
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new EventPublishException(PublishFailureReason.UNAVAILABLE, "Publishing was interrupted.", e);
        }
        catch (TimeoutException e) {
            log.warn("Publish to '{}' not acknowledged within {}s", topic, ACK_TIMEOUT_SECONDS);
            throw new EventPublishException(PublishFailureReason.TIMEOUT,
                    "Event Hubs did not acknowledge the event within " + ACK_TIMEOUT_SECONDS + " seconds.", e);
        }
        catch (ExecutionException | RuntimeException e) {
            Throwable cause = e instanceof ExecutionException && e.getCause() != null ? e.getCause() : e;
            log.warn("Publish to '{}' failed", topic, cause);
            throw classify(cause);
        }
    }

    /** Maps a failure to a coarse, safe description by class name only; the full exception is logged, not returned. */
    static EventPublishException classify(Throwable failure) {
        StringBuilder chain = new StringBuilder();
        for (Throwable t = failure; t != null; t = t.getCause()) {
            chain.append(chain.isEmpty() ? "" : " <- ").append(t.getClass().getSimpleName());
        }
        PublishFailureReason reason = PublishFailureReason.of(failure);
        return new EventPublishException(reason, reason.description() + " (" + chain + ")", failure);
    }
}
