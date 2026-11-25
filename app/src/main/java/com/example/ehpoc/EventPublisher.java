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
            throw new EventPublishException("unavailable", "Publishing was interrupted.", e);
        }
        catch (TimeoutException e) {
            log.warn("Publish to '{}' not acknowledged within {}s", topic, ACK_TIMEOUT_SECONDS);
            throw new EventPublishException("timeout",
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
        String chain = "";
        String reason = "unavailable";
        for (Throwable t = failure; t != null; t = t.getCause()) {
            String name = t.getClass().getName();
            chain = chain.isEmpty() ? t.getClass().getSimpleName() : chain + " <- " + t.getClass().getSimpleName();
            if (reason.equals("unavailable")) {
                if (name.contains("Authorization")) {
                    reason = "authorization";
                }
                else if (name.contains("Authentication") || name.contains("CredentialUnavailable") || name.contains("LoginException")) {
                    reason = "authentication";
                }
                else if (name.contains("Timeout")) {
                    reason = "timeout";
                }
            }
        }
        String description = switch (reason) {
            case "authentication" -> "Authentication to Event Hubs failed (workload identity token not obtained or rejected).";
            case "authorization" -> "Event Hubs rejected the identity as not authorised to send. Role assignments can take up to five minutes to take effect.";
            case "timeout" -> "Event Hubs could not be reached in time.";
            default -> "Event Hubs is unavailable.";
        };
        return new EventPublishException(reason, description + " (" + chain + ")", failure);
    }
}
