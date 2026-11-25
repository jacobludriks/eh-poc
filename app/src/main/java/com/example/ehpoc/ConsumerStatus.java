package com.example.ehpoc;

import java.time.Instant;

import org.springframework.stereotype.Component;

/**
 * What the consumer has done and last failed at, for /api/diagnostics. Records exception CLASS NAMES only:
 * exception messages from the Kafka and identity libraries can carry endpoint or identity details.
 */
@Component
public class ConsumerStatus {

    private volatile Instant lastConsumedAt;
    private volatile String lastErrorClass;
    private volatile Instant lastErrorAt;

    public void consumed() {
        lastConsumedAt = Instant.now();
    }

    public void failed(Throwable error) {
        // Name the outermost and the root cause (e.g. KafkaException <- CredentialUnavailableException).
        Throwable root = error;
        while (root.getCause() != null && root.getCause() != root) {
            root = root.getCause();
        }
        failed(root == error ? error.getClass().getName()
                : error.getClass().getName() + " <- " + root.getClass().getName());
    }

    /** Records a failure known only by the (class) name of the Kafka error behind it. */
    public void failed(String errorClass) {
        lastErrorClass = errorClass;
        lastErrorAt = Instant.now();
    }

    public Instant lastConsumedAt() {
        return lastConsumedAt;
    }

    public String lastErrorClass() {
        return lastErrorClass;
    }

    public Instant lastErrorAt() {
        return lastErrorAt;
    }
}
