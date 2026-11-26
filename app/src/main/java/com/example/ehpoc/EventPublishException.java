package com.example.ehpoc;

/**
 * Publishing to Event Hubs failed or was not acknowledged in time (maps to HTTP 503).
 *
 * <p>The message is safe to return to clients: it names a coarse reason and exception class names only,
 * never exception messages, which may carry endpoint or identity details.
 */
public class EventPublishException extends RuntimeException {

    private final PublishFailureReason reason;

    public EventPublishException(PublishFailureReason reason, String message, Throwable cause) {
        super(message, cause);
        this.reason = reason;
    }

    /** The coarse category; its {@link PublishFailureReason#value() wire value} is returned to API clients. */
    public PublishFailureReason reason() {
        return reason;
    }
}
