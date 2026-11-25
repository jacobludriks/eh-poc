package com.example.ehpoc;

/**
 * Publishing to Event Hubs failed or was not acknowledged in time (maps to HTTP 503).
 *
 * <p>The message is safe to return to clients: it names a coarse reason and exception class names only,
 * never exception messages, which may carry endpoint or identity details.
 */
public class EventPublishException extends RuntimeException {

    private final String reason;

    public EventPublishException(String reason, String message, Throwable cause) {
        super(message, cause);
        this.reason = reason;
    }

    /** One of {@code timeout}, {@code authentication}, {@code authorization}, {@code unavailable}. */
    public String reason() {
        return reason;
    }
}
