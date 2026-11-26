package com.example.ehpoc;

import java.time.Instant;
import java.util.UUID;

/**
 * The event published to (and, from ticket 04, consumed from) the event hub. Serialised as JSON:
 * {@code {"id":"<uuid>","message":"...","sentAt":"<ISO-8601 instant>","source":"api|dashboard"}}.
 * The id is also the Kafka record key.
 */
public record Event(String id, String message, Instant sentAt, String source) {

    /** Allowed values for {@link #source()}. */
    public static final String SOURCE_API = "api";
    public static final String SOURCE_DASHBOARD = "dashboard";

    public static Event create(String message, String source) {
        return new Event(UUID.randomUUID().toString(), message, Instant.now(), source);
    }
}
