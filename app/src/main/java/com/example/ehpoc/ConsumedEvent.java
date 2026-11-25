package com.example.ehpoc;

import java.time.Instant;

/**
 * An event as it came back through the consumer: the {@link Event} fields plus where it was read from.
 * {@code key} is the Kafka record key (the publisher sets it to the event id), {@code partition} and
 * {@code offset} locate it in the event hub, and {@code consumedAt} is when this app read it.
 *
 * <p>Serialised as flat JSON: {@code {"id","message","sentAt","source","key","partition","offset","consumedAt"}}.
 */
public record ConsumedEvent(String id, String message, Instant sentAt, String source, String key, int partition,
        long offset, Instant consumedAt) {

    public static ConsumedEvent of(Event event, String key, int partition, long offset) {
        return new ConsumedEvent(event.id(), event.message(), event.sentAt(), event.source(), key, partition, offset,
                Instant.now());
    }
}
