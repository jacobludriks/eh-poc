package com.example.ehpoc;

/** A published event together with the broker acknowledgement (where Event Hubs stored it). */
public record PublishResult(Event event, String topic, int partition, long offset) {
}
