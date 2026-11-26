package com.example.ehpoc;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.springframework.stereotype.Component;

/**
 * Thread-safe, bounded, in-memory view of the most recently consumed events. When full, the oldest event is
 * dropped first. This is only a view: clearing it never touches the event hub.
 */
@Component
public class EventBuffer {

    /** The most recent events kept. */
    public static final int CAPACITY = 200;

    private final ArrayDeque<ConsumedEvent> events = new ArrayDeque<>(CAPACITY);

    public synchronized void add(ConsumedEvent event) {
        if (events.size() >= CAPACITY) {
            events.removeFirst();
        }
        events.addLast(event);
    }

    /** A copy of the buffered events, newest first. */
    public synchronized List<ConsumedEvent> newestFirst() {
        List<ConsumedEvent> copy = new ArrayList<>(events);
        Collections.reverse(copy);
        return copy;
    }

    /** Empties the buffer and returns how many events it held. */
    public synchronized int clear() {
        int removed = events.size();
        events.clear();
        return removed;
    }

    public synchronized int size() {
        return events.size();
    }

    public int capacity() {
        return CAPACITY;
    }
}
