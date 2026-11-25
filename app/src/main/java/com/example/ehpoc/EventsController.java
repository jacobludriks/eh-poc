package com.example.ehpoc;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.springframework.http.CacheControl;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

/** HTTP surface for events: send (to the hub), list and stream (from the consumer), clear (the in-memory view). */
@RestController
@RequestMapping("/api/events")
public class EventsController {

    /** Longest accepted message (characters). Far below the Event Hubs request size cap (max.request.size 1,000,000). */
    static final int MAX_MESSAGE_LENGTH = 1000;

    private static final Set<String> SOURCES = Set.of(Event.SOURCE_API, Event.SOURCE_DASHBOARD);

    /** Body of POST /api/events. */
    public record SendEventRequest(String message, String source) {
    }

    private final EventPublisher publisher;
    private final EventBuffer buffer;
    private final EventStream stream;

    public EventsController(EventPublisher publisher, EventBuffer buffer, EventStream stream) {
        this.publisher = publisher;
        this.buffer = buffer;
        this.stream = stream;
    }

    /** The buffered (consumed) events, newest first, with the buffer's fill and capacity. */
    @GetMapping
    public ResponseEntity<Map<String, Object>> list() {
        List<ConsumedEvent> events = buffer.newestFirst();
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("count", events.size());
        body.put("capacity", buffer.capacity());
        body.put("events", events);
        return ResponseEntity.ok().cacheControl(CacheControl.noStore()).body(body);
    }

    /** Server-Sent Events: each newly consumed event as it arrives, and a notification when the view is cleared. */
    @GetMapping(path = "/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public ResponseEntity<SseEmitter> stream() {
        return ResponseEntity.ok()
                .cacheControl(CacheControl.noCache())
                // Tells reverse proxies not to buffer the stream.
                .header("X-Accel-Buffering", "no")
                .body(stream.subscribe());
    }

    /** Empties the in-memory view only. The event hub itself is untouched. */
    @PostMapping("/clear")
    public Map<String, Object> clear() {
        int removed = buffer.clear();
        stream.publishCleared(removed);
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("cleared", removed);
        body.put("scope", "in-memory view");
        body.put("eventHubPurged", false);
        body.put("message", "Cleared the in-memory view only. The event hub itself is not purged: its events remain "
                + "until the retention period expires.");
        return body;
    }

    @PostMapping
    public ResponseEntity<PublishResult> send(@RequestBody SendEventRequest request) {
        String message = request.message();
        if (message == null || message.isBlank()) {
            throw new InvalidEventException("message must not be blank");
        }
        if (message.length() > MAX_MESSAGE_LENGTH) {
            throw new InvalidEventException("message must be at most " + MAX_MESSAGE_LENGTH + " characters");
        }
        String source = request.source() == null || request.source().isBlank() ? Event.SOURCE_API : request.source();
        if (!SOURCES.contains(source)) {
            throw new InvalidEventException("source must be one of " + SOURCES.stream().sorted().toList());
        }
        return ResponseEntity.status(HttpStatus.CREATED).body(publisher.publish(message, source));
    }

    @ExceptionHandler(InvalidEventException.class)
    ResponseEntity<Map<String, Object>> invalid(InvalidEventException e) {
        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(errorBody("invalid_event", e.getMessage()));
    }

    @ExceptionHandler(EventPublishException.class)
    ResponseEntity<Map<String, Object>> publishFailed(EventPublishException e) {
        Map<String, Object> body = errorBody("publish_failed", e.getMessage());
        body.put("reason", e.reason());
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body(body);
    }

    private static Map<String, Object> errorBody(String error, String message) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("error", error);
        body.put("message", message);
        return body;
    }
}
