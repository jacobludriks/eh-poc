package com.example.ehpoc;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/** HTTP surface for events. Ticket 04 adds list, stream and clear next to the send below. */
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

    public EventsController(EventPublisher publisher) {
        this.publisher = publisher;
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
