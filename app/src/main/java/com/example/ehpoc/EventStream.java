package com.example.ehpoc;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;
import tools.jackson.databind.json.JsonMapper;

/**
 * Server-Sent Events fan-out. One single-threaded executor does all sending, so a slow browser never blocks
 * the Kafka consumer thread and events keep their order. Emitters that fail to send, time out or complete are
 * removed; a heartbeat comment goes to every client periodically so idle intermediaries do not drop the connection.
 *
 * <p>SSE event names: {@code event} (data: a {@link ConsumedEvent}) and {@code cleared} (data:
 * {@code {"removed":n,"clearedAt":"<instant>"}}). Heartbeats are comment lines, not events.
 */
@Component
public class EventStream {

    private static final Logger log = LoggerFactory.getLogger(EventStream.class);

    public static final String EVENT = "event";
    public static final String CLEARED = "cleared";

    private final JsonMapper json;
    private final Duration heartbeatInterval;
    private final CopyOnWriteArraySet<SseEmitter> emitters = new CopyOnWriteArraySet<>();
    private final ScheduledExecutorService sender = DaemonScheduler.singleThread("sse-sender");

    public EventStream(JsonMapper json, @Value("${ehpoc.sse.heartbeat-interval:15s}") Duration heartbeatInterval) {
        this.json = json;
        this.heartbeatInterval = heartbeatInterval;
    }

    @PostConstruct
    void startHeartbeat() {
        long millis = heartbeatInterval.toMillis();
        sender.scheduleWithFixedDelay(() -> sendToAll(SseEmitter.event().comment("keepalive")), millis, millis,
                TimeUnit.MILLISECONDS);
    }

    /** Registers a new client. No timeout: the connection lives until the client leaves or the app stops. */
    public SseEmitter subscribe() {
        SseEmitter emitter = new SseEmitter(0L);
        emitter.onCompletion(() -> emitters.remove(emitter));
        emitter.onTimeout(() -> emitters.remove(emitter));
        emitter.onError(error -> emitters.remove(emitter));
        emitters.add(emitter);
        // Flushes the response headers so the client sees the connection is open before any event arrives.
        send(emitter, SseEmitter.event().comment("connected"));
        return emitter;
    }

    public int clientCount() {
        return emitters.size();
    }

    public void publishEvent(ConsumedEvent event) {
        publish(EVENT, event);
    }

    public void publishCleared(int removed) {
        publish(CLEARED, Map.of("removed", removed, "clearedAt", Instant.now().toString()));
    }

    private void publish(String name, Object data) {
        if (emitters.isEmpty()) {
            return;
        }
        String payload = json.writeValueAsString(data);
        sender.execute(() -> sendToAll(SseEmitter.event().name(name).data(payload)));
    }

    private void sendToAll(SseEmitter.SseEventBuilder event) {
        for (SseEmitter emitter : emitters) {
            send(emitter, event);
        }
    }

    private void send(SseEmitter emitter, SseEmitter.SseEventBuilder event) {
        try {
            emitter.send(event);
        }
        catch (Exception e) {
            // A dead or stalled client (broken pipe, already completed): drop it, never let it affect the others.
            drop(emitter);
            log.debug("Dropped an SSE client ({})", e.getClass().getSimpleName());
        }
    }

    /** Ends open streams as soon as shutdown starts so they do not hold up graceful shutdown. */
    @EventListener(ContextClosedEvent.class)
    void completeAll() {
        for (SseEmitter emitter : emitters) {
            drop(emitter);
        }
    }

    /** Forgets the client and ends its stream; completing an already unusable emitter is not an error. */
    private void drop(SseEmitter emitter) {
        emitters.remove(emitter);
        try {
            emitter.complete();
        }
        catch (Exception ignored) {
            // already unusable
        }
    }

    @PreDestroy
    void stop() {
        sender.shutdownNow();
    }
}
