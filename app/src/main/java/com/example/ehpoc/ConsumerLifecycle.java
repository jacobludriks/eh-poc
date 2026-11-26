package com.example.ehpoc;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.event.ConsumerRetryAuthEvent;
import org.springframework.kafka.event.ConsumerStoppedEvent;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.stereotype.Component;

/**
 * Keeps the Kafka listener running without ever letting it take the app down.
 *
 * <p>Creating a Kafka consumer performs the OAUTHBEARER login (a workload identity token request) on the calling
 * thread, so with an auto-started listener a missing identity, a not-yet-propagated role assignment or an
 * unreachable hub would fail application start and crash-loop the pod, hiding the very failure the diagnostics
 * are meant to show. Instead the listener starts after the app is ready, on a background thread, and is
 * retried while it is not running. Failures are recorded (class names only) for /api/diagnostics. Readiness and
 * liveness probes do not depend on Kafka, so the pod stays Ready while this retries.
 */
@Component
public class ConsumerLifecycle {

    private static final Logger log = LoggerFactory.getLogger(ConsumerLifecycle.class);

    static final long RETRY_SECONDS = 15;

    private final KafkaListenerEndpointRegistry registry;
    private final ConsumerStatus status;
    private final ScheduledExecutorService supervisor = DaemonScheduler.singleThread("consumer-supervisor");

    public ConsumerLifecycle(KafkaListenerEndpointRegistry registry, ConsumerStatus status) {
        this.registry = registry;
        this.status = status;
    }

    @EventListener(ApplicationReadyEvent.class)
    void begin() {
        supervisor.scheduleWithFixedDelay(this::ensureRunning, 0, RETRY_SECONDS, TimeUnit.SECONDS);
    }

    private void ensureRunning() {
        MessageListenerContainer container = registry.getListenerContainer(EventConsumer.LISTENER_ID);
        if (container == null || container.isRunning()) {
            return;
        }
        try {
            container.start();
            log.info("Kafka listener '{}' started", EventConsumer.LISTENER_ID);
        }
        catch (Throwable e) {
            status.failed(e);
            log.warn("Kafka listener '{}' failed to start ({}); retrying in {}s", EventConsumer.LISTENER_ID,
                    e.getClass().getName(), RETRY_SECONDS);
            log.debug("Kafka listener start failure", e);
            // A container whose consumer could not be created still reports running; reset it so the next try
            // starts cleanly and diagnostics do not show a listener that is not really there.
            try {
                container.stop();
            }
            catch (Throwable ignored) {
                // nothing more to clean up
            }
        }
    }

    /**
     * The container is retrying after the broker rejected the identity (published because
     * spring.kafka.listener.auth-exception-retry-interval is set). Typical causes: the workload identity token was
     * refused, or the role assignment has not propagated yet.
     */
    @EventListener(ConsumerRetryAuthEvent.class)
    void authRetry(ConsumerRetryAuthEvent event) {
        String errorClass = event.getReason() == ConsumerRetryAuthEvent.Reason.AUTHORIZATION
                ? "org.apache.kafka.common.errors.AuthorizationException"
                : "org.apache.kafka.common.errors.AuthenticationException";
        status.failed(errorClass);
        log.warn("Kafka consumer is retrying after {}", errorClass);
    }

    /** The consumer thread stopped for a reason other than a normal stop; the supervisor restarts it. */
    @EventListener(ConsumerStoppedEvent.class)
    void consumerStopped(ConsumerStoppedEvent event) {
        if (event.getReason() != ConsumerStoppedEvent.Reason.NORMAL) {
            status.failed("org.springframework.kafka.event.ConsumerStoppedEvent." + event.getReason());
            log.warn("Kafka consumer stopped abnormally ({})", event.getReason());
        }
    }

    @EventListener(ContextClosedEvent.class)
    void stop() {
        supervisor.shutdownNow();
    }
}
