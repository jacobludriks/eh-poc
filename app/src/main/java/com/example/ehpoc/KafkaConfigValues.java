package com.example.ehpoc;

import org.apache.kafka.common.config.types.Password;

/**
 * Renders effective Kafka client configuration values for /api/diagnostics. The one place that knows a
 * {@link Password}-typed value (the JAAS configuration) must never be returned: it renders as Kafka's own mask,
 * and callers that only need to know whether it is set, or carries a marker, ask a yes/no question instead.
 */
final class KafkaConfigValues {

    private KafkaConfigValues() {
    }

    /** The value as safe text: classes by name, passwords masked ("[hidden]"), everything else as its string form. */
    static String asString(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Class<?> type) {
            return type.getName();
        }
        return value.toString();
    }

    /** Whether the value is set and not blank (reads a password's content, never returns it). */
    static boolean isPresent(Object value) {
        String text = rawText(value);
        return text != null && !text.isBlank();
    }

    /** Whether the value's text contains the marker (reads a password's content, never returns it). */
    static boolean contains(Object value, String marker) {
        String text = rawText(value);
        return text != null && text.contains(marker);
    }

    private static String rawText(Object value) {
        if (value == null) {
            return null;
        }
        return value instanceof Password password ? password.value() : value.toString();
    }
}
