package com.example.ehpoc;

import java.util.List;

/**
 * Why a publish failed, as a coarse and safe category. {@link #value()} is the wire value of the API's
 * {@code reason} field. Failures are recognised by exception class NAME fragments (never messages, which may carry
 * endpoint or identity details), so the Azure and Kafka exception types need not be referenced here.
 */
public enum PublishFailureReason {

    // Declaration order is the precedence when one exception name matches several categories.
    AUTHORIZATION("authorization",
            "Event Hubs rejected the identity as not authorised to send. Role assignments can take up to five minutes to take effect.",
            "Authorization"),
    AUTHENTICATION("authentication",
            "Authentication to Event Hubs failed (workload identity token not obtained or rejected).",
            "Authentication", "CredentialUnavailable", "LoginException"),
    TIMEOUT("timeout", "Event Hubs could not be reached in time.", "Timeout"),
    UNAVAILABLE("unavailable", "Event Hubs is unavailable.");

    private final String value;
    private final String description;
    private final List<String> classNameFragments;

    PublishFailureReason(String value, String description, String... classNameFragments) {
        this.value = value;
        this.description = description;
        this.classNameFragments = List.of(classNameFragments);
    }

    /** The wire value: one of timeout, authentication, authorization, unavailable. */
    public String value() {
        return value;
    }

    /** A safe, human-readable sentence for this category. */
    public String description() {
        return description;
    }

    /** The category of a failure chain: the outermost cause whose class name matches a category decides. */
    static PublishFailureReason of(Throwable failure) {
        for (Throwable t = failure; t != null; t = t.getCause()) {
            String name = t.getClass().getName();
            for (PublishFailureReason reason : values()) {
                if (reason.classNameFragments.stream().anyMatch(name::contains)) {
                    return reason;
                }
            }
        }
        return UNAVAILABLE;
    }
}
