package com.example.ehpoc;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.common.errors.SaslAuthenticationException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

/** Pure unit test of how a publish failure becomes a coarse reason and the API's 503 body. */
class PublishFailureClassificationTest {

    @Test
    void authorizationFailureIsAuthorization() {
        EventPublishException e = EventPublisher.classify(new TopicAuthorizationException("denied"));

        assertThat(e.reason().value()).isEqualTo("authorization");
        assertThat(e.getMessage()).contains("not authorised to send").contains("TopicAuthorizationException");
    }

    @Test
    void authenticationFailuresAreAuthentication() {
        assertThat(EventPublisher.classify(new SaslAuthenticationException("bad")).reason().value())
                .isEqualTo("authentication");
        assertThat(EventPublisher.classify(new RuntimeException("wrapped", new javax.security.auth.login.LoginException("x")))
                .reason().value()).isEqualTo("authentication");
        // Matched by class name, so the Azure SDK's exception need not be on this test's classpath.
        assertThat(EventPublisher.classify(new IllegalStateException(new CredentialUnavailableException()))
                .reason().value()).isEqualTo("authentication");
    }

    /** Stand-in with the same simple name as the Azure Identity exception raised when no credential is obtained. */
    static class CredentialUnavailableException extends RuntimeException {
    }

    @Test
    void timeoutIsTimeout() {
        EventPublishException e = EventPublisher.classify(new org.apache.kafka.common.errors.TimeoutException("slow"));

        assertThat(e.reason().value()).isEqualTo("timeout");
        assertThat(e.getMessage()).startsWith("Event Hubs could not be reached in time.");
    }

    @Test
    void anythingElseIsUnavailable() {
        EventPublishException e = EventPublisher.classify(new IOException("connection reset"));

        assertThat(e.reason().value()).isEqualTo("unavailable");
        assertThat(e.getMessage()).startsWith("Event Hubs is unavailable.").contains("(IOException)");
    }

    @Test
    void theOutermostMatchingCauseWinsAndTheWholeChainIsNamed() {
        Throwable failure = new RuntimeException("top",
                new SaslAuthenticationException("auth", new TimeoutException("inner timeout")));

        EventPublishException e = EventPublisher.classify(failure);

        assertThat(e.reason().value()).isEqualTo("authentication");
        assertThat(e.getMessage()).endsWith("(RuntimeException <- SaslAuthenticationException <- TimeoutException)");
    }

    @Test
    void theErrorBodyKeepsTheDocumentedWireValues() {
        for (String reason : new String[] { "timeout", "authentication", "authorization", "unavailable" }) {
            PublishFailureReason parsed = PublishFailureReason.valueOf(reason.toUpperCase(java.util.Locale.ROOT));
            ResponseEntity<Map<String, Object>> response = new EventsController(null, null, null)
                    .publishFailed(new EventPublishException(parsed, "m", null));

            assertThat(response.getStatusCode()).isEqualTo(HttpStatus.SERVICE_UNAVAILABLE);
            assertThat(response.getBody()).containsEntry("error", "publish_failed").containsEntry("reason", reason);
        }
    }
}
