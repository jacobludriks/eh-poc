package com.example.ehpoc;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.kafka.common.config.types.Password;
import org.junit.jupiter.api.Test;

/** Pure unit test of how effective Kafka client configuration values are rendered for diagnostics. */
class KafkaConfigValuesTest {

    @Test
    void passwordTypedValuesAreMaskedAndTheirValueIsNeverReturned() {
        String rendered = KafkaConfigValues.asString(new Password("super-secret-jaas"));

        assertThat(rendered).doesNotContain("super-secret-jaas");
        assertThat(rendered).isEqualTo("[hidden]");
    }

    @Test
    void plainValuesAndClassesRenderAsText() {
        assertThat(KafkaConfigValues.asString(null)).isNull();
        assertThat(KafkaConfigValues.asString("dashboard")).isEqualTo("dashboard");
        assertThat(KafkaConfigValues.asString(String.class)).isEqualTo("java.lang.String");
        assertThat(KafkaConfigValues.asString(2)).isEqualTo("2");
    }

    @Test
    void presenceAndMarkerChecksSeeInsideAPasswordWithoutReturningIt() {
        Object jaas = new Password("OAuthBearerLoginModule required azure.configured=\"true\";");

        assertThat(KafkaConfigValues.isPresent(jaas)).isTrue();
        assertThat(KafkaConfigValues.contains(jaas, "azure.configured")).isTrue();
        assertThat(KafkaConfigValues.contains(jaas, "something-else")).isFalse();
        assertThat(KafkaConfigValues.isPresent(null)).isFalse();
        assertThat(KafkaConfigValues.isPresent(new Password(" "))).isFalse();
        assertThat(KafkaConfigValues.contains(null, "azure.configured")).isFalse();
        assertThat(KafkaConfigValues.contains("plain azure.configured text", "azure.configured")).isTrue();
    }
}
