package com.example.ehpoc;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Pure unit test of which configured property names count as "a credential is configured" (names only; values are
 * never read). Both sides matter: a missed credential hides a failed PoC, a false positive fails a healthy deploy.
 */
class CredentialPropertyMatcherTest {

    @ParameterizedTest
    @ValueSource(strings = {
            "spring.cloud.azure.credential.client-secret",
            "spring.cloud.azure.credential.client-certificate-path",
            "spring.cloud.azure.credential.client-certificate-password",
            "spring.cloud.azure.credential.password",
            "spring.cloud.azure.credential.username",
            "spring.cloud.azure.credential.managed-identity-enabled",
            "spring.cloud.azure.eventhubs.connection-string",
            "spring.cloud.azure.eventhubs.sas-token",
            "spring.cloud.azure.eventhubs.shared-access-key",
            "spring.cloud.azure.eventhubs.shared-access-signature",
            "spring.cloud.azure.eventhubs.kafka.properties.sasl.jaas.config",
            "spring.kafka.properties.sasl.jaas.config",
            "spring.kafka.properties[sasl.jaas.config]",
            "spring.kafka.properties.sasl.mechanism",
            "spring.kafka.ssl.key-store-password",
            "spring.kafka.ssl.key-password",
            // The same names as environment variables.
            "SPRING_CLOUD_AZURE_CREDENTIAL_CLIENTSECRET",
            "SPRING_CLOUD_AZURE_CREDENTIAL_CLIENT_SECRET",
            "SPRING_KAFKA_PROPERTIES_SASL_JAAS_CONFIG",
            "AZURE_CLIENT_SECRET",
            "AZURE_CLIENT_CERTIFICATE_PATH",
            "AZURE_PASSWORD" })
    void flagsPropertiesThatSupplyACredential(String name) {
        assertThat(KafkaDiagnostics.isCredentialProperty(name)).as(name).isTrue();
    }

    @ParameterizedTest
    @ValueSource(strings = {
            // TLS settings and other names that merely contain "key", "sas" or "ssl" as a substring.
            "spring.kafka.ssl.protocol",
            "spring.kafka.ssl.key-store-type",
            "spring.kafka.properties.ssl.keystore.type",
            "spring.kafka.properties.ssl.endpoint.identification.algorithm",
            "spring.kafka.properties.security.protocol",
            "spring.kafka.consumer.key-deserializer",
            "spring.kafka.producer.key-serializer",
            "spring.cloud.azure.eventhubs.kafka.keys",
            "spring.cloud.azure.eventhubs.kafka.keystore-enabled",
            "spring.cloud.azure.eventhubs.sassy",
            "spring.cloud.azure.profile.tenant-id",
            "spring.cloud.azure.credential.client-id",
            "spring.kafka.bootstrap-servers",
            "spring.kafka.consumer.group-id",
            "spring.kafka.listener.auth-exception-retry-interval",
            // The webhook-injected workload identity variables are the intended mechanism, not a secret.
            "AZURE_CLIENT_ID",
            "AZURE_TENANT_ID",
            "AZURE_FEDERATED_TOKEN_FILE",
            "AZURE_AUTHORITY_HOST",
            "AZURE_TOKEN_CREDENTIALS",
            // Not Spring Cloud Azure or Spring Kafka properties.
            "ehpoc.eventhubs.topic",
            "java.class.path",
            "PASSWORD_STORE_DIR",
            "SSH_AUTH_SOCK" })
    void ignoresBenignPropertiesThatOnlyLookSimilar(String name) {
        assertThat(KafkaDiagnostics.isCredentialProperty(name)).as(name).isFalse();
    }
}
