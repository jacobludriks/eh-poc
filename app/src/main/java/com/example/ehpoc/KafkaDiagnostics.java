package com.example.ehpoc;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.EnumerablePropertySource;
import org.springframework.core.env.PropertySource;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.stereotype.Component;

/**
 * Reports, from the REAL effective Kafka client configuration, whether Spring Cloud Azure's passwordless
 * (OAUTHBEARER) auto-configuration took effect. Nothing here is hard-coded as "healthy": the facts are read from
 * the {@link ProducerFactory}'s configuration (what the KafkaProducer is actually built from, after Spring Cloud
 * Azure's post-processor has run), and any silent disablement is returned as a failure.
 *
 * <p>Never exposes values that could be secrets: the JAAS configuration is reduced to a boolean, credential
 * properties are listed by NAME only, and no token is ever read.
 */
@Component
public class KafkaDiagnostics {

    /** The OAUTHBEARER login callback handler shipped by Spring Cloud Azure. */
    static final String AZURE_HANDLER_CLASS =
            "com.azure.spring.cloud.service.implementation.kafka.KafkaOAuth2AuthenticateCallbackHandler";

    /** Marker option Spring Cloud Azure adds to the JAAS configuration when it configured the client. */
    private static final String AZURE_JAAS_MARKER = "azure.configured";

    /** Prefixes (as name segments) of the Spring property families that can configure how the app authenticates. */
    private static final List<String> AZURE_PREFIX = List.of("spring", "cloud", "azure");
    private static final List<String> KAFKA_PREFIX = List.of("spring", "kafka");

    /**
     * Name segment sequences that mean a credential is configured under {@code spring.cloud.azure.*}: client secret
     * or certificate, user name or password, managed identity (which forces IMDS and bypasses the federated token),
     * connection string, SAS and shared access keys/signatures, account and access keys, token credential beans,
     * and any SASL/JAAS override.
     * A multi-word sequence also matches its run-together form (relaxed binding accepts {@code clientsecret}).
     */
    private static final List<List<String>> AZURE_CREDENTIAL_SEGMENTS = List.of(
            List.of("client", "secret"), List.of("client", "certificate"), List.of("secret"), List.of("certificate"),
            List.of("password"), List.of("username"), List.of("managed", "identity"), List.of("connection", "string"),
            List.of("sas"), List.of("shared", "access"), List.of("access", "key"), List.of("account", "key"),
            List.of("token", "credential"), List.of("sasl"), List.of("jaas"));

    /**
     * Name segments that mean the application itself supplies authentication under {@code spring.kafka.*}: any SASL
     * or JAAS setting (the passwordless one is added by Spring Cloud Azure, never set here), or a password, secret
     * or credential. TLS settings such as {@code ssl.keystore.type} are not credentials.
     */
    private static final List<List<String>> KAFKA_CREDENTIAL_SEGMENTS = List.of(
            List.of("sasl"), List.of("jaas"), List.of("password"), List.of("secret"), List.of("credential"));

    /** Environment variables (exact names) that would bypass the federated workload identity token. */
    private static final Set<String> CREDENTIAL_ENV_VARS = Set.of(
            "AZURE_CLIENT_SECRET", "AZURE_CLIENT_CERTIFICATE_PATH", "AZURE_CLIENT_CERTIFICATE_PASSWORD",
            "AZURE_USERNAME", "AZURE_PASSWORD");

    private final ProducerFactory<String, String> producerFactory;
    private final ConfigurableEnvironment environment;

    public KafkaDiagnostics(ProducerFactory<String, String> producerFactory, ConfigurableEnvironment environment) {
        this.producerFactory = producerFactory;
        this.environment = environment;
    }

    public Map<String, Object> report() {
        Map<String, Object> config = producerFactory.getConfigurationProperties();

        List<String> bootstrapServers = bootstrapServers(config.get("bootstrap.servers"));
        String securityProtocol = KafkaConfigValues.asString(config.get("security.protocol"));
        String saslMechanism = KafkaConfigValues.asString(config.get("sasl.mechanism"));
        String handlerClass = KafkaConfigValues.asString(config.get("sasl.login.callback.handler.class"));
        // The JAAS configuration is a Kafka Password: only its presence and Azure's marker are read, never returned.
        Object jaas = config.get("sasl.jaas.config");
        boolean jaasPresent = KafkaConfigValues.isPresent(jaas);
        boolean jaasMarkedByAzure = KafkaConfigValues.contains(jaas, AZURE_JAAS_MARKER);
        Set<String> credentialProperties = credentialPropertyNames();

        List<String> problems = new ArrayList<>();
        if (bootstrapServers.size() != 1) {
            problems.add("bootstrap.servers must be exactly one server (found " + bootstrapServers.size() + ")");
        }
        else if (!bootstrapServers.get(0).endsWith(":" + DiagnosticsController.KAFKA_PORT)
                || bootstrapServers.get(0).startsWith(":")) {
            problems.add("bootstrap server must be <namespace>.servicebus.windows.net:" + DiagnosticsController.KAFKA_PORT
                    + " (found " + bootstrapServers.get(0) + ")");
        }
        if (!"SASL_SSL".equals(securityProtocol)) {
            problems.add("security.protocol is " + securityProtocol + ", expected SASL_SSL");
        }
        if (!"OAUTHBEARER".equals(saslMechanism)) {
            problems.add("sasl.mechanism is " + saslMechanism + ", expected OAUTHBEARER");
        }
        if (!AZURE_HANDLER_CLASS.equals(handlerClass)) {
            problems.add("sasl.login.callback.handler.class is " + handlerClass + ", expected Azure's KafkaOAuth2AuthenticateCallbackHandler");
        }
        if (!jaasMarkedByAzure) {
            problems.add("sasl.jaas.config was not set by Spring Cloud Azure (auto-configuration skipped the Kafka properties)");
        }
        // The client configuration is the passwordless one only if every check above passed.
        boolean applied = problems.isEmpty();
        if (!credentialProperties.isEmpty()) {
            problems.add("credential properties are configured: " + String.join(", ", credentialProperties));
        }

        Map<String, Object> kafka = new LinkedHashMap<>();
        kafka.put("bootstrapServers", bootstrapServers);
        kafka.put("securityProtocol", securityProtocol);
        kafka.put("saslMechanism", saslMechanism);
        kafka.put("loginCallbackHandlerClass", handlerClass);
        kafka.put("jaasConfigPresent", jaasPresent);
        kafka.put("jaasConfiguredBySpringCloudAzure", jaasMarkedByAzure);
        // True only when the whole effective client configuration is the passwordless one; see "problems" otherwise.
        kafka.put("passwordlessAutoConfigApplied", applied);
        kafka.put("credentialPropertiesConfigured", !credentialProperties.isEmpty());
        kafka.put("credentialPropertyNames", credentialProperties);
        kafka.put("problems", problems);
        return kafka;
    }

    private static List<String> bootstrapServers(Object value) {
        if (value == null) {
            return List.of();
        }
        if (value instanceof Collection<?> servers) {
            return servers.stream().map(String::valueOf).map(String::trim).filter(s -> !s.isEmpty()).toList();
        }
        return Arrays.stream(String.valueOf(value).split(",")).map(String::trim).filter(s -> !s.isEmpty()).toList();
    }

    /**
     * Whether a property or environment variable NAME supplies a credential. Names are compared by whole segments
     * (split on dots, dashes, underscores, brackets and camelCase humps), so {@code client-secret},
     * {@code clientSecret} and {@code SPRING_CLOUD_AZURE_CREDENTIAL_CLIENT_SECRET} all match, while
     * {@code ssl.keystore.type} or {@code kafka.keys} do not match merely because they contain "key", "sas" or "ssl".
     */
    static boolean isCredentialProperty(String name) {
        if (CREDENTIAL_ENV_VARS.contains(name)) {
            return true;
        }
        List<String> segments = Arrays.stream(name.replaceAll("([a-z0-9])([A-Z])", "$1.$2")
                .toLowerCase(Locale.ROOT).split("[^a-z0-9]+")).filter(s -> !s.isEmpty()).toList();
        if (startsWith(segments, AZURE_PREFIX)) {
            return containsAny(segments.subList(AZURE_PREFIX.size(), segments.size()), AZURE_CREDENTIAL_SEGMENTS);
        }
        if (startsWith(segments, KAFKA_PREFIX)) {
            return containsAny(segments.subList(KAFKA_PREFIX.size(), segments.size()), KAFKA_CREDENTIAL_SEGMENTS);
        }
        return false;
    }

    private static boolean startsWith(List<String> segments, List<String> prefix) {
        return segments.size() >= prefix.size() && segments.subList(0, prefix.size()).equals(prefix);
    }

    private static boolean containsAny(List<String> segments, List<List<String>> sequences) {
        return sequences.stream().anyMatch(sequence -> Collections.indexOfSubList(segments, sequence) >= 0
                || (sequence.size() > 1 && segments.contains(String.join("", sequence))));
    }

    /** Names (never values) of configured properties and variables that would supply a credential. */
    private Set<String> credentialPropertyNames() {
        Set<String> names = new TreeSet<>();
        for (PropertySource<?> source : environment.getPropertySources()) {
            if (source instanceof EnumerablePropertySource<?> enumerable) {
                for (String name : enumerable.getPropertyNames()) {
                    if (isCredentialProperty(name)) {
                        names.add(name);
                    }
                }
            }
        }
        return names;
    }
}
