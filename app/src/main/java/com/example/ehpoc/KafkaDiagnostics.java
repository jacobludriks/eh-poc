package com.example.ehpoc;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Pattern;

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

    /** Spring property names (relaxed, dotted form) that would mean a credential is configured in the application. */
    private static final Pattern CREDENTIAL_PROPERTY = Pattern.compile(
            "^spring\\.cloud\\.azure\\..*(credential|client\\.?secret|client\\.?certificate|managed\\.?identity|connection\\.?string|password|username|token|sas|key).*"
                    + "|^spring\\.kafka\\..*(sasl|security|jaas|password|secret|ssl|credential).*");

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
        String securityProtocol = asString(config.get("security.protocol"));
        String saslMechanism = asString(config.get("sasl.mechanism"));
        String handlerClass = asString(config.get("sasl.login.callback.handler.class"));
        String jaas = asString(config.get("sasl.jaas.config"));
        boolean jaasPresent = jaas != null && !jaas.isBlank();
        boolean jaasMarkedByAzure = jaasPresent && jaas.contains(AZURE_JAAS_MARKER);
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

    private static String asString(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Class<?> type) {
            return type.getName();
        }
        // A password-typed Kafka config (the JAAS configuration) renders as a masked value and is only used for
        // presence and marker checks, never returned.
        return value instanceof org.apache.kafka.common.config.types.Password password ? password.value() : value.toString();
    }

    /** Names (never values) of configured properties and variables that would supply a credential. */
    private Set<String> credentialPropertyNames() {
        Set<String> names = new TreeSet<>();
        for (PropertySource<?> source : environment.getPropertySources()) {
            if (source instanceof EnumerablePropertySource<?> enumerable) {
                for (String name : enumerable.getPropertyNames()) {
                    String relaxed = name.toLowerCase(Locale.ROOT).replace('_', '.').replace('-', '.');
                    if (CREDENTIAL_ENV_VARS.contains(name) || CREDENTIAL_PROPERTY.matcher(relaxed).matches()) {
                        names.add(name);
                    }
                }
            }
        }
        return names;
    }
}
