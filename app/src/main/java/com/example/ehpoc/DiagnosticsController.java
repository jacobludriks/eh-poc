package com.example.ehpoc;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

import org.springframework.core.env.Environment;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Diagnostics endpoint. Reports, from inside the running pod, the facts that prove the private,
 * passwordless wiring: where the Event Hubs host resolves, whether that address is private, and whether the
 * workload identity environment was injected, and (the "kafka" section) the effective Kafka client security
 * configuration and whether Spring Cloud Azure's passwordless auto-configuration took effect.
 *
 * <p>Never reports token contents or any secret: workload identity variables are reported as booleans only.
 */
@RestController
public class DiagnosticsController {

    /** Event Hubs Kafka endpoint port. */
    static final int KAFKA_PORT = 9093;

    private final Environment env;
    private final KafkaDiagnostics kafkaDiagnostics;

    public DiagnosticsController(Environment env, KafkaDiagnostics kafkaDiagnostics) {
        this.env = env;
        this.kafkaDiagnostics = kafkaDiagnostics;
    }

    @GetMapping("/api/diagnostics")
    public Map<String, Object> diagnostics() {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("app", "ehpoc");
        body.put("javaVersion", System.getProperty("java.version"));
        body.put("hostname", env.getProperty("HOSTNAME", "unknown"));
        body.put("time", Instant.now().toString());
        body.put("eventHubs", eventHubs());
        body.put("workloadIdentity", workloadIdentity());
        body.put("kafka", kafkaDiagnostics.report());
        return body;
    }

    private Map<String, Object> eventHubs() {
        String fqdn = blankToNull(env.getProperty("EVENTHUBS_NAMESPACE_FQDN"));
        String topic = blankToNull(env.getProperty("EVENTHUBS_TOPIC"));

        Map<String, Object> eh = new LinkedHashMap<>();
        eh.put("configured", fqdn != null);
        eh.put("topic", topic);
        eh.put("bootstrapServer", fqdn == null ? null : fqdn + ":" + KAFKA_PORT);
        eh.put("bootstrapHost", fqdn);
        eh.put("resolvedIp", null);
        eh.put("resolvedIpIsPrivate", null);
        eh.put("resolutionError", null);

        if (fqdn == null) {
            eh.put("resolutionError", "EVENTHUBS_NAMESPACE_FQDN is not set");
            return eh;
        }
        try {
            InetAddress address = InetAddress.getByName(fqdn);
            eh.put("resolvedIp", address.getHostAddress());
            eh.put("resolvedIpIsPrivate", isPrivate(address));
        }
        catch (UnknownHostException e) {
            // Off-cluster the namespace has no public address; report it instead of failing the endpoint.
            eh.put("resolutionError", "cannot resolve " + fqdn);
        }
        return eh;
    }

    private Map<String, Object> workloadIdentity() {
        Map<String, Object> wi = new LinkedHashMap<>();
        wi.put("AZURE_CLIENT_ID", isSet("AZURE_CLIENT_ID"));
        wi.put("AZURE_TENANT_ID", isSet("AZURE_TENANT_ID"));
        wi.put("AZURE_FEDERATED_TOKEN_FILE", isSet("AZURE_FEDERATED_TOKEN_FILE"));
        // Plain configuration value (a credential type name), not a secret.
        wi.put("AZURE_TOKEN_CREDENTIALS", blankToNull(env.getProperty("AZURE_TOKEN_CREDENTIALS")));
        return wi;
    }

    private boolean isSet(String name) {
        return blankToNull(env.getProperty(name)) != null;
    }

    private static String blankToNull(String value) {
        return value == null || value.isBlank() ? null : value;
    }

    /** RFC 1918 for IPv4 (10/8, 172.16/12, 192.168/16); unique local fc00::/7 for IPv6. */
    static boolean isPrivate(InetAddress address) {
        if (address instanceof Inet4Address) {
            return address.isSiteLocalAddress();
        }
        return (address.getAddress()[0] & 0xfe) == 0xfc;
    }
}
