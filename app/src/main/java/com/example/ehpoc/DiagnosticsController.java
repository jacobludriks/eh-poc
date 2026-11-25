package com.example.ehpoc;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Diagnostics endpoint. In the walking skeleton this is a stub; later tickets add the bootstrap host
 * resolution (02) and the Kafka/SASL/auto-configuration facts (03).
 */
@RestController
public class DiagnosticsController {

    @GetMapping("/api/diagnostics")
    public Map<String, Object> diagnostics() {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("app", "ehpoc");
        body.put("stub", true);
        body.put("javaVersion", System.getProperty("java.version"));
        body.put("hostname", System.getenv().getOrDefault("HOSTNAME", "unknown"));
        body.put("time", Instant.now().toString());
        return body;
    }
}
