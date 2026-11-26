package com.example.ehpoc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.IntStream;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/**
 * LOCAL-ONLY verification of the consumer, buffer, SSE and clear behaviour, at the same seam as the deployed
 * smoke test (the public HTTP surface), against an in-process PLAINTEXT broker (EmbeddedKafka). It cannot prove
 * anything about Azure: no workload identity, SASL, private endpoint or Event Hubs semantics are involved.
 * The broker address replaces the bootstrap server for this test only; the real configuration is untouched.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "ehpoc.eventhubs.topic=events",
        "ehpoc.sse.heartbeat-interval=1s" })
@EmbeddedKafka(partitions = 2, topics = "events")
class ConsumerRoundTripLocalTest {

    @Value("${local.server.port}")
    int port;

    @Autowired
    JsonMapper json;

    @Autowired
    KafkaTemplate<String, String> kafka;

    @Autowired
    KafkaListenerEndpointRegistry registry;

    @Value("${spring.embedded.kafka.brokers}")
    String brokers;

    private final HttpClient http = HttpClient.newHttpClient();

    @BeforeEach
    void consumerReadyAndViewEmpty() throws Exception {
        // The consumer starts at "latest": anything sent before partitions are assigned would be skipped.
        await().atMost(Duration.ofSeconds(60)).untilAsserted(() ->
                assertThat(get("/api/diagnostics").at("/consumer/assignedPartitions").asInt()).isEqualTo(2));
        post("/api/events/clear", "");
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                assertThat(get("/api/events").get("count").asInt()).isZero());
    }

    @Test
    void sentEventComesBackThroughTheConsumerWithSameIdMessageAndKey() throws Exception {
        JsonNode sent = json.readTree(post("/api/events", "{\"message\":\"hello round trip\",\"source\":\"api\"}").body());
        String id = sent.at("/event/id").asString();

        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
            JsonNode view = get("/api/events");
            assertThat(view.get("capacity").asInt()).isEqualTo(200);
            assertThat(view.get("events")).hasSize(1);
            JsonNode e = view.get("events").get(0);
            assertThat(e.get("id").asString()).isEqualTo(id);
            assertThat(e.get("key").asString()).isEqualTo(id);
            assertThat(e.get("message").asString()).isEqualTo("hello round trip");
            assertThat(e.get("source").asString()).isEqualTo("api");
            assertThat(e.get("sentAt").asString()).isEqualTo(sent.at("/event/sentAt").asString());
            assertThat(e.get("partition").asInt()).isEqualTo(sent.get("partition").asInt());
            assertThat(e.get("offset").asLong()).isEqualTo(sent.get("offset").asLong());
            assertThat(e.get("consumedAt").asString()).isNotBlank();
        });
        JsonNode consumer = get("/api/diagnostics").get("consumer");
        assertThat(consumer.get("groupId").asString()).isEqualTo("dashboard");
        assertThat(consumer.get("topic").asString()).isEqualTo("events");
        assertThat(consumer.get("listenerRunning").asBoolean()).isTrue();
        assertThat(consumer.get("bufferedCount").asInt()).isEqualTo(1);
        assertThat(consumer.get("bufferCapacity").asInt()).isEqualTo(200);
        assertThat(consumer.get("lastConsumedAt").asString()).isNotBlank();
    }

    @Test
    void listIsNewestFirstAndNeverExceedsTwoHundredDroppingTheOldestFirst() throws Exception {
        for (int i = 1; i <= 205; i++) {
            post("/api/events", "{\"message\":\"m" + i + "\"}");
        }
        await().atMost(Duration.ofSeconds(60)).untilAsserted(() -> {
            JsonNode events = get("/api/events").get("events");
            assertThat(events).hasSize(200);
            // Same-partition ordering is guaranteed, cross-partition is not, so check membership and the bound.
            List<String> messages = IntStream.range(0, events.size()).mapToObj(i -> events.get(i).get("message").asString()).toList();
            assertThat(messages).doesNotHaveDuplicates();
        });
        JsonNode view = get("/api/events");
        assertThat(view.get("count").asInt()).isEqualTo(200);
        List<String> messages = IntStream.range(0, 200).mapToObj(i -> view.get("events").get(i).get("message").asString()).toList();
        // The newest events are kept and the first sent are dropped.
        assertThat(messages).contains("m205", "m204", "m6").doesNotContain("m1", "m2", "m3", "m4", "m5");
    }

    @Test
    void sseStreamDeliversNewEventWithoutPollingAndSendsHeartbeatsAndClearedNotification() throws Exception {
        HttpResponse<java.io.InputStream> response = http.send(
                HttpRequest.newBuilder(uri("/api/events/stream")).GET().build(), HttpResponse.BodyHandlers.ofInputStream());
        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(response.headers().firstValue("Content-Type").orElse("")).startsWith("text/event-stream");

        List<String> lines = new CopyOnWriteArrayList<>();
        Thread reader = Thread.ofVirtual().start(() -> {
            try (BufferedReader r = new BufferedReader(new InputStreamReader(response.body(), StandardCharsets.UTF_8))) {
                String line;
                while ((line = r.readLine()) != null) {
                    lines.add(line);
                }
            }
            catch (Exception ignored) {
                // stream closed by the test
            }
        });
        try {
            // Heartbeats are SSE comment lines so idle intermediaries do not drop the connection.
            await().atMost(Duration.ofSeconds(10)).until(() -> lines.stream().filter(l -> l.startsWith(":")).count() >= 2);

            String id = json.readTree(post("/api/events", "{\"message\":\"streamed\"}").body()).at("/event/id").asString();
            await().atMost(Duration.ofSeconds(20)).until(() -> lines.contains("event:event"));
            String data = lines.get(lines.indexOf("event:event") + 1);
            assertThat(data).startsWith("data:");
            JsonNode e = json.readTree(data.substring("data:".length()));
            assertThat(e.get("id").asString()).isEqualTo(id);
            assertThat(e.get("message").asString()).isEqualTo("streamed");

            post("/api/events/clear", "");
            await().atMost(Duration.ofSeconds(10)).until(() -> lines.contains("event:cleared"));
            JsonNode cleared = json.readTree(lines.get(lines.indexOf("event:cleared") + 1).substring("data:".length()));
            assertThat(cleared.get("removed").asInt()).isEqualTo(1);
        }
        finally {
            response.body().close();
            reader.interrupt();
        }
    }

    @Test
    void clearEmptiesTheViewOnlyAndSaysTheEventHubIsNotPurged() throws Exception {
        post("/api/events", "{\"message\":\"to be cleared\"}");
        await().atMost(Duration.ofSeconds(20)).untilAsserted(() ->
                assertThat(get("/api/events").get("count").asInt()).isEqualTo(1));

        HttpResponse<String> cleared = post("/api/events/clear", "");
        assertThat(cleared.statusCode()).isEqualTo(200);
        JsonNode body = json.readTree(cleared.body());
        assertThat(body.get("cleared").asInt()).isEqualTo(1);
        assertThat(body.get("scope").asString()).isEqualTo("in-memory view");
        assertThat(body.get("eventHubPurged").asBoolean()).isFalse();
        assertThat(body.get("message").asString()).contains("not purged").contains("retention");

        JsonNode view = get("/api/events");
        assertThat(view.get("count").asInt()).isZero();
        assertThat(view.get("events")).isEmpty();
    }

    @Test
    void malformedMessagesAreSkippedAndTheConsumerKeepsGoing() throws Exception {
        kafka.send("events", "bad-1", "this is not json").get();
        kafka.send("events", "bad-2", "{\"id\":").get();
        kafka.send("events", "bad-3", "{\"unexpected\":true}").get();
        kafka.send("events", "bad-4", "null").get();
        String id = json.readTree(post("/api/events", "{\"message\":\"after bad ones\"}").body()).at("/event/id").asString();

        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
            JsonNode events = get("/api/events").get("events");
            assertThat(events).hasSize(1);
            assertThat(events.get(0).get("id").asString()).isEqualTo(id);
        });
        assertThat(get("/api/diagnostics").at("/consumer/listenerRunning").asBoolean()).isTrue();
    }

    @Test
    void offsetsAreCommittedSoARestartResumesInsteadOfReplaying() throws Exception {
        for (int i = 0; i < 5; i++) {
            post("/api/events", "{\"message\":\"commit me " + i + "\"}");
        }
        await().atMost(Duration.ofSeconds(20)).untilAsserted(() ->
                assertThat(get("/api/events").get("count").asInt()).isEqualTo(5));

        try (Admin admin = Admin.create(Map.of("bootstrap.servers", brokers))) {
            await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
                Map<TopicPartition, OffsetAndMetadata> committed =
                        admin.listConsumerGroupOffsets("dashboard").partitionsToOffsetAndMetadata().get();
                long total = committed.values().stream().mapToLong(OffsetAndMetadata::offset).sum();
                assertThat(total).isGreaterThanOrEqualTo(5);
            });
        }
    }

    @Test
    void afterAConsumerRestartPreviouslyConsumedEventsAreNotReplayedButMissedOnesAreDelivered() throws Exception {
        for (int i = 0; i < 5; i++) {
            post("/api/events", "{\"message\":\"before restart " + i + "\"}");
        }
        await().atMost(Duration.ofSeconds(20)).untilAsserted(() ->
                assertThat(get("/api/events").get("count").asInt()).isEqualTo(5));

        // Stand-in for a pod restart: a new consumer instance in the same group (the in-memory buffer is emptied
        // as a new pod's would be). Stopping the container commits the pending offsets.
        var container = registry.getListenerContainer(EventConsumer.LISTENER_ID);
        container.stop();
        post("/api/events/clear", "");
        String missed = json.readTree(post("/api/events", "{\"message\":\"sent while down\"}").body()).at("/event/id").asString();
        container.start();

        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            JsonNode events = get("/api/events").get("events");
            assertThat(events).hasSize(1);
            assertThat(events.get(0).get("id").asString()).isEqualTo(missed);
        });
        Thread.sleep(3000);
        assertThat(get("/api/events").get("count").asInt()).isEqualTo(1);
    }

    private URI uri(String path) {
        return URI.create("http://localhost:" + port + path);
    }

    @Test
    void dashboardPageIsServedAtRootAsOneSelfContainedPageUsingTheDocumentedApi() throws Exception {
        HttpResponse<String> page = http.send(HttpRequest.newBuilder(uri("/")).GET().build(), HttpResponse.BodyHandlers.ofString());

        assertThat(page.statusCode()).isEqualTo(200);
        assertThat(page.headers().firstValue("Content-Type").orElse("")).startsWith("text/html");
        String html = page.body();
        // Works offline from the app: nothing is loaded from another origin.
        assertThat(html).doesNotContainPattern("(?i)(src|href)\\s*=\\s*[\"']?(https?:)?//");
        // Untrusted event data must never reach innerHTML.
        assertThat(html).doesNotContain("innerHTML").doesNotContain("insertAdjacentHTML").doesNotContain("document.write");
        // It talks to exactly the documented endpoints.
        assertThat(html).contains("/api/events", "/api/events/stream", "/api/events/clear", "/api/diagnostics");
    }

    private JsonNode get(String path) throws Exception {
        HttpResponse<String> response = http.send(HttpRequest.newBuilder(uri(path)).GET().build(), HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).isEqualTo(200);
        return json.readTree(response.body());
    }

    private HttpResponse<String> post(String path, String body) throws Exception {
        return http.send(HttpRequest.newBuilder(uri(path)).header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body)).build(), HttpResponse.BodyHandlers.ofString());
    }
}
