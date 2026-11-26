// Fake of the deployed app's HTTP surface, for exercising scripts/smoke-test.ps1 locally (no Azure, no Java).
// It serves the API contract documented on ticket 04 ("API contract"): readiness, /api/diagnostics,
// POST /api/events, GET /api/events, GET /api/events/stream (SSE) and POST /api/events/clear.
// Behaviour is canned and switchable through Opts so a test can break exactly one thing at a time.
// Test support only; never deployed.
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;

public sealed class FakeEventsApp : IDisposable
{
    readonly HttpListener listener = new HttpListener();
    readonly object gate = new object();
    readonly List<JsonObject> buffer = new List<JsonObject>();   // newest first
    readonly List<HttpListenerResponse> sseClients = new List<HttpListenerResponse>();
    int diagnosticsCalls, postCalls, offset, partitionCounter;

    public string BaseUrl { get; private set; }

    // Canned diagnostics body (JSON text) and the body served during the first DiagnosticsWarmupCalls requests.
    public string DiagnosticsJson { get; set; }
    public string WarmupDiagnosticsJson { get; set; }
    public int DiagnosticsWarmupCalls { get; set; }

    // Switches. Defaults describe a healthy app.
    public int ReadinessDownCalls { get; set; }          // first N readiness probes answer 503
    public int PostFailFirst { get; set; }               // first N POSTs answer 503 publish_failed (role propagation)
    public int PostStatus { get; set; } = 201;           // status once past PostFailFirst (non-201 = error body)
    public bool PostOmitOffset { get; set; }             // 201 without partition/offset
    public bool ListDropsEvent { get; set; }             // consumer never delivers: event absent from GET /api/events
    public string ListMutates { get; set; } = "";        // "message" | "key" | "offset": list shows a different value
    public bool SseDropsEvent { get; set; }              // stream never pushes the event
    public bool SseTampersMessage { get; set; }          // stream pushes the event with a different message
    public bool ClearIsNoop { get; set; }                // clear leaves the buffer untouched
    public bool ClearPurgesHub { get; set; }             // clear claims eventHubPurged=true

    // Workload-identity simulation (negative test, ticket 06). The "pod" obtains its token when it starts, so the
    // federated credential only matters at a restart (POST /__fake/restart, called by the fake kubectl): the app
    // reads FederationFile (the canned `az identity federated-credential list` data) and, unless IgnoreFederation,
    // is "auth broken" afterwards if no credential has the subject ValidSubject. While broken, POST /api/events
    // answers 503 with BrokenReason and diagnostics show no assigned partitions plus BrokenLastErrorClass.
    // FederationLagRestarts models Entra propagation delay: that many restarts still see the previous state.
    public string FederationFile { get; set; }
    public string ValidSubject { get; set; } = "system:serviceaccount:poc:ehpoc";
    public bool IgnoreFederation { get; set; }           // credential changes never take effect (control case)
    public int FederationLagRestarts { get; set; }
    public string BrokenReason { get; set; } = "authentication";
    public string BrokenLastErrorClass { get; set; } = "org.apache.kafka.common.KafkaException <- javax.security.auth.login.LoginException";
    public bool AuthBroken { get { lock (gate) return authBroken; } }
    bool authBroken;

    public List<string> RequestLog { get; } = new List<string>();

    public FakeEventsApp(int port)
    {
        BaseUrl = "http://127.0.0.1:" + port;
        listener.Prefixes.Add(BaseUrl + "/");
    }

    public void Start()
    {
        listener.Start();
        Task.Run(async () =>
        {
            while (listener.IsListening)
            {
                HttpListenerContext ctx;
                try { ctx = await listener.GetContextAsync(); } catch { break; }
                _ = Task.Run(() => Handle(ctx));
            }
        });
    }

    /// <summary>Back to a healthy app with an empty buffer (called between scenarios).</summary>
    public void Reset()
    {
        lock (gate)
        {
            buffer.Clear();
            foreach (var c in sseClients) { try { c.Close(); } catch { } }
            sseClients.Clear();
            diagnosticsCalls = postCalls = offset = partitionCounter = readinessCalls = 0;
            ReadinessDownCalls = PostFailFirst = DiagnosticsWarmupCalls = 0;
            PostStatus = 201;
            PostOmitOffset = ListDropsEvent = SseDropsEvent = SseTampersMessage = ClearIsNoop = ClearPurgesHub = false;
            ListMutates = "";
            FederationFile = null; ValidSubject = "system:serviceaccount:poc:ehpoc"; IgnoreFederation = false; FederationLagRestarts = 0;
            BrokenReason = "authentication";
            BrokenLastErrorClass = "org.apache.kafka.common.KafkaException <- javax.security.auth.login.LoginException";
            authBroken = false;
            DiagnosticsJson = WarmupDiagnosticsJson = null;
            RequestLog.Clear();
        }
    }

    int readinessCalls;

    void Handle(HttpListenerContext ctx)
    {
        var req = ctx.Request;
        var res = ctx.Response;
        try
        {
            var path = req.Url.AbsolutePath.TrimEnd('/');
            lock (gate) { RequestLog.Add(req.HttpMethod + " " + path); }

            if (req.HttpMethod == "GET" && path == "/actuator/health/readiness")
            {
                bool down;
                lock (gate) { down = readinessCalls++ < ReadinessDownCalls; }
                Json(res, down ? 503 : 200, down ? "{\"status\":\"DOWN\"}" : "{\"status\":\"UP\"}");
            }
            else if (req.HttpMethod == "GET" && path == "/api/diagnostics")
            {
                string body;
                lock (gate)
                {
                    body = diagnosticsCalls++ < DiagnosticsWarmupCalls ? WarmupDiagnosticsJson : DiagnosticsJson;
                    if (authBroken) body = BrokenDiagnostics(body);
                }
                Json(res, 200, body);
            }
            else if (req.HttpMethod == "POST" && path == "/__fake/restart")
            {
                // Test-only control endpoint: the fake kubectl calls it for `rollout restart`.
                lock (gate) { Restart(); }
                Json(res, 200, "{\"restarted\":true}");
            }
            else if (req.HttpMethod == "POST" && path == "/api/events")
            {
                Post(req, res);
            }
            else if (req.HttpMethod == "GET" && path == "/api/events")
            {
                string body;
                lock (gate)
                {
                    var arr = new JsonArray();
                    foreach (var e in buffer) arr.Add(Mutate(e));
                    var o = new JsonObject { ["count"] = buffer.Count, ["capacity"] = 200, ["events"] = arr };
                    body = o.ToJsonString();
                }
                res.AddHeader("Cache-Control", "no-store");
                Json(res, 200, body);
            }
            else if (req.HttpMethod == "GET" && path == "/api/events/stream")
            {
                res.StatusCode = 200;
                res.ContentType = "text/event-stream";
                res.AddHeader("Cache-Control", "no-cache");
                res.SendChunked = true;
                Write(res, ":connected\n\n");
                lock (gate) { sseClients.Add(res); }
                return; // stays open; closed by Reset/Dispose or when the client goes away
            }
            else if (req.HttpMethod == "POST" && path == "/api/events/clear")
            {
                string body;
                lock (gate)
                {
                    int removed = buffer.Count;
                    if (!ClearIsNoop) buffer.Clear();
                    var o = new JsonObject
                    {
                        ["cleared"] = removed,
                        ["scope"] = "in-memory view",
                        ["eventHubPurged"] = ClearPurgesHub,
                        ["message"] = "Cleared the in-memory view only."
                    };
                    body = o.ToJsonString();
                    if (!ClearIsNoop) Broadcast("cleared", "{\"removed\":" + removed + ",\"clearedAt\":\"" + DateTime.UtcNow.ToString("o") + "\"}");
                }
                Json(res, 200, body);
            }
            else
            {
                Json(res, 404, "{\"error\":\"not_found\"}");
            }
        }
        catch
        {
            try { res.StatusCode = 500; res.Close(); } catch { }
        }
    }

    void Post(HttpListenerRequest req, HttpListenerResponse res)
    {
        string raw;
        using (var r = new StreamReader(req.InputStream, Encoding.UTF8)) raw = r.ReadToEnd();
        JsonNode parsed = JsonNode.Parse(raw);
        string message = parsed?["message"]?.GetValue<string>();

        string failBody = null; int failStatus = 0; string responseBody = null;
        lock (gate)
        {
            postCalls++;
            if (authBroken)
            {
                failStatus = 503;
                failBody = "{\"error\":\"publish_failed\",\"message\":\"canned\",\"reason\":\"" + BrokenReason + "\"}";
            }
            else if (postCalls <= PostFailFirst)
            {
                failStatus = 503;
                failBody = "{\"error\":\"publish_failed\",\"message\":\"canned\",\"reason\":\"authorization\"}";
            }
            else if (PostStatus != 201)
            {
                failStatus = PostStatus;
                failBody = "{\"error\":\"canned_error\",\"message\":\"canned\"}";
            }
            else
            {
                var id = Guid.NewGuid().ToString();
                var partition = partitionCounter++ % 2;
                var off = offset++;
                var ev = new JsonObject
                {
                    ["id"] = id,
                    ["message"] = message,
                    ["sentAt"] = DateTime.UtcNow.ToString("o"),
                    ["source"] = parsed?["source"]?.GetValue<string>() ?? "api"
                };
                var consumed = new JsonObject
                {
                    ["id"] = id,
                    ["message"] = message,
                    ["sentAt"] = ev["sentAt"].GetValue<string>(),
                    ["source"] = ev["source"].GetValue<string>(),
                    ["key"] = id,
                    ["partition"] = partition,
                    ["offset"] = off,
                    ["consumedAt"] = DateTime.UtcNow.ToString("o")
                };
                var resp = new JsonObject { ["event"] = ev, ["topic"] = "events" };
                if (!PostOmitOffset) { resp["partition"] = partition; resp["offset"] = off; }
                responseBody = resp.ToJsonString();
                if (!ListDropsEvent) buffer.Insert(0, consumed);
                if (!SseDropsEvent)
                {
                    var pushed = (JsonObject)JsonNode.Parse(consumed.ToJsonString());
                    if (SseTampersMessage) pushed["message"] = "tampered";
                    Broadcast("event", pushed.ToJsonString());
                }
            }
        }
        if (failBody != null) Json(res, failStatus, failBody);
        else Json(res, 201, responseBody);
    }

    // Caller holds the lock. A pod restart: the in-memory view and the SSE connections are gone, and the new pod
    // acquires its token against whatever federated credential Entra honours now (subject to the lag).
    void Restart()
    {
        buffer.Clear();
        foreach (var c in sseClients) { try { c.Close(); } catch { } }
        sseClients.Clear();
        bool shouldBeBroken = false;
        if (!IgnoreFederation && FederationFile != null)
        {
            var creds = JsonNode.Parse(File.ReadAllText(FederationFile));
            var list = creds is JsonArray a ? a : new JsonArray(creds.DeepClone());
            shouldBeBroken = !list.Any(c => c?["subject"]?.GetValue<string>() == ValidSubject);
        }
        if (shouldBeBroken != authBroken)
        {
            if (FederationLagRestarts > 0) FederationLagRestarts--;
            else authBroken = shouldBeBroken;
        }
    }

    string BrokenDiagnostics(string body)
    {
        var d = JsonNode.Parse(body);
        d["consumer"]["assignedPartitions"] = 0;
        d["consumer"]["lastErrorClass"] = BrokenLastErrorClass;
        d["consumer"]["lastErrorAt"] = DateTime.UtcNow.ToString("o");
        return d.ToJsonString();
    }

    // Applies the ListMutates switch to a copy of a buffered event.
    JsonObject Mutate(JsonObject e)
    {
        var c = (JsonObject)JsonNode.Parse(e.ToJsonString());
        if (ListMutates == "message") c["message"] = "tampered";
        if (ListMutates == "key") c["key"] = "other-key";
        if (ListMutates == "offset") c["offset"] = (c["offset"].GetValue<int>()) + 1000;
        return c;
    }

    // Caller holds the lock. Spring writes no space after the colon, so neither do we.
    void Broadcast(string name, string data)
    {
        foreach (var c in sseClients.ToList())
        {
            try { Write(c, "event:" + name + "\ndata:" + data + "\n\n"); }
            catch { sseClients.Remove(c); }
        }
    }

    static void Write(HttpListenerResponse res, string text)
    {
        var bytes = Encoding.UTF8.GetBytes(text);
        res.OutputStream.Write(bytes, 0, bytes.Length);
        res.OutputStream.Flush();
    }

    static void Json(HttpListenerResponse res, int status, string body)
    {
        var bytes = Encoding.UTF8.GetBytes(body ?? "{}");
        res.StatusCode = status;
        res.ContentType = "application/json";
        res.ContentLength64 = bytes.Length;
        res.OutputStream.Write(bytes, 0, bytes.Length);
        res.Close();
    }

    public void Dispose()
    {
        lock (gate)
        {
            foreach (var c in sseClients) { try { c.Close(); } catch { } }
            sseClients.Clear();
        }
        try { listener.Stop(); listener.Close(); } catch { }
    }
}
