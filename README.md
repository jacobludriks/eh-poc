# Passwordless Spring Kafka to Azure Event Hubs on AKS (proof of concept)

A disposable, runnable proof that a Spring Boot service can produce to and consume from Azure Event Hubs over the
Kafka protocol with **no password, connection string or SAS key**, authenticating with an AKS workload identity
(federated token), while the Event Hubs namespace is reachable **only through a private endpoint**.

## What it proves

- One pod on a one-node AKS cluster sends events to an event hub with spring-kafka, and a listener in the Kafka
  consumer group `dashboard` reads them back. A one-page dashboard (`/`) sends events, shows them live over
  Server-Sent Events and clears the in-memory view (the hub itself is never purged).
- Authentication is Spring Cloud Azure's OAUTHBEARER callback handler using a workload identity token. The
  namespace has local (key) authentication disabled, so a secret could not work even by accident.
- `/api/diagnostics` shows, from inside the pod, the effective SASL mechanism, the login handler, whether the
  passwordless auto-configuration really took effect, the private IP the bootstrap host resolves to, and that no
  credential properties are configured.
- Stack: Java 21, Spring Boot 4.1.2, Spring Cloud Azure 7.5.0. Region `australiaeast` by default.

Layout: `app/` (the service), `infra/` (Bicep), `k8s/` (manifests), `scripts/` (deploy, smoke test, destroy and
their local tests), `.scratch/eventhubs-passwordless-poc/` (spec and tickets).

## Prerequisites

- PowerShell 7.3+ and the Azure CLI, signed in (`az login`) to the subscription to use. `kubectl` is installed for
  you through the Azure CLI if missing. No local Java, Maven or Docker is needed to deploy.
- Permission to **create role assignments** (Owner or User Access Administrator, or Role Based Access Control
  Administrator, on the subscription or the resource group). The templates assign AcrPull to the cluster and Event Hubs
  Data Sender/Receiver to the workload identity. Contributor alone fails with `AuthorizationFailed`. `deploy.ps1`
  warns (does not block) if it cannot confirm this.
- Quota in the region for a small AKS node pool, and the resource providers registered (the script registers them).
- **`az acr build` available on the subscription.** The image is built remotely by ACR Tasks. Some subscription
  types (free trials, sponsorships) block ACR Tasks; see below.

### If ACR Tasks are blocked

`deploy.ps1` then stops at the build with a message naming the image. Build the image anywhere you have Docker and
push it to the registry under exactly that name, then re-run `deploy.ps1`: it sees the tag already in ACR and skips
the build.

```powershell
az acr login --name <acr-name>                      # needs AcrPush (or Owner/Contributor) on the registry
docker build -t <acr-login-server>/ehpoc:<tag> app  # <tag> is the one printed by the failed run
docker push <acr-login-server>/ehpoc:<tag>
```

The registry is the one deliberate exception to private networking: a **Basic-tier ACR with public endpoints**.
Private endpoints for ACR need the Premium tier, and the PoC is about Event Hubs. Everything else on the data path
(Event Hubs with public access disabled, a private endpoint, the linked private DNS zone) is private; the AKS API
server is public but restricted to your IP.

## Run it

```powershell
pwsh -File scripts/deploy.ps1   # AKS creation takes several minutes on first run
pwsh -File scripts/smoke-test.ps1                # proves the claims; exit code 0 only if every assertion passes
pwsh -File scripts/smoke-test.ps1 -NegativeTest  # opt-in, see warning
pwsh -File scripts/destroy.ps1                   # deletes the whole resource group
```

- `deploy.ps1` detects your public IP (override with `-CallerIp`), deploys the infrastructure, builds the image,
  applies `k8s/`, waits for readiness, then sends one probe event through Event Hubs. It prints the dashboard
  (`http://<ip>/`), `/actuator/health`, `/api/diagnostics` and the send endpoint. Only your IP can reach them. Safe
  to re-run. Role assignments can take up to about five minutes to take effect, so a first-run authorization failure
  is retried (`-VerifyTimeoutSeconds`, default 360) rather than treated as a defect.
- `smoke-test.ps1` checks the HTTP surface (round trip, SSE, diagnostics, clear) and the Azure resources (local
  authentication and public access disabled, role scope, federated subject, no secrets in the namespace).
- **`-NegativeTest` temporarily breaks the federated credential** of the deployed PoC to prove the success is
  caused by workload identity, then restores it (also on failure or Ctrl-C). It asks for confirmation unless
  `-Force`. If a run is killed midway: `pwsh -File scripts/smoke-test.ps1 -RestoreFederatedCredential`.
- `destroy.ps1` asks you to type the resource group name unless `-Force`.

## Local validation (no Azure needed)

```powershell
cd app; ./mvnw package                                 # unit tests plus a round trip against an in-process broker
pwsh -File scripts/tests/smoke-test.tests.ps1          # smoke test against a fake app and fake az/kubectl (~3 min)
pwsh -File scripts/tests/deploy.helpers.tests.ps1      # parse checks and deploy.ps1 helper functions
```

These prove the consumer, buffer, SSE, clear, diagnostics and script logic. They cannot prove anything about Azure.

## Fallback: Spring Boot 3.5.x with Spring Cloud Azure 6.6.0

If Azure's callback handler turns out incompatible with the Kafka 4.2.x client that Boot 4.1.2 brings, move to the
older line. In `app/pom.xml`:

1. Parent `spring-boot-starter-parent` to a 3.5.x release; `spring-cloud-azure.version` to `6.6.0`.
2. Replace `spring-boot-starter-webmvc` with `spring-boot-starter-web`, and `spring-boot-starter-kafka` with plain
   `org.springframework.kafka:spring-kafka` (Boot 3 has no Kafka starter). Keep `spring-boot-starter-actuator`,
   `spring-cloud-azure-starter` (the base starter; check it resolves from the 6.6.0 BOM) and the test dependencies.
3. Jackson 2 instead of Jackson 3: `tools.jackson.databind.json.JsonMapper` / `tools.jackson.databind.JsonNode`
   become `com.fasterxml.jackson.databind.json.JsonMapper` / `com.fasterxml.jackson.databind.JsonNode`
   (`EventConsumer`, `EventPublisher`, `EventStream`, and the test). Jackson 2's `writeValueAsString` and
   `readValue` throw checked `JsonProcessingException`, so wrap them; in tests `JsonNode.asString()` is `asText()`.
4. Any code or test touching `KafkaProperties` imports it from `org.springframework.boot.autoconfigure.kafka`
   (Boot 4 moved it to `org.springframework.boot.kafka.autoconfigure`). Other Boot 4 package moves show up as compile
   errors; fix them the same way.
5. Verify: `cd app; ./mvnw package` (the round-trip test must pass), then redeploy and run the smoke test. The
   configuration in `application.yml` and the Kubernetes and Bicep files do not change.

Record the outcome either way (worked, failed, caveats, fallback taken) at the end of the PoC (ticket 08).

## Status: built and locally validated, NOT yet deployed

Nothing has been deployed to Azure. Built and checked locally only: the app compiles and its tests pass, the Bicep
templates build and lint, scripts parse, and the smoke-test logic passes against fakes. **Unverified until a real
deployment** (the spec's risks, plus ones added since):

- Azure's OAUTHBEARER callback handler working with the Kafka 4.2.x client.
- Whether offset commits need more than the Event Hubs Data Receiver role.
- Token refresh behaviour over a long-running connection.
- Role-assignment propagation time (up to about five minutes), and the retry budget being enough.
- Private DNS resolving the public namespace name to the private IP from inside the pod.
- The pod hardening in `k8s/20-deployment.yaml`: read-only root filesystem with `/tmp` writable, non-root user
  10001, no mounted API token (the workload identity webhook should still project its own token volume), and the
  1Gi memory limit being enough. It was checked locally only by running the jar with a write-protected working
  directory.
- `az acr build` being allowed on the subscription, and AKS/Event Hubs quota in the region.

Treat any success claim as unproven until `scripts/smoke-test.ps1` passes against a live deployment.
