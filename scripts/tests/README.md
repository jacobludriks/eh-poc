# Local harness for scripts/smoke-test.ps1

Proves each smoke-test assertion works without any Azure access: the real script runs against a fake app (the HTTP
API contract from ticket 04, canned data) and fake `az` / `kubectl` shims that are on PATH for the child process
only. Needs only `pwsh` 7.3+. It never contacts Azure or a cluster.

```powershell
pwsh -File scripts/tests/smoke-test.tests.ps1                  # all scenarios (about 3 minutes)
pwsh -File scripts/tests/smoke-test.tests.ps1 -Only 'roles-*'  # a subset, by scenario name wildcard
pwsh -File scripts/tests/smoke-test.tests.ps1 -ShowOutput      # also print the smoke-test output when a scenario is ok
```

Exit code 0 means every scenario behaved as expected: the healthy scenarios pass all assertions, and each mutation
makes exactly the named assertion(s) FAIL with a non-zero exit.

- `FakeEventsApp.cs`: in-process HTTP server (readiness, diagnostics, POST/GET events, SSE, clear) with switches
  to break one behaviour at a time.
- `shims/fake-az.ps1`, `shims/fake-kubectl.ps1`: serve canned JSON from the scenario directory, log every call to
  `calls.log`, and fail on any command they do not know.
- `fixtures/`: the healthy canned data (diagnostics, namespace, role assignments, federated credentials, secrets,
  deploy state). All ids are placeholders.
- `smoke-test.tests.ps1`: the scenario table. A scenario is a name, the assertion ids that must FAIL (empty means
  all pass) and a `Mutate` scriptblock that edits the fixture data. `$allIds` there is the independent list of
  assertions the script must report; keep it in step with `smoke-test.ps1`.

## Negative mode (`-NegativeTest`, `-RestoreFederatedCredential`)

The opt-in negative test breaks the federated credential, so it is only ever run here against the fakes. The fake
app models workload identity the way the real one behaves: a token is exchanged when the pod starts, so the fake
`kubectl rollout restart` calls the fake app's test-only `POST /__fake/restart`, which reads the same
`federated-credentials.json` the fake `az identity federated-credential update` rewrites. After a restart with no
credential for the valid subject the app answers `POST /api/events` with 503 (`BrokenReason`, default
`authentication`) and diagnostics show no assigned partitions and a `lastErrorClass` (`BrokenLastErrorClass`).
Fake-app switches: `FederationLagRestarts` (Entra propagation delay, counted in restarts, both directions),
`IgnoreFederation` (the change never takes effect), `AuthBroken` (a pod that starts out broken). Shim switches
(per-scenario files): `update-fail-calls.txt` (comma list of update call numbers that fail) and
`fail-rollout-restart`.

Scenarios (`negative-*`, `restore-switch-*`) cover: the full cycle (baseline, break, expected failure attributed to
authentication, restore, recovery); refusal without confirmation / with a leftover backup / when the credential is
already broken / with several credentials (exit 2, nothing changed); a failing baseline changes nothing; failures
that are not authentication (unavailable, authorization) are not attributed to it, while a timeout whose consumer
error is a login failure is; propagation lag in both directions; a change with no effect (reported, still
restored); an error mid-run, a flaky restore (retried), an impossible restore (loud, backup kept) and a stopped run
(the script is stopped inside a runspace, the same stop Ctrl-C causes) all end with the credential restored where
restoring is possible; and the recovery switch (restores from the saved file, restarts, checks the round trip; does
nothing without a saved file; refuses a file from another identity). A standard run is also checked to make no
credential update and no restart. A scenario can set `Ids` (the full id set it expects, for runs that are not the
standard set) and `ExpectExit` (a setup problem: no assertion may be reported).

These tests cannot prove the real system: the real SASL handshake, DNS, role propagation and Azure resource
properties are only exercised by running `scripts/smoke-test.ps1` against a live deployment.

## Checks for scripts/deploy.ps1 (`deploy.helpers.tests.ps1`)

`pwsh -File scripts/tests/deploy.helpers.tests.ps1` parses every script and unit-checks the pure helpers of
`deploy.ps1` (IPv4 octet bounds, the role-assignment check against a fake `az`, the "ACR Tasks are blocked" hint and
the capture pattern around `az acr build`). It extracts the functions from the script's syntax tree, so `deploy.ps1`
itself is never run and nothing touches Azure. The full deployment is only exercised by really running it.
