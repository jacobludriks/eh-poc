# Local harness for scripts/smoke-test.ps1

Proves each smoke-test assertion works without any Azure access: the real script runs against a fake app (the HTTP
API contract from ticket 04, canned data) and fake `az` / `kubectl` shims that are on PATH for the child process
only. Needs only `pwsh` 7.3+. It never contacts Azure or a cluster.

```powershell
pwsh -File scripts/tests/smoke-test.tests.ps1                  # all scenarios (about 2 minutes)
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

These tests cannot prove the real system: the real SASL handshake, DNS, role propagation and Azure resource
properties are only exercised by running `scripts/smoke-test.ps1` against a live deployment.
