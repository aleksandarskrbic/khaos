# Runbook: build, test, release

Everything you need to drive the Go rewrite by hand.

---

# 1. Build and run locally

## Prerequisites

- **Go 1.26+** — `go version`
- **Docker** — only for the local cluster. `khaos simulate` against your own broker needs nothing else.

That is the whole list. There is no Python, no `uv`, no virtualenv, and no librdkafka:
the Kafka client is pure Go.

## Build

```bash
cd ~/GoProjects/khaos
go build -o khaos ./cmd/khaos
./khaos --version
```

For a release-shaped binary (stripped, reproducible, no cgo):

```bash
CGO_ENABLED=0 go build -trimpath -ldflags "-s -w -X main.version=1.0.0" -o khaos ./cmd/khaos
```

Run without building at all:

```bash
go run ./cmd/khaos list
```

Install onto your PATH:

```bash
go install ./cmd/khaos      # lands in $(go env GOPATH)/bin
```

## The check script

`./scripts/check.sh` is what CI runs: gofmt, vet, tests, static build, smoke test.
Add `-r` for the race detector. **It needs no Docker** — the tests use an in-process
Kafka broker (`kfake`).

```bash
./scripts/check.sh        # ~1 min
./scripts/check.sh -r     # ~3 min, race detector
```

---

# 2. Manual test plan

Work through these in order. Each step says what to look for, so a wrong result is
obvious. Times assume a warm Docker.

## Step 0 — automated suite first

```bash
./scripts/check.sh -r
```

**Expect:** `All checks passed.` If this fails, stop; nothing below will be meaningful.

## Step 1 — no cluster needed

```bash
./khaos --version
./khaos --help
./khaos list
./khaos validate                                     # every bundled scenario
./khaos validate traffic/high-throughput
./khaos validate ./scenarios/chaos/broker-chaos.yaml
```

**Expect:** `list` prints a two-column table of 21 scenarios grouped into five categories,
starting with the line `Available Scenarios` (the homebrew formula asserts that string).
Every `validate` call prints `valid` per scenario and a closing `all N scenario(s) valid`,
and exits 0 (`echo $?`). Bare `./khaos validate` covers all 21 and reports four warnings —
warnings do not fail it.

Check the exit codes, which scripts branch on:

```bash
./khaos; echo "exit=$?"                              # help on stderr, exit=2
./khaos bogus; echo "exit=$?"                        # exit=2
./khaos run --nope; echo "exit=$?"                   # exit=2
./khaos run; echo "exit=$?"                          # exit=1, names `khaos list`
./khaos validate no/such/scenario; echo "exit=$?"    # exit=1
```

**Expect:** `2` for a bad invocation, `1` for a command that ran and failed. This is what
Click did under the Python version.

Now prove validation actually reports everything at once:

```bash
cat > /tmp/bad.yaml <<'EOF'
name: broken
topics:
  - name: orders
    partitions: -3
    replication_factor: 9
    producer_config:
      acks: "maybe"
EOF
./khaos validate /tmp/bad.yaml; echo "exit=$?"
```

**Expect:** three separate errors — `partitions`, `replication_factor` and
`producer_config.acks` — each with a **line number**, then a `1 of 1 scenario(s) invalid`
tally and `exit=1`. Not one error — all of them. (Python had no line numbers; this is the
one deliberate improvement.)

`--strict` additionally rejects keys no khaos version has ever read, which catches a typo
in a field name rather than silently ignoring it:

```bash
printf 'name: typo\ntopics:\n  - name: orders\n    partitons: 6\n' > /tmp/typo.yaml
./khaos validate /tmp/typo.yaml; echo "lenient exit=$?"
./khaos validate --strict /tmp/typo.yaml; echo "strict exit=$?"
```

## Step 2 — start the cluster

```bash
./khaos cluster-up
./khaos cluster-status
```

**Expect:** `cluster ready: 127.0.0.1:9092,127.0.0.1:9093,127.0.0.1:9094`, then three
brokers `running` on 9092/9093/9094.

> **Port 8080 conflict.** The stack also starts `kafka-ui` on 8080. If something else holds
> it (nocodb, for example) `cluster-up` fails and now tells you *which* port. Free it, or
> ignore kafka-ui — the brokers work regardless.

`cluster-up` is idempotent; running it again is safe.

ZooKeeper mode uses the same ports and the same compose project, so it is a drop-in check:

```bash
./khaos cluster-down
./khaos cluster-up -m zookeeper
./khaos cluster-status -m zookeeper     # brokers plus a zookeeper service
./khaos cluster-down
```

## Step 3 — a basic run

```bash
./khaos run traffic/high-throughput -d 15 -k
```

`-k` keeps the cluster up for the following steps.

**Expect:** a live terminal UI with a topic table, counts climbing, lag near zero. After
15s it exits and prints a summary. Roughly 60k produced. Errors should be **0**.

The `LAG` column here is khaos's own produced−consumed count, exactly as the Python
version reported it — no broker is asked anything. Step 6b covers the real one.

Then the headless path, which is what runs in a container:

```bash
./khaos run traffic/high-throughput -d 15 -k --tui off --log-json
```

**Expect:** one JSON log line every 10s, then the summary. No TUI. This must work when
piped: `... --tui off | cat` should not corrupt anything.

A typo in either flag is rejected rather than silently falling back:

```bash
./khaos run traffic/high-throughput --tui of; echo "exit=$?"          # exit=1
./khaos run traffic/high-throughput -m zookeper; echo "exit=$?"       # exit=1
```

Now the teardown, which must happen whether the run succeeded or not:

```bash
# Success, no -k: the cluster is stopped again.
./khaos run traffic/high-throughput -d 5 --tui off
docker ps --filter name=kafka-      # empty

# Failure, no -k: still stopped. Nothing listens on port 1.
./khaos cluster-up
./khaos run traffic/high-throughput -b 127.0.0.1:1 -d 5 --tui off; echo "exit=$?"
docker ps --filter name=kafka-      # empty

# Failure WITH -k: left running, so you can inspect what went wrong.
./khaos cluster-up
./khaos run traffic/high-throughput -b 127.0.0.1:1 -d 5 -k --tui off; echo "exit=$?"
docker ps --filter name=kafka-      # three brokers still up
./khaos cluster-down
```

**Expect:** `stopping Kafka cluster...` on the first two, `Kafka cluster left running` on
the third, and `exit=1` on the two failures. Leaking a three-broker cluster on every failed
run is the most expensive thing this CLI could get wrong; `cmd/khaos/run_teardown_test.go`
pins all four combinations in CI.

## Step 4 — Ctrl-C actually works

```bash
./khaos run traffic/high-throughput -k        # -d defaults to 0 = run forever
```

Let it run ~10s, then press **Ctrl-C once**.

**Expect:** it stops within a couple of seconds and prints a summary. Press Ctrl-C twice
in quick succession and it dies immediately. Neither should hang.

This is the specific defect the rewrite targets: in the Python version the render loop
owned the shutdown path, so a stalled UI could wedge an infinite run.

## Step 5 — broker faults

```bash
cat > /tmp/fault.yaml <<'EOF'
name: fault-check
description: stop and restart a broker
topics:
  - name: fault-check
    partitions: 6
    replication_factor: 3
    num_producers: 2
    producer_rate: 200
    producer_config:
      acks: "all"
incidents:
  - type: stop_broker
    at_seconds: 8
    broker: kafka-2
  - type: start_broker
    at_seconds: 20
    broker: kafka-2
EOF
./khaos run /tmp/fault.yaml -d 32 -k --tui off
```

**Expect:** events at ~8s (`Stopping kafka-2`) and ~20s (`Starting kafka-2`). Production
continues throughout; errors stay 0 because `acks: all` plus RF 3 survives one broker.

Verify the broker really was killed:

```bash
docker ps --filter name=kafka- --format '{{.Names}}\t{{.Status}}'
```

**Expect:** `kafka-2` uptime is much lower than kafka-1/kafka-3.

## Step 6 — client-side incidents

```bash
./khaos run chaos/rebalance-storm -d 80 -k --tui off      # consumers torn down and recreated
./khaos run chaos/throughput-drop  -d 60 -k --tui off     # consumer delay ramps, lag grows
```

Both durations are chosen to outlast the incident schedules in the YAML, which is easy to
get wrong: `rebalance-storm` waits `initial_delay_seconds: 15` and only then starts its
`every_seconds: 20` ticker, so the first rebalance lands at **T+35s** and the second at
T+55s — a 40s run sees none at all. `throughput-drop` fires once at `at_seconds: 30`.

**Expect:** rebalance-storm ends with `rebalances 2` or more in the summary;
throughput-drop shows lag climbing from T+30s onwards.

## Step 6b — real consumer-group lag

Off by default. Everything above ran with the self-reported `LAG` column and must keep
doing so; this step is the only one that asks the brokers.

```bash
./khaos run chaos/throughput-drop -d 60 -k --lag-poll 5s
```

**Expect:** the topic table now carries **two** lag columns, `LAG(SELF)` and
`LAG(BROKER)`, and per-group rows carry their own broker figure. `LAG(BROKER)` is usually
far larger than `LAG(SELF)` in short runs: khaos auto-commits every 5s, so a group that
has read everything but committed nothing is genuinely behind by the whole log, and only
the broker column knows that. A group whose row reads `unknown` was not measured — that
is not a zero.

Then prove it is off by default and that it never becomes load-bearing:

```bash
./khaos run traffic/high-throughput -d 15 -k --tui off | grep -i broker   # no output
```

The failure path matters more than the happy one, because managed clusters routinely deny
`DESCRIBE` on consumer groups. Point khaos at a cluster where the credentials lack that
right (or revoke it) and re-run with `--lag-poll 5s`:

**Expect:** the run behaves *identically* — same throughput, same exit code, errors still
0. One warning is logged naming the group and the reason, **once**, not once per poll, and
the broker column reads `unknown` for the whole run. Leave it running for a few minutes
and confirm the warning does not repeat.

## Step 7 — flows

```bash
./khaos run flows/order-flow -d 20 -k --tui off
```

**Expect:** a flow section in the summary with `started`, `completed` and `messages`.
Completed should track started closely.

## Step 8 — Avro and Protobuf against a real Schema Registry

```bash
./khaos cluster-up --schema-registry
curl -s localhost:8081/subjects            # []

./khaos run serialization/avro-example -d 10 -k --tui off
./khaos run serialization/protobuf-example -d 10 -k --tui off

curl -s localhost:8081/subjects            # ["orders-value","shipments-value"]
```

Now the test that matters — can a **Java** consumer read what Go wrote? A wrong Confluent
header or protobuf message index round-trips fine in Go and fails only on the JVM.

```bash
docker exec schema-registry kafka-avro-console-consumer \
  --bootstrap-server kafka-1:29092 --topic orders \
  --from-beginning --max-messages 3 \
  --property schema.registry.url=http://localhost:8081

docker exec schema-registry kafka-protobuf-console-consumer \
  --bootstrap-server kafka-1:29092 --topic shipments \
  --from-beginning --max-messages 3 \
  --property schema.registry.url=http://localhost:8081
```

**Expect:** three decoded JSON records from each. Note `created_at` is an **integer**
(epoch millis, not ISO-8601) — that is Python's behaviour, deliberately preserved.

This registry is open, which is the default path and must keep working with no extra
flags. Step 8b covers the secured one.

## Step 8b — a Schema Registry that requires credentials

Confluent Cloud and Aiven put the registry behind auth, which Python could not reach at
all. Stand up the same thing locally: nginx with basic auth, proxying the registry from
step 8 on port 8082.

```bash
printf 'sruser:%s\n' "$(openssl passwd -apr1 srsecret)" > /tmp/khaos-sr.htpasswd
cat > /tmp/khaos-sr.conf <<'EOF'
server {
  listen 8082;
  location / {
    auth_basic           "schema registry";
    auth_basic_user_file /etc/nginx/htpasswd;
    proxy_pass           http://schema-registry:8081;
  }
}
EOF

docker run -d --rm --name sr-auth --network docker_kafka-net -p 8082:8082 \
  -v /tmp/khaos-sr.conf:/etc/nginx/conf.d/default.conf:ro \
  -v /tmp/khaos-sr.htpasswd:/etc/nginx/htpasswd:ro \
  nginx:alpine

curl -s -o /dev/null -w '%{http_code}\n' localhost:8082/subjects   # 401
curl -s -u sruser:srsecret localhost:8082/subjects                 # the subject list
```

**1. No credentials — the error has to be diagnosable.**

```bash
./khaos run serialization/avro-example -d 5 -k --tui off \
  --schema-registry-url http://localhost:8082
```

**Expect:** a failure *before any message is produced*, naming the URL and both flags:
`schema registry at "http://localhost:8082" rejected the startup reachability check with
HTTP 401 Unauthorized: this registry requires authentication but no credentials were
given; pass --schema-registry-username and --schema-registry-password …`. A bare
`401 Unauthorized` here is a bug.

**2. Wrong password.**

```bash
./khaos run serialization/avro-example -d 5 -k --tui off \
  --schema-registry-url http://localhost:8082 \
  --schema-registry-username sruser --schema-registry-password nope
```

**Expect:** the same shape, but pointing at `--schema-registry-username` /
`--schema-registry-password` and noting that on Confluent Cloud those are the **Schema
Registry** API key and secret, not the Kafka cluster's.

**3. Correct credentials — a normal run.**

```bash
./khaos run serialization/avro-example -d 10 -k --tui off \
  --schema-registry-url http://localhost:8082 \
  --schema-registry-username sruser --schema-registry-password srsecret

curl -s -u sruser:srsecret localhost:8082/subjects   # ["orders-value"]
```

**Expect:** identical output to step 8 — the credentials are used for the startup probe,
the schema registration and every fetch.

**4. Incoherent flags fail at startup, not on the first message.**

```bash
./khaos run serialization/avro-example --schema-registry-url http://localhost:8082 \
  --schema-registry-username sruser
./khaos run serialization/avro-example --schema-registry-url http://localhost:8082 \
  --schema-registry-username sruser --schema-registry-password srsecret \
  --schema-registry-token tok
./khaos run serialization/avro-example --schema-registry-url http://localhost:8082 \
  --schema-registry-ca-location /tmp/khaos-sr.conf
```

**Expect:** three different errors — password required with username; token cannot be
combined with basic auth; TLS files given against a URL that is not `https` (which would
have connected in the clear with the CA silently ignored).

```bash
docker rm -f sr-auth
```

## Step 9 — external cluster path

```bash
./khaos simulate traffic/high-throughput \
  -b 127.0.0.1:9092 -d 10 --tui off
```

**Expect:** identical behaviour to `run`, but broker-fault incidents report as *skipped*
because khaos does not own the cluster. Try `./khaos simulate chaos/leadership-churn -b 127.0.0.1:9092 -d 50 --tui off`
and look for the skip event rather than a failure.

## Step 10 — multiple scenarios together

```bash
./khaos run traffic/high-throughput traffic/consumer-lag -d 15 -k --tui off
```

**Expect:** topics from both scenarios in one run.

## Step 11 — tear down

```bash
./khaos cluster-down
docker ps            # no kafka-* containers
docker volume ls     # no khaos_* volumes either
```

**Expect:** a `note: cluster-down always removes data volumes` line on stderr, then
`cluster stopped`. Schema Registry goes down with it if step 8 started it. `--volumes`/`-v`
is accepted for compatibility with the Python CLI but changes nothing — the volumes go
either way.

## Step 12 — the container image

```bash
docker build -t khaos:local .
docker run --rm khaos:local --version
docker run --rm khaos:local list
```

**Expect:** builds with no C toolchain, and the image is tiny — scenarios and compose files
are embedded in the binary.

---

# 3. Releasing with GoReleaser

## What it does

On a pushed tag, GoReleaser reads `.goreleaser.yaml` and, in one run:

1. cross-compiles the binary for **linux/darwin/windows x amd64/arm64** (5 combos;
   windows/arm64 is excluded);
2. packs each into a `.tar.gz` (`.zip` on Windows) with README, LICENSE and CHANGELOG;
3. writes `checksums.txt`;
4. generates a changelog from commit messages since the previous tag;
5. builds **two container images** (amd64, arm64), pushes them to `ghcr.io`, and joins
   them under one multi-arch tag;
6. creates the **GitHub Release** and attaches everything.

Cross-compiling to five targets in one job is only possible because the Kafka client is
pure Go. With a cgo client each target would need its own C toolchain.

## Credentials

**You need to set up nothing.** `.github/workflows/release.yml` uses only
`secrets.GITHUB_TOKEN`, which GitHub Actions injects automatically. It covers both
creating the release and pushing to `ghcr.io`, because ghcr accepts the Actions token for
the repo's own namespace.

The workflow declares the permissions it needs:

```yaml
permissions:
  contents: write     # create the release, upload artifacts
  packages: write     # push to ghcr.io
```

One thing to check once, in the GitHub UI: **Settings → Actions → General → Workflow
permissions** must allow read *and* write. If it is read-only the release fails with a 403.

## Cutting a release

```bash
# 1. Make sure main is green
./scripts/check.sh -r

# 2. Dry run — builds everything locally, publishes nothing
go install github.com/goreleaser/goreleaser/v2@latest
goreleaser check                     # validate the config
goreleaser release --snapshot --clean
ls dist/                             # inspect the artifacts

# 3. Tag and push
git tag -a v1.0.0 -m "v1.0.0: Go rewrite"
git push origin v1.0.0
```

The tag push triggers the workflow. Watch it under Actions.

`--snapshot --clean` is the safe rehearsal: it does the whole build without needing a tag,
without touching GitHub, and without pushing images.

## First release after the rewrite

Bump the **major** version. `v1.0.0` is right: the CLI contract is preserved but the
distribution, the partition placement of any given key, and the faker provider names all
change. Anyone pinning `0.7.x` should not be dragged along silently.
