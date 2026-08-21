# Handoff — khaos Python → Go rewrite

Written for a fresh session with no prior context. Read this top to bottom before touching
anything.

---

## 1. Where things stand

The rewrite is **done and working**. Python is deleted. ~13.3k lines of Go across 9
packages, ~14.3k lines of tests, 341 test functions, all green under `-race`.

Verified end to end against a real 3-broker Docker cluster: traffic generation, broker
stop/restart faults, consumer rebalances, correlated flows, and Avro + Protobuf through a
live Schema Registry — with the **Java** `kafka-avro-console-consumer` and
`kafka-protobuf-console-consumer` successfully reading Go-produced records.

Branch: `go-rewrite`. **Nothing is committed.** `git status` shows 86 deletions (the Python
tree), 6 modifications, 8 untracked. That is expected — see rule 1 below.

Verify at any time:

```bash
./scripts/check.sh        # gofmt, vet, tests, static build, smoke test. No Docker needed.
./scripts/check.sh -r     # same, with the race detector
```

---

## 2. HARD RULES — do not break these

### Rule 1: NO git write commands. Ever.

The user explicitly forbade `git add`, `git commit`, `git push`. All work stays as
working-tree changes; they will make the commits themselves. Read-only git (`status`,
`diff`, `show`, `log`) is fine and often necessary — the deleted Python source is still
reachable via `git show HEAD:src/khaos/...`, which is the specification.

### Rule 2: Behaviour must match the Python version exactly.

The user stated this twice, forcefully, after I twice "improved" behaviour without asking.
The Python implementation is the specification, including its bugs. Everything below is
deliberately faithful, each behind a single flippable variable:

| Behaviour | Where | Value |
|---|---|---|
| Topics deleted + recreated every run | `internal/kafka/policy.go:65` | `RecreateTopicsByDefault = true` |
| Partition = CRC32 of key (librdkafka `consistent_random`, NOT murmur2) | `internal/kafka/policy.go:26` + `partitioner.go` | hand-written |
| Eager range+roundrobin rebalancing | `internal/kafka/policy.go:46` | not cooperative-sticky |
| `change_producer_rate` is a **no-op** | `internal/engine/producer.go:137` | `LiveProducerRateChanges = false` |
| Impossible cardinality **hangs forever** | `internal/generate/field.go` | `BoundCardinalityFill = false` |
| Flow instances unbounded | `internal/engine/flow.go:35` | `DefaultFlowConcurrency = 0` |
| `target.indices` / `group_id` accepted and **ignored** | `internal/scenario/incident.go` | filter removed |
| Rebalanced consumer **loses** its failure config | `internal/scenario/incident.go` | `Conf` not passed to `CreateConsumer` |
| Hardcoded `kafka-1/2/3`, `replication_factor <= 3` | `internal/scenario/validate.go:42` | `var Cluster` |

**If you think one of these is a bug: it is. Leave it. Ask first.**

Two things genuinely could not be made identical, both documented in code: faker →
gofakeit produces different values and has no locales, and protobuf+registry no longer
re-registers a regex-parsed schema (Go has no equivalent regex parser).

### Rule 3: The user is terse and wants working code.

Short answers. Verify claims with commands rather than asserting them — I was wrong three
times by inferring instead of checking, and each time running one command would have
caught it.

---

## 3. THE IMMEDIATE TASK: restyle the live TUI

This is what the next session is for.

The user compared the Go CLI unfavourably to Python's Rich output ("why is UI so ugly? it
just text"). I restyled the **static** commands with lipgloss and they now look right:

- `internal/tui/../cmd/khaos/style.go` — shared palette, TTY/`NO_COLOR` detection
- `cmd/khaos/scenarios.go` — `list` renders via `charm.land/lipgloss/v2/table`, rounded
  border, coloured category headings and scenario names, width-capped and wrapping
- `cmd/khaos/cluster.go` — `cluster-status` same treatment, state coloured green/yellow
- `cmd/khaos/scenarios.go` — `validate` gets `✓` / `!` / `✗` and coloured severities

**`internal/tui/tui.go` (460 lines) has NOT had this treatment.** It is the screen a user
stares at for a whole run, and it still lays out columns by hand:

```go
hdr := fmt.Sprintf("%-28s %12s %12s %10s", "TOPIC", "PRODUCED", "CONSUMED", lagHdr)
row := fmt.Sprintf("%-28s %12d %12d %s", ...)
```

What to do:

- Rebuild the topic table, flow table and totals with `charm.land/lipgloss/v2/table`,
  matching the look already established in `cmd/khaos/style.go`. Reuse that palette rather
  than inventing a second one — currently `tui.go:90-95` defines its own styles.
- Respect the terminal width (`tea.WindowSizeMsg` is already handled, `m.width`).
- Keep the events pane; consider `charm.land/bubbles/v2` viewport if it needs scrolling.
- **Do not change what is displayed**, only how. `internal/tui/render_test.go` (614 lines)
  pins the content; expect to update assertions that hardcode column widths, and keep the
  semantic ones.

### Non-negotiable constraint on the TUI

The engine must stay completely independent of it. This is the entire point of the
rewrite: in Python the run executed *inside* `with Live(...)` and the render loop owned
both the duration deadline and the shutdown trigger (`src/khaos/executor/base.py:385-398`),
so a stalled UI could wedge an infinite run.

Therefore:

- `internal/engine` must NEVER import `internal/tui`.
- The TUI's only read API is `engine.Snapshot()` — a pure value it **pulls** on a tick.
  Never push from the engine, never hand the TUI a pointer into engine state.
- The output layer runs OUTSIDE the engine's errgroup (`cmd/khaos/run.go`), with its own
  context and a panic recover, so a TUI crash degrades to headless instead of killing the
  run.

---

## 3b. SECOND TASK: image-pull progress on first run

Right now the very first `khaos cluster-up` (or `khaos run`, which auto-starts the cluster)
prints `Starting Kafka cluster...` and then **sits silently for several minutes** while
Docker pulls ~1GB of images. It looks hung. On every later run the images are cached and it
is fast, so the bad experience hits exactly once — on someone's first contact with the tool.

Wanted: detect whether the images are already present, and only if some are missing, show a
real progress bar while they download.

### The facts, all verified on 2026-08-20 — do not re-derive these

**The four images** (from `internal/localcluster/assets/*.yml`):

```
confluentinc/cp-kafka:7.6.0
confluentinc/cp-schema-registry:7.6.0
confluentinc/cp-zookeeper:7.6.0
provectuslabs/kafka-ui:latest
```

Enumerate them at runtime rather than hardcoding — `docker compose -f <file> config --images`
works and respects which compose file is in play, but note it prints **duplicates** (once per
service: cp-kafka appears three times). Dedupe.

**Existence check** — `docker image inspect <ref>`, exit 0 means present:

```bash
docker image inspect confluentinc/cp-kafka:7.6.0 >/dev/null 2>&1
```

**Progress stream** — this is the key find. `docker compose` supports
`--progress json` (values: `auto, tty, plain, json, quiet`) and emits newline-delimited
JSON. `docker pull` does NOT have this flag; it must be `docker compose`.

Real captured output from `docker compose -f <file> --progress json pull`:

```json
{"level":"warning","msg":"... the attribute `version` is obsolete ...","time":"..."}
{"id":"Image confluentinc/cp-zookeeper:7.6.0","status":"Working","text":"Pulling"}
{"id":"32d02e677a77","parent_id":"Image confluentinc/cp-zookeeper:7.6.0","status":"Working","text":"Pulling fs layer","details":"0B"}
{"id":"90bb2e7c3a87","parent_id":"Image confluentinc/cp-zookeeper:7.6.0","status":"Working","text":"Downloading","details":"2.097MB","current":2097152,"total":114892458,"percent":1}
{"id":"32d02e677a77","parent_id":"Image confluentinc/cp-zookeeper:7.6.0","status":"Done","text":"Download complete","details":"0B","percent":100}
{"id":"Image alpine:3.19","status":"Done","text":"Pulled"}
```

So you get, per layer: `current` / `total` bytes and `percent`, with `parent_id` linking the
layer to its image, and `status` of `Working` or `Done`. Summing `current`/`total` across
layers gives a genuine byte-level bar; counting images gives "2 of 4".

Two parsing gotchas, both visible above:
- Lines with `level`/`msg` and no `id` are **log records, not progress** — skip them.
- An image-level event has `id` prefixed `"Image "` and no `parent_id`; a layer event has
  `parent_id`. Do not mix them or you will double-count.

### Implementation notes

- Belongs in `internal/localcluster` (a `Pull`/`EnsureImages` method), invoked from
  `Cluster.Up` before `compose up`. Keep it out of `cmd/`.
- **If nothing is missing, do nothing at all** — no bar, no output, no delay. That is the
  actual ask.
- `docker compose pull --policy missing` exists and skips present images, but you still want
  the pre-check so you can decide whether to show any UI.
- Render with `charm.land/bubbles/v2/progress`, styled from `cmd/khaos/style.go`. But the
  progress bar lives in a command that has not started the Bubble Tea TUI yet, so either run
  a short standalone `tea.Program` for the pull phase or draw with carriage returns. A
  standalone program is cleaner and matches the existing separation.
- **Must degrade when stdout is not a TTY** (`--tui off`, CI, piped): emit plain lines such
  as `pulling confluentinc/cp-kafka:7.6.0 (1/4)` instead of a bar. `colorEnabled` /
  `detectColor()` in `cmd/khaos/style.go` already makes this decision; reuse it rather than
  adding a second one.
- Honour context cancellation — Ctrl-C during a 1GB pull must exit promptly.
- Testable without Docker: factor the JSON-stream parser into a pure function over an
  `io.Reader` and feed it the captured lines above as a fixture. Do not require a daemon in
  CI. The existence check can be an injected `func(string) bool`.

### While you are in there

All four compose files start with an obsolete `version:` key, which makes Docker print a
warning on **every single invocation**:

```
the attribute `version` is obsolete, it will be ignored
```

Deleting that line from `internal/localcluster/assets/*.yml` silences it. Harmless but noisy,
and it will pollute the progress output you are about to parse.

---

## 4. Architecture map

```
cmd/khaos/              CLI: main, run/simulate, list/validate, cluster-*, style.go
internal/scenario/      domain model, YAML decode, validation, incidents  (no deps)
internal/generate/      field/key/payload generators, correlated flows
internal/codec/         JSON | Avro | Protobuf, ± Schema Registry
internal/kafka/         franz-go clients, SASL/TLS, admin, policy.go, partitioner.go
internal/engine/        producers, consumers, scheduler, counters, Snapshot
internal/localcluster/  Docker Compose control (compose files embedded via go:embed)
internal/tui/           Bubble Tea UI — imports engine, never the reverse
internal/telemetry/     slog, prometheus, /healthz + /metrics
```

Dependency direction: `scenario` ← `codec` ← `generate` ← `engine` → `kafka`.
`tui` and `telemetry` depend on `engine`. Nothing depends on `tui`.

Key types:
- `engine.Snapshot` — the engine's entire read API (`internal/engine/stats.go`)
- `scenario.Command` — incident → command ADT, unit-testable without a broker
- `codec.Codec` — two methods, four implementations
- `codec.Doc` / `generate.Doc` — ordered documents; JSON key order is contractual
  (Python's `json.dumps` preserves insertion order, Go maps sort)

---

## 5. Traps — I hit every one of these

**`go mod tidy` will silently empty go.mod.** It prunes anything no package imports yet. I
ran it early and wiped every dependency, blocking three parallel agents. If you add
packages incrementally, do not tidy until they all import what they need.

**Test doubles that are more helpful than reality hide real bugs.** `fakeCluster` in
`cmd/khaos/run_teardown_test.go` made `BootstrapServers` return an error when the cluster
was down. The real one falls back to `127.0.0.1:9092` and never errors. `khaos run`'s
auto-start guard tested the error, so it was dead code and shipped broken — `run` on a
stopped cluster died with "connection refused". The double is now faithful, and
`Cluster.Running()` is the correct is-it-up test.

**Port 8080.** The bundled compose stack runs `kafka-ui` on 8080, which collides with the
user's nocodb container. `cluster-up` fails; the error now names the real port (it used to
hardcode a wrong claim about 9092-9094). Stop nocodb with
`docker compose -p 2_pg stop` — **never** `down -v`, their Postgres volume is in there.

**`defaults.py` is a trap.** Python has two disagreeing default sets. `scenarios/scenario.py`
wins for YAML-loaded configs (`acks="1"`, `compression="lz4"`, `200/500/50`);
`defaults.py` says `"all"`/`"none"`/`100/1000/100` and applies only where a config is built
without scenario data. `internal/scenario/types.go` collapses both into one const block —
do not "correct" those values.

**Homebrew.** `khaos` is in homebrew-core as a **Python** formula, auto-bumped by
BrewTestBot from PyPI. Stop publishing to PyPI and it freezes at 0.7.1 Python forever. Fix
is one PR converting it to a Go formula, after which BrewTestBot resumes from git tags. The
user does not care about this (`RUNBOOK.md` §4 marks it OPTIONAL). Do not spend time on it
unless asked. One live constraint though: the formula's test asserts `khaos list` contains
**"Available Scenarios"** — `cmd/khaos/list_contract_test.go` pins that string.

**Tests need no Docker.** `kfake` is an in-process broker speaking the real Kafka protocol.
Use it. `cmd/khaos/main_test.go` drives the whole CLI against it.

---

## 6. Verification

```bash
# Fast, no Docker
./scripts/check.sh -r

# With the real cluster (free port 8080 first)
go build -o khaos ./cmd/khaos
./khaos cluster-up
./khaos run traffic/high-throughput -d 15 -k       # TUI
./khaos run traffic/high-throughput -d 15 -k --tui off --log-json   # headless
./khaos cluster-down
```

`RUNBOOK.md` §2 has a 12-step manual test plan with expected output for each step. The two
steps worth not skipping are **6** (Ctrl-C must stop an infinite run promptly — the defect
this rewrite targets) and **9** (Java consumers reading Go-produced Avro/Protobuf — the
only way to catch a wrong Confluent header).

---

## 7. Open items

`DECISIONS.md` — 13 questions, every default already live and Python-matching. The user has
not answered them and does not have to; ignore unless asked.

Remaining work, none blocking:

- **TUI restyle** — section 3. Primary task.
- **Image-pull progress** — section 3b. Secondary task, fully specified.
- `assets/demo.gif` shows the old Rich UI. `scripts/demo.tape` needs **no edits** (every
  command in it works); just re-record: `cd scripts && vhs demo.tape && mv demo.gif ../assets/`.
  Needs Docker up.
- Final PyPI `0.7.2` announcing the move to Go, so pip users are not stranded. Optional.
- `internal/telemetry` is built and tested but the engine does not feed it yet. The user
  said Prometheus is not a priority.

---

## 8. Docs in this repo

- `RUNBOOK.md` — build, 12-step manual test plan, GoReleaser, Homebrew
- `DECISIONS.md` — the 13 open decisions with exact file:line for each
- `README.md` — user-facing; install/CLI/architecture updated, YAML reference unchanged
  (the scenario format is genuinely identical to Python's)
