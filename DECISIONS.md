# Open decisions

Seventeen decisions. Four are still open; the rest are settled and struck through. Each one is live in the code right now at the
value in **Current**. To change one, edit the file in **Where** — nothing else needs touching.

Write your answer in the **You** column. If you agree with the default, put `ok`.

---

## Already wired — flip by editing one line

| # | Question | Current | Where | If you flip it | You |
|---|---|---|---|---|---|
| **D1** | Which partition does a key land on? Python inherits librdkafka CRC32; franz-go defaults to murmur2 (what Java uses). | **CRC32 — matches Python** (hand-written, `internal/kafka/partitioner.go`) | `internal/kafka/policy.go:26` | Set `kgo.StickyKeyPartitioner(nil)` for Java-compatible murmur2. A *different* partition then goes hot in `hot-partition`/`single_key`. | |
| **D2** | Consumer-group rebalance protocol. Python inherits eager range+roundrobin; franz-go defaults to cooperative-sticky. | eager range+roundrobin | `internal/kafka/policy.go:55` | `rebalance-storm` and `uneven-assignment` behave visibly differently — they're *about* rebalances. | |
| **D4** | Delete + recreate topics on every run? Python does, unconditionally. | **on — matches Python** | `internal/kafka/policy.go:74` | Set `false`, or pass `--recreate-topics=false`, to leave existing topics and their data alone. | |
| **D9** | Producer buffer full: block or drop? Python lets the error kill the producer permanently. | block (backpressure) | `internal/kafka/policy.go:89` | Affects the numbers during broker-chaos, which is exactly when it happens. | |
| **D10** | Hardcoded `kafka-1/2/3` broker names and `replication_factor <= 3`. Rejects any external cluster. | Python's values kept | `internal/scenario/validate.go:42` | Reassign `Cluster` to relax. Blocks your 24/7 multi-cluster use until you do. | |
| **D13** | Flow instances in flight at once. Python is unbounded and untracked. | **0 = unbounded — matches Python** | `internal/engine/flow.go:35` | Set a positive value to bound the pool; issuance then blocks at saturation and shows as `Saturated` in the snapshot. | |

## Python bugs reproduced on purpose — flip if you'd rather have the fix

You said behaviour must match Python, so each of these reproduces a Python defect. Every one
is a single flip, and each site carries a comment explaining both sides.

| # | Python's behaviour, now reproduced | Flip it at | You |
|---|---|---|---|
| **D5** | `target.indices` and `target.group_id` are parsed and then **ignored**, so `targeted-incidents.yaml` hits *every* consumer instead of the ones it names. | restore the `Indices` filter in `selectConsumers`, `internal/scenario/incident.go` | |
| **D11** | `change_producer_rate` is a **no-op** — three shipped chaos scenarios silently do nothing. | `LiveProducerRateChanges = true`, `internal/engine/producer.go:137` | |
| **D16** | A consumer recreated by `rebalance_consumer` **loses** `failure_rate`/`on_failure`/`max_retries` and reverts to defaults, so a rebalance-storm quietly stops testing failures partway through. The engine agent flagged this as the one worth reconsidering: unlike the others it is **invisible** — nothing in the output says the replacement lost its config. | carry `victim.Conf` into `CreateConsumer`, `internal/scenario/incident.go` | |
| **D17** | An impossible cardinality (`min: 0, max: 5, cardinality: 100`) **hangs forever** with no output. | pass `generate.BoundFillAttempts(n)`, `internal/generate/field.go` | |

## Implemented as recommended — say if you disagree

| # | Question | What I did | Where | You |
|---|---|---|---|---|
| **D7** | `--ssl-key-password` (encrypted keys). Go stdlib can't read PKCS#8-encrypted keys. | Rejected up front with an error naming the flag and the `openssl pkcs8 -nocrypt` fix. | `internal/kafka/security.go` | |

## Not done — need your answer before I build them

| # | Question | Status | You |
|---|---|---|---|
| ~~**D3**~~ | ~~Lag is Python's fake `produced − consumed`.~~ | **DONE, opt-in.** `--lag-poll 5s` asks the brokers for real committed-offset-vs-end-offset lag on a slow independent ticker. Default 0 = off, so the column is unchanged unless you ask. Shows **both** columns (`LAG(SELF)` / `LAG(BROKER)`) rather than silently swapping one for the other. A denied DESCRIBE, a timeout, or a partial answer renders **`unknown`** — never a zero — is logged once rather than every tick, and never fails the run. Headless logs get `broker_lag` too. | |
| **D6** | Faker → gofakeit. Names differ (`street_address`→`street`, `postcode`→`zip`, `country_code`→`countryabr`), `date_this_month` has no equivalent, **locale is gone entirely**. | Alias table built for the ~20 providers your scenarios and README use. Unmapped names error clearly. `locale:` is accepted and ignored. **User scenario YAML can break here.** | |
| **D8** | Avro library. `hamba/avro v2.31.0` is archived with a reported unpatched CVE; `goavro` is in maintenance mode. | Using hamba, pinned. Needs an applicability check — if the CVE is in the decode path and khaos only encodes schemas it generated, exposure may be nil. | |
| ~~**D12**~~ | ~~Schema Registry auth impossible — blocked Confluent Cloud and Aiven.~~ | **DONE.** `--schema-registry-username/-password/-token/-ca-location/-cert-location/-key-location`. Credentials are exercised by the startup probe, so a bad one fails immediately rather than on first produce. A 401/403 now names the URL and the likely wrong flag, including that **the SR API key is not the Kafka API key** — the most common Confluent Cloud mistake. Basic+bearer together, half a credential pair, and TLS files against a non-`https` URL are all rejected up front. | |
| ~~**D14**~~ | ~~`--skip-topic-creation` did not skip topic creation.~~ | **FIXED.** `engine.Config.SkipTopicCreation` now short-circuits `setupTopics`, so no topic admin call is issued at all — matching Python, which never invokes TopicManager when the flag is set. Two tests: one proves the topic is absent with the flag, the paired one proves it IS created without, so the first cannot pass vacuously. | |
| ~~**D15**~~ | ~~`cluster-down` always removed data volumes.~~ | **FIXED.** `Down(ctx, removeVolumes bool)`; `--volumes`/`-v` controls it, default false as in Python. Note this changes nothing observable today: **none of the four compose files declares a volume** — the brokers keep their logs at `KAFKA_LOG_DIRS=/tmp/kraft-combined-logs` inside the container layer, which `compose down` discards either way. So the earlier "silent data loss" framing was wrong. Fixed so the flag stops lying and stays correct if a volume is ever added. | |

---

## Also needs you

- Nothing blocking. The local cluster, broker faults, and Avro/Protobuf against a live
  Schema Registry were all verified end-to-end against real Docker, including Java
  consumers reading Go-produced records. Python is deleted.
- **README**: the CLI Reference and RUNBOOK command lists are now reconciled against the
  real `--help` output. The rest of the README (scenario authoring, field types, flows)
  still describes the Python behaviour and has not been re-checked against the Go
  implementation.
