# Handoff: replace hamba/avro with goavro

Not started yet — this session got interrupted before writing any code. Read this top to
bottom before touching `internal/codec`.

## The ask

Swap the Avro backend in `internal/codec` from `github.com/hamba/avro/v2` to
`github.com/linkedin/goavro/v2`. `DECISIONS.md` D8 already flagged the reason: hamba/avro
is archived upstream with a reported unpatched CVE. goavro is confirmed reachable —
`go list -m -versions github.com/linkedin/goavro/v2` returned versions up to **v2.15.0**
over the proxy in this environment, so `go get github.com/linkedin/goavro/v2@v2.15.0` should
work. That `go get` was not run yet (interrupted) — go.mod/go.sum still show only hamba.

This is unrelated to the engine-bugs work done earlier in the same session (metrics wiring,
`target.indices`, `change_producer_rate` — see `git status` / `git diff`, all uncommitted
and already reviewed-pending). Don't conflate the two; this is a fresh, separate task.

## Why this is not a mechanical find-and-replace

hamba/avro and goavro have genuinely different APIs, and the current code leans hard on
hamba's object model. Read `internal/codec/avro.go`, `avro_coerce.go`, and the relevant
bits of `codec.go` in full before starting.

**hamba/avro** (current):
- `avro.ParseWithCache(text, ns, *avro.SchemaCache)` returns a `avro.Schema` — a real object
  model: `*avro.RecordSchema` (`.Fields()`, each with `.Name()`/`.Type()`), `*avro.ArraySchema`
  (`.Items()`), `*avro.MapSchema` (`.Values()`), `*avro.UnionSchema` (`.Types()`),
  `*avro.RefSchema` (`.Schema()`, unwraps a second reference to a named type).
- `avro.Marshal(schema, nativeGoValue)` / `avro.Unmarshal(schema, bytes, &out)` work directly
  against plain Go maps (`map[string]any`) and auto-unwrap/auto-wrap unions (a nullable
  field is just given its bare value or `nil`, never a wrapped union map).
- Decodes `timestamp-millis` to `time.Time` and `time-millis` to `time.Duration`
  automatically — `avroScalar()` in `avro.go` converts these back to plain epoch-millis
  `int64` because khaos always generates/round-trips integers, never `time.Time`.
- `coerceAvro()` in `avro_coerce.go` walks the schema alongside the value converting Go
  types hamba is strict about (int64→int for `int`, float64→float32 for `float`, string→
  []byte for `bytes`), and — this is the part that will need to invert — **unwraps unions**:
  for `*avro.UnionSchema` it just recurses into the first non-null branch, because hamba's
  `Marshal` doesn't want anything union-shaped on the way in.
- `parseAvro()` uses a *fresh* `avro.SchemaCache` per call specifically so that one topic's
  generated schema can never resolve a named-type reference against another topic's cache
  entry (`TestAvroParseDoesNotShareNamesBetweenSchemas` pins this — hamba's default parse
  path publishes into a shared package-level cache, which is the bug being dodged).

**goavro/v2** (target — verify the exact API against the actual fetched module, this is
from general knowledge, not re-checked against v2.15.0 docs in this session):
- `goavro.NewCodec(schemaJSON string) (*goavro.Codec, error)` — no per-parse cache knob, and
  I don't think it has hamba's shared-global-cache problem in the first place (no evidence
  goavro publishes named types outside one schema document), but **confirm this** — if it
  does leak names across `NewCodec` calls the same isolation problem needs solving.
- `codec.BinaryFromNative(buf []byte, native interface{}) ([]byte, error)` and
  `codec.NativeFromBinary(buf []byte) (interface{}, []byte, error)` — the encode/decode
  pair. Note `NativeFromBinary` returns `(interface{}, []byte remaining, error)`, not
  `(value, error)`.
- **Unions are NOT auto-wrapped/unwrapped.** goavro represents an Avro union value as a Go
  `map[string]interface{}` with exactly one key: the branch's type name (e.g. a
  `["null","string"]` field holding "hi" must be handed to `BinaryFromNative` as
  `map[string]interface{}{"string": "hi"}`, and a null value as literal `nil`). Every
  place `coerceAvro` currently *unwraps* a union will need to instead *wrap* it for encode,
  and `avroDecodedValue`/`avroScalar` will need to *unwrap* the `{branchName: value}` map
  goavro hands back on decode. Get the branch-name-to-Avro-type mapping right (it's the
  schema's own type names — `"string"`, `"long"`, a record's `name`, etc.) or every optional
  field breaks.
- **No object schema model exposed the same way.** goavro doesn't hand back a walkable
  `RecordSchema`/`ArraySchema`/`MapSchema` tree — `avroRecordDoc()`'s trick of walking
  `rec.Fields()` in schema order to rebuild a Doc with stable key order won't have that to
  lean on. `avroSchemaJSON()` (already in `avro.go`, used for the registry conversion path)
  parses the schema text into a generic `map[string]any` — that's the fallback to get field
  order back if goavro's package doesn't expose one; check whether goavro's codec object
  exposes anything like a parsed schema tree first (there's some `CodecForStandardJSONFull`
  variant and possibly a `.Schema()` that only returns the *text* back, not a tree) before
  hand-rolling JSON-schema walking.
- Logical type handling (`uuid`, `timestamp-millis`) may or may not be native in whatever
  goavro version lands — verify with a quick round-trip test before assuming `avroScalar`'s
  `time.Time`/`time.Duration` conversion logic is still needed at all. It's plausible goavro
  just hands back a plain `int64` for `timestamp-millis` with no logical-type coercion,
  which would *simplify* `avroScalar`, not complicate it — check before porting that
  function over unchanged.

## Files touched

- `internal/codec/avro.go` — `avroCodec.Encode`/`Decode`, `parseAvro`, `avroRecordDoc`,
  `avroDecodedValue`, `avroScalar`, `derefAvro`. `AvroSchemaText`/`avroFieldEntries`/
  `avroTypeOf` (schema *generation*, not parsing) are format-agnostic and should need **no
  changes** — they just build the JSON schema text via `codec.Doc`, independent of which
  library parses it back.
- `internal/codec/avro_coerce.go` — `coerceAvro` needs a real rewrite (union wrap direction
  flips, per above), not a search-and-replace of type names.
- `internal/codec/codec.go` — `newAvroCodec()` currently type-asserts
  `parsed.(*avro.RecordSchema)` to build `avroCodec{schema, record}`; whatever replaces
  `record` (a wrapper carrying goavro's codec + a field-order list, most likely) gets built
  here.
- `internal/codec/avro_test.go` — imports `github.com/hamba/avro/v2` directly in two tests
  (`TestAvroRoundTrip`, `TestAvroTimestampNormalisation` both do
  `schema.(*avro.RecordSchema)`) — these need to build whatever the new `avroCodec` struct
  shape is instead. **The test *behavior* they assert must not change**: exact schema JSON
  text (`TestAvroSchemaText`), full round-trip identity including nested record/array/map/
  enum/uuid/timestamp *and doc key order* (`TestAvroRoundTrip`), schema-cache isolation
  across topics (`TestAvroParseDoesNotShareNamesBetweenSchemas` — port the intent even if
  goavro's isolation story turns out to be different), timestamp epoch-millis round-trip
  (`TestAvroTimestampNormalisation`).
- Check `internal/codec/registry.go` and `internal/codec/convert.go` for any other direct
  `avro.` references (a stale grep from this session found matches only in avro.go/
  avro_coerce.go/codec.go, but re-grep after starting — state may have shifted).
- `go.mod`/`go.sum`: remove `github.com/hamba/avro/v2`, add `github.com/linkedin/goavro/v2`.

## Why this needs real care, not a quick swap

This is wire-format code. `HANDOFF.md` records that Avro-through-a-live-Schema-Registry was
verified end to end against the **Java** `kafka-avro-console-consumer` — a subtle behavior
drift here (wrong union wrapping, lost field order, a logical type silently mishandled)
would not show up as a compile error or even necessarily a failing unit test if the test
itself gets ported sloppily; it would show up as bytes a real Avro reader chokes on or
silently misinterprets. Budget time for:
1. Writing the new `avroCodec`/`coerceAvro` carefully, matching goavro's actual native-value
   conventions (verify against real `go doc` output / a scratch program, not memory).
2. Porting every existing avro test with the same assertions, not weaker ones.
3. If at all possible, an actual round trip through a running Schema Registry + a real Avro
   reader (Java console consumer, or at minimum `avro-tools` on the CLI) — not just Go
   round-trip tests, since the whole point of switching libraries is not to introduce a new
   interop bug while fixing the CVE/archived-library problem.
4. Once green: update `DECISIONS.md` D8 to reflect it's done (same pattern as D5/D11/D14/D15
   in that file — strike through, note what changed, leave the "You" column for the user
   rather than filling in "ok" yourself).

## Repo state as of this handoff

- `go.mod`/`go.sum`: unchanged, still hamba/avro only. No `go get` was run.
- No code changes made for this task at all.
- Unrelated to this: earlier in the session, three engine bugs were fixed (dead `/metrics`,
  `target.indices` ignored, `change_producer_rate` no-op) — all uncommitted, all tested,
  waiting on the user's review. Don't touch those files as part of this task unless asked.
