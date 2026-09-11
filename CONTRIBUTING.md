# Contributing to khaos

Thanks for your interest in contributing!

## Development setup

Requires Go 1.26 or newer (`go.mod` declares 1.26.5).

```bash
git clone https://github.com/aleksandarskrbic/khaos.git
cd khaos
go build -o khaos ./cmd/khaos
./khaos --help
```

The test suite needs no Docker. It runs against
[kfake](https://pkg.go.dev/github.com/twmb/franz-go/pkg/kfake), an in-process broker that
speaks the real Kafka protocol, so the engine, the clients and the CLI are all exercised
without a container. Docker is only needed to drive the bundled cluster by hand --
`khaos cluster-up`, or `khaos run` against it -- while trying a change out end to end.

## Before submitting a PR

```bash
./scripts/check.sh      # gofmt, go vet, go test, static build + `khaos --version`
./scripts/check.sh -r   # the same with -race, which is what CI runs
```

`check.sh` is the local equivalent of the `test` job in `.github/workflows/ci.yml`:

- `gofmt -l cmd internal` must print nothing.
- `go vet ./...`
- `go test ./...` -- `-race -timeout 15m` under `-r` and in CI, `-timeout 10m` otherwise.
- `CGO_ENABLED=0 go build ./cmd/khaos`, then run the result. The static binary is the
  product, so it gets built the way it ships. CI goes one step further and smoke-tests it
  with `--version`, `list` and `validate traffic/high-throughput`.

CI runs two jobs `check.sh` does not:

- **cross-compile** builds linux/amd64, linux/arm64, darwin/amd64, darwin/arm64 and
  windows/amd64. Anything that drags in cgo fails here first.
- **govulncheck** is advisory. It reports vulnerabilities in required modules even when the
  calling code path is unreachable, so it never fails the build on its own.

Pull requests touching `website/` get their own workflow, `website-ci.yml`: `npm ci`,
`npm run lint`, `npm run build` and an `opennextjs-cloudflare build`, all from `website/` on
Node 22. Merging them to `main` runs `deploy-website.yml`, which deploys the site.

## Code style

- Run `gofmt` before committing; `./scripts/check.sh` catches anything unformatted.
- Match the existing package structure (see the Architecture section in `README.md`):
  `internal/engine` stays independent of any UI (`internal/tui`), `internal/kafka` is the
  only package that constructs Kafka (`kgo`/`kadm`) clients -- `internal/codec` owns the
  Schema Registry client -- and `internal/scenario` depends on nothing else in the repo.
- Comments say why, not what. The dash used throughout the codebase is ` -- `, not an em
  dash.
- Tests are required for new features.
- Commit subjects follow conventional commits. `scripts/release.sh` drafts the CHANGELOG
  entry from `feat:`, `fix:`, `perf:`, `refactor:` and `docs:` subjects since the last tag
  and skips everything else, so a user-visible change described as `chore:` silently misses
  the release notes.

## Adding a scenario

Scenario YAML lives in `scenarios/<category>/<name>.yaml`, and the binary embeds a copy
under `internal/scenario/bundled/`. Add the file in both places, byte for byte:
`TestBundledCopyMatchesRepo` fails the moment the two drift, and `TestShippedScenariosValidate`
and `TestBundledScenariosAreGeneratable` check that everything shipped both validates and
actually generates data.

`khaos list` and `khaos validate` pick the new file up with no code change. A new category
directory also wants an entry in `categoryOrder` and `categoryInfo` in
`cmd/khaos/scenarios.go`, or it sorts last in `khaos list` under a title-cased directory
name with no description.

## Pull request process

1. Fork the repo and branch from `main`.
2. Run `./scripts/check.sh -r` and make sure it passes.
3. Update the docs if behaviour changed. User-facing documentation is
   `website/content/docs/`, and the flag tables live in
   `website/content/docs/reference/cli.mdx`.
4. Open the PR with a clear description of what changed and why.

One other file is worth knowing about: `RUNBOOK.md` covers building, releasing and driving a
cluster by hand.

The deliberate behavioural choices -- which partitioner, which rebalance protocol, whether
topics are recreated, what a producer does when its buffer fills -- are documented at the
symbol that sets each one, so the explanation cannot drift from the value. Most are in
`internal/kafka/policy.go` (`Partitioner`, `Balancers`, `RecreateTopicsByDefault`,
`BlockOnBufferFull`); the other two are `scenario.ClusterAssumptions` in
`internal/scenario/validate.go` and `engine.DefaultFlowConcurrency` in
`internal/engine/flow.go`. Each doc comment gives the current value, why it was chosen, and
what changes if you flip it.
