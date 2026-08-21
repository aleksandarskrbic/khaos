# Contributing to khaos

Thanks for your interest in contributing!

## Development Setup

Requires Go 1.26+.

```bash
git clone https://github.com/aleksandarskrbic/khaos.git
cd khaos
go build -o khaos ./cmd/khaos
./khaos --help
```

No Docker required to work on khaos itself — tests run against
[kfake](https://pkg.go.dev/github.com/twmb/franz-go/pkg/kfake), an in-process broker that speaks
the real Kafka protocol. Docker is only needed if you want to run `khaos run` against the bundled
local cluster manually while testing changes.

## Before Submitting a PR

Run all checks:

```bash
./scripts/check.sh

# add -r to also run the race detector (what CI runs)
./scripts/check.sh -r
```

This runs:
- `gofmt` — formatting
- `go vet` — static analysis
- `go test` — the test suite
- a static build (`CGO_ENABLED=0`) plus a smoke check (`khaos --version`)

## Code Style

- Run `gofmt` before committing; `./scripts/check.sh` will catch anything unformatted.
- Match the existing package structure (see the Architecture section in `README.md`) — the
  scenario engine (`internal/engine`) stays independent of any UI (`internal/tui`), and
  `internal/kafka` stays the only package that talks to franz-go directly.
- Tests are required for new features.

## Pull Request Process

1. Fork the repo and create your branch from `main`.
2. Run `./scripts/check.sh -r` and ensure all checks pass.
3. Update documentation if needed — the website source lives in `website/content/docs/`.
4. Submit PR with a clear description of changes.
