#!/usr/bin/env bash
# Local equivalent of the CI job. No Docker required: the tests use kfake, an in-process
# broker speaking the real Kafka protocol.
set -euo pipefail

echo "==> gofmt"
unformatted=$(gofmt -l cmd internal)
if [ -n "$unformatted" ]; then
    echo "not gofmt'd:"
    echo "$unformatted"
    exit 1
fi

echo "==> vet"
go vet ./...

echo "==> test"
if [ "${1:-}" = "-r" ]; then
    go test -race -timeout 15m ./...
else
    go test -timeout 10m ./...
fi

echo "==> build"
CGO_ENABLED=0 go build -trimpath -o /tmp/khaos ./cmd/khaos
/tmp/khaos --version

echo "All checks passed."
