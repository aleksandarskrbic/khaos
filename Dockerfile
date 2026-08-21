# khaos is a pure-Go static binary, so the runtime image needs nothing but the binary
# itself. That is the whole payoff of choosing franz-go over the cgo-based
# confluent-kafka-go: CGO_ENABLED=0 with no C toolchain, and a scratch final image.

FROM golang:1.26-alpine AS build
WORKDIR /src

# Dependencies first so the module layer caches independently of source edits.
COPY go.mod go.sum ./
RUN go mod download

COPY . .
ARG VERSION=dev
RUN CGO_ENABLED=0 go build \
      -trimpath \
      -ldflags "-s -w -X main.version=${VERSION}" \
      -o /khaos ./cmd/khaos

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /khaos /khaos

# Scenarios and the compose files are embedded in the binary, so no assets are copied.
# Mount your own scenario with -v and pass its path, or use a bundled name.
USER nonroot:nonroot
ENTRYPOINT ["/khaos"]
