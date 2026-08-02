# Self-contained build: the build context is THIS directory. The shared code
# lives in ./shared and is wired in via `replace github.com/kubetrace/shared => ./shared`,
# so no sibling module is needed:
#   docker build -f Dockerfile .
#
# --- Stage 1: Build ---
FROM golang:1.25-alpine AS builder

# Module mode, not workspace mode.
ENV GOWORK=off

WORKDIR /src

# Dependency manifests first, so the module cache layer survives source edits.
COPY shared/go.mod ./shared/
COPY go.mod go.sum ./
RUN go mod download

# Source code (service + vendored shared package)
COPY . .

# Build statically linked binary with symbols intact for eBPF auto-instrumentation
RUN CGO_ENABLED=0 GOOS=linux go build -o /out/server ./cmd/server

# --- Stage 2: Final image ---
FROM alpine:3.19

# Install CA certificates for secure connections (MinIO, etc)
RUN apk --no-cache add ca-certificates

WORKDIR /app

# Run as non-root user for security
RUN addgroup -S appgroup && adduser -S appuser -G appgroup
USER appuser

COPY --from=builder /out/server .

EXPOSE 8080

ENTRYPOINT ["./server"]
