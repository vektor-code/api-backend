# --- Stage 1: Build ---
FROM golang:1.25-alpine AS builder

WORKDIR /app

# Copy dependency files
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build statically linked binary with symbols intact for eBPF auto-instrumentation
RUN CGO_ENABLED=0 GOOS=linux go build -o server ./cmd/server

# --- Stage 2: Final image ---
FROM alpine:3.19

# Install CA certificates for secure connections (MinIO, etc)
RUN apk --no-cache add ca-certificates

WORKDIR /app

# Run as non-root user for security
RUN addgroup -S appgroup && adduser -S appuser -G appgroup
USER appuser

COPY --from=builder /app/server .

EXPOSE 8080

ENTRYPOINT ["./server"]
