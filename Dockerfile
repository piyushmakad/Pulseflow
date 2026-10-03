# syntax=docker/dockerfile:1

# Build one static binary for either linux/amd64 or linux/arm64. BuildKit sets
# TARGETOS and TARGETARCH when docker buildx builds a multi-platform image.
FROM golang:1.24.5-alpine AS builder

ARG TARGETOS=linux
ARG TARGETARCH=amd64

WORKDIR /app

COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download

COPY . .
RUN --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -trimpath -ldflags="-s -w" -o /app/bin/pulseflow ./cmd/pulseflow

# Runtime stage: the application does not need a compiler or package manager.
FROM alpine:3.21

RUN apk --no-cache add ca-certificates tzdata \
    && addgroup -S -g 10001 pulseflow \
    && adduser -S -D -H -u 10001 -G pulseflow pulseflow

WORKDIR /app

COPY --from=builder --chown=pulseflow:pulseflow /app/bin/pulseflow /app/pulseflow

EXPOSE 8080
EXPOSE 9090

USER 10001:10001

STOPSIGNAL SIGTERM

ENTRYPOINT ["./pulseflow"]
CMD ["--mode=all"]
