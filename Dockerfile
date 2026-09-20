# Build stage
FROM golang:1.23-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /app/bin/pulseflow ./cmd/pulseflow

# Runtime stage
FROM alpine:3.19

RUN apk --no-cache add ca-certificates tzdata

WORKDIR /app

COPY --from=builder /app/bin/pulseflow .
COPY --from=builder /app/migrations ./migrations

EXPOSE 8080

ENTRYPOINT ["./pulseflow"]
CMD ["--mode=all"]
