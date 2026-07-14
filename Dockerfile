FROM golang:1.26-alpine AS builder

WORKDIR /app

# Download dependencies (cached layer)
COPY go.mod go.sum ./
RUN go mod download

# Copy source and build
COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build -ldflags="-s -w" -o /bin/nxt-msa-notifications ./cmd/server

# ─── Runtime image ───
FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=builder /bin/nxt-msa-notifications /nxt-msa-notifications

EXPOSE 8085

ENTRYPOINT ["/nxt-msa-notifications"]
