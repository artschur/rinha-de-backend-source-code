# Multi-stage build for smaller image
FROM golang:1.24-alpine AS builder

WORKDIR /app

# Copy go mod files
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build with optimizations for Intel i7-6700 (Skylake) - GOAMD64=v3
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 GOAMD64=v3 go build \
    -ldflags="-w -s" \
    -gcflags="-B" \
    -a -installsuffix cgo \
    -tags netgo,osusergo \
    -o main .

# Final stage - minimal image
FROM alpine:latest

RUN apk --no-cache add ca-certificates

WORKDIR /root/

# Copy the binary
COPY --from=builder /app/main .

# Use non-root user for security
RUN adduser -D -s /bin/sh appuser
USER appuser

EXPOSE 8080

CMD ["./main"]
