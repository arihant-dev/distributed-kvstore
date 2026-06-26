FROM golang:1.25-alpine AS builder

WORKDIR /app

# Copy mod files and download dependencies
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build the binary
RUN CGO_ENABLED=0 GOOS=linux go build -o /kvnode ./cmd/kvnode/main.go

# Use a scratch image for the final container
FROM alpine:latest
WORKDIR /app
COPY --from=builder /kvnode /kvnode

# Start the node
ENTRYPOINT ["/kvnode"]
