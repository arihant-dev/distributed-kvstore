FROM golang:1.25-alpine AS builder
WORKDIR /app
COPY go.mod ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /orchestrator .

FROM alpine:latest
WORKDIR /app
COPY --from=builder /orchestrator /orchestrator
ENTRYPOINT ["/orchestrator"]
