FROM golang:1.25-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -o /chaos-agent .

FROM alpine:latest

# Install iproute2 so the agent can exec `tc` into containers if needed
RUN apk add --no-cache iproute2

WORKDIR /app
COPY --from=builder /chaos-agent /chaos-agent

EXPOSE 9090

ENTRYPOINT ["/chaos-agent"]
