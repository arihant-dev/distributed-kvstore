FROM golang:1.25-alpine AS builder
WORKDIR /app
COPY go.mod ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /traffic-sim .

FROM alpine:latest
WORKDIR /app
COPY --from=builder /traffic-sim /traffic-sim
ENTRYPOINT ["/traffic-sim"]
