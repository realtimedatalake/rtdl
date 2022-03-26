FROM golang:1.17-alpine as builder
WORKDIR /app
ENV GIN_MODE=release
COPY go.mod ./
COPY go.sum ./
COPY ingest-service.go ./
RUN go mod download -x
RUN go build -o ./ingest-service

FROM golang:1.17-alpine as runner
WORKDIR /app
ENV GIN_MODE=release
COPY --from=builder /app/ingest-service ./ingest-service
EXPOSE 8080
CMD [ "./ingest-service" ]
