FROM golang:1.17-alpine

WORKDIR /app

COPY go.mod ./
COPY go.sum ./
RUN go mod download

COPY ingest-service.go ./

ENV GIN_MODE=release
RUN go build -o ./ingest-service

EXPOSE 8080

CMD [ "./ingest-service" ]
