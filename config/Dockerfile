FROM golang:1.17-alpine as builder
WORKDIR /app
COPY go.mod ./
COPY go.sum ./
COPY config-service.go ./
RUN go mod download -x
RUN go build -o ./config-service

FROM golang:1.17-alpine as runner
WORKDIR /app
COPY --from=builder /app/config-service ./config-service
EXPOSE 80
CMD [ "./config-service" ]
