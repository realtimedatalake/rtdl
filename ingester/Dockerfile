FROM golang:1.16-alpine
WORKDIR /app
COPY go.mod ./
COPY go.sum ./
COPY ingester.go ./
RUN go mod download -x
RUN go build -o ./ingester
EXPOSE 8082
CMD [ "/ingester" ]
