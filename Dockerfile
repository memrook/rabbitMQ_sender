# syntax=docker/dockerfile:1

FROM golang:1.18-alpine

WORKDIR /app

COPY go.mod ./
COPY go.sum ./
COPY config.json ./

RUN go mod download

COPY . ./

RUN go build -o /senderMQ

EXPOSE 8082

CMD ["/senderMQ"]


