# BUILD
FROM golang:1.14-alpine as build

ARG SERVICE=broadcaster

ENV GO111MODULE=on

WORKDIR /build

COPY go.mod go.mod ./
RUN go mod download

COPY . .


RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o bin/service cmd/$SERVICE/main.go

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o bin/migrate db/migrations/migrate.go

# RUN
FROM alpine
WORKDIR /app

COPY --from=build /build/bin/* /app/

COPY ./db/migrations/* ./db/migrations/

ENV GIN_MODE=release
CMD ["/app/service"]
