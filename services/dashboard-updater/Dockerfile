# syntax=docker/dockerfile:1
FROM golang:1.26-alpine AS builder
WORKDIR /src
COPY go.mod ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/dashboard-updater .

FROM alpine:3.22
COPY --from=builder /out/dashboard-updater /dashboard-updater
ENTRYPOINT ["/dashboard-updater"]
