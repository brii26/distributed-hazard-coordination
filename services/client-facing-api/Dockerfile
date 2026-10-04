# syntax=docker/dockerfile:1
FROM golang:1.26-alpine AS builder
WORKDIR /src
COPY go.mod ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/client-facing-api .

FROM alpine:3.22
COPY --from=builder /out/client-facing-api /client-facing-api
ENTRYPOINT ["/client-facing-api"]
