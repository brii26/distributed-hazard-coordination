# syntax=docker/dockerfile:1
FROM golang:1.26-alpine AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/situation-summarizer . && CGO_ENABLED=0 go build -o /out/fakellm ./cmd/fakellm

FROM alpine:3.22
COPY --from=builder /out/situation-summarizer /situation-summarizer
COPY --from=builder /out/fakellm /fakellm
ENTRYPOINT ["/situation-summarizer"]
