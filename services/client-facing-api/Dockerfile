# syntax=docker/dockerfile:1
FROM golang:1.23-alpine AS builder
WORKDIR /src
COPY go.mod ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/client-facing-api .

FROM gcr.io/distroless/static-debian12
COPY --from=builder /out/client-facing-api /client-facing-api
ENTRYPOINT ["/client-facing-api"]
