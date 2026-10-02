# syntax=docker/dockerfile:1
FROM golang:1.23-alpine AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/auth-service .

FROM gcr.io/distroless/static-debian12
COPY --from=builder /out/auth-service /auth-service
ENTRYPOINT ["/auth-service"]
