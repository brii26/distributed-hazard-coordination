# syntax=docker/dockerfile:1
FROM golang:1.26-alpine AS builder
WORKDIR /src
COPY go.mod ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/pvmbg-mock .

FROM gcr.io/distroless/static-debian12
COPY --from=builder /out/pvmbg-mock /pvmbg-mock
ENTRYPOINT ["/pvmbg-mock"]
