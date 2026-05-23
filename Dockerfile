FROM golang:1.25-bookworm AS builder

ARG BUILD_VERSION=dev

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
    -ldflags="-X github.com/transmogr/transmogr/internal/app.Version=${BUILD_VERSION}" \
    -o /out/transmogr ./cmd/transmogr
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /out/transmogr-init ./cmd/init

FROM debian:bookworm-slim

RUN addgroup --system --gid 65532 nonroot && \
    adduser --system --uid 65532 --ingroup nonroot --no-create-home nonroot

WORKDIR /app

COPY --from=builder /out/transmogr /usr/local/bin/transmogr
COPY --from=builder /out/transmogr-init /usr/local/bin/transmogr-init

USER nonroot

CMD ["/usr/local/bin/transmogr"]
