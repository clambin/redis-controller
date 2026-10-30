FROM --platform=${BUILDPLATFORM:-linux/amd64} golang:1.27 AS builder

ARG TARGETPLATFORM
ARG BUILDPLATFORM
ARG TARGETOS
ARG TARGETARCH
ARG VERSION
ENV VERSION=$VERSION

WORKDIR /app/
ADD . .
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build \
    -ldflags="-X main.version=$VERSION" \
    -o redis-controller \
    cmd/redis-controller/main.go

FROM alpine

RUN apk add --no-cache tzdata

WORKDIR /app
COPY --from=builder /app/redis-controller /app/redis-controller

RUN /usr/sbin/addgroup app
RUN /usr/sbin/adduser app -G app -D
USER app

ENTRYPOINT ["/app/redis-controller"]
CMD []
