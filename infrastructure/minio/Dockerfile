# syntax=docker/dockerfile:1.7

FROM golang:1.24.8-alpine3.22 AS build

ARG MINIO_VERSION=RELEASE.2025-10-15T17-29-55Z
ARG MC_VERSION=RELEASE.2025-08-13T08-35-41Z

RUN CGO_ENABLED=0 go install github.com/minio/minio@${MINIO_VERSION} \
    && CGO_ENABLED=0 go install github.com/minio/mc@${MC_VERSION}

FROM alpine:3.22.1

RUN apk add --no-cache ca-certificates tzdata \
    && addgroup -g 10002 minio \
    && adduser -D -u 10002 -G minio minio \
    && mkdir -p /data /home/minio/.mc \
    && chown -R minio:minio /data /home/minio

COPY --from=build /go/bin/minio /usr/bin/minio
COPY --from=build /go/bin/mc /usr/bin/mc

USER minio
VOLUME ["/data"]
EXPOSE 9000 9001
ENTRYPOINT ["/usr/bin/minio"]
CMD ["server", "/data", "--console-address", ":9001"]
