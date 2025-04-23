# FROM alpine:3.21
FROM rust:alpine

RUN apk update && apk upgrade
RUN apk add --no-cache ca-certificates gcc build-base curl perl nodejs npm git bash cmake pkgconf python3 linux-headers

WORKDIR /build
ENV SQLX_OFFLINE=true
ENV RUST_BACKTRACE=full
