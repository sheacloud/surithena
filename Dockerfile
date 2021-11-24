FROM golang:1.17-alpine as build

COPY . /build
RUN cd /build; CGO_ENABLED=0 GOBIN=/bin/ go build -o /bin/eve-processor ./cmd/eve-processor;

FROM alpine:latest as certs
RUN apk --update add ca-certificates

FROM scratch as prod

ENV PATH=/bin

COPY --from=build /bin/eve-processor /bin/eve-processor
COPY --from=certs /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt

VOLUME /tmp

CMD ["/bin/eve-processor"]