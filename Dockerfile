FROM alpine:3.2

RUN apk update
RUN mkdir -p /etc/
RUN mkdir -p /config/

COPY ./edgefs-csi /

ENTRYPOINT ["/edgefs-csi"]
