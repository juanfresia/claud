FROM golang:1.11

ADD . /claud/

ENTRYPOINT ["/bin/sh", "-c"]
