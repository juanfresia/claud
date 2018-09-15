FROM golang:1.11

ADD . /claud/

RUN cd /claud && \
    make

ENTRYPOINT ["/bin/sh", "-c"]
