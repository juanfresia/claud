from golang:1.11

RUN mkdir -p /claud/
RUN mkdir -p /go/src/github.com/juanfresia
RUN ln -s /claud /go/src/github.com/juanfresia/claud
ADD . /claud/

RUN cd /claud && \
    make

ENTRYPOINT ["/bin/sh", "-c"]
