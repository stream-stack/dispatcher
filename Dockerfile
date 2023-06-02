FROM golang:1.20.4 as plugin

ENV GOPROXY="https://goproxy.cn,direct"
WORKDIR /workspace
COPY plugins/cloudevents/ /workspace/
RUN unset HTTPS_PROXY;unset HTTP_PROXY;unset http_proxy;unset https_proxy; go mod tidy -v && \
    go build -buildmode=plugin -o cloudevents.so main.go

FROM golang:1.20.4 as builder

ENV GOPROXY="https://goproxy.cn,direct"
WORKDIR /workspace
COPY . /workspace/
RUN unset HTTPS_PROXY;unset HTTP_PROXY;unset http_proxy;unset https_proxy;go mod download
RUN unset HTTPS_PROXY;unset HTTP_PROXY;unset http_proxy;unset https_proxy;go get github.com/stream-stack/common
RUN unset HTTPS_PROXY;unset HTTP_PROXY;unset http_proxy;unset https_proxy;go build -o dispatcher cmd/main.go

FROM ubuntu:latest
WORKDIR /
COPY --from=builder /workspace/dispatcher .
RUN mkdir -p ./plugins/cloudevents
COPY --from=plugin /workspace/cloudevents.so ./plugins/cloudevents/cloudevents.so

CMD ["./dispatcher"]
