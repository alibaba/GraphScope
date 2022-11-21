# The vineyard-runtime image including all vineyard-related
# dependencies that could graphscope interactive engine.

ARG BASE_VERSION=v0.10.2
FROM registry.cn-hongkong.aliyuncs.com/graphscope/vineyard-dev:$BASE_VERSION AS builder

WORKDIR /root

RUN mkdir artifacts && \
    cd artifacts && \
    mkdir bin lib lib64 && \
    cp /usr/local/bin/vineyard-graph-loader bin/ && \
    cp -P /usr/local/lib/lib* lib/ && \
    cp -P /usr/local/lib64/lib* lib64/ && \
    rm -f lib/libgrpc* && \
    tar czf artifacts.tar.gz ./*

FROM centos:7 AS runtime
COPY --from=builder /root/artifacts/artifacts.tar.gz /root/artifacts.tar.gz

RUN tar xzf /root/artifacts.tar.gz -C /usr/local/

ENV LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/local/lib:/usr/local/lib64

RUN yum install -y libunwind-devel && \
    yum clean all -y --enablerepo='*' && \
    rm -rf /var/cache/yum

RUN useradd -m graphscope -u 1001 \
    && echo 'graphscope ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers
USER graphscope
WORKDIR /home/graphscope