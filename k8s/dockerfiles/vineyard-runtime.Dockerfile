# The vineyard-runtime image including all vineyard-related
# dependencies that could graphscope interactive engine.

ARG REGISTRY=registry.cn-hongkong.aliyuncs.com
ARG BUILDER_VERSION=latest
FROM $REGISTRY/graphscope/vineyard-dev:$BUILDER_VERSION AS builder

USER root
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
COPY --from=builder /opt/openmpi /opt/openmpi
COPY --from=builder /opt/vineyard /opt/vineyard
RUN tar xzf /root/artifacts.tar.gz -C /usr/local/ && rm /root/artifacts.tar.gz

ENV LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/local/lib:/usr/local/lib64

RUN yum install -y sudo libunwind-devel && \
    yum clean all -y --enablerepo='*' && \
    rm -rf /var/cache/yum

RUN useradd -m graphscope -u 1001 \
    && echo 'graphscope ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers
USER graphscope
WORKDIR /home/graphscope

RUN sudo mkdir -p /var/log/graphscope \
  && sudo chown -R graphscope:graphscope /var/log/graphscope

