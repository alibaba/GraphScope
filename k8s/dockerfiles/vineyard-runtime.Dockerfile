# The vineyard-runtime image including all vineyard-related
# dependencies that could graphscope interactive engine.

ARG REGISTRY=registry.cn-hongkong.aliyuncs.com
ARG BUILDER_VERSION=latest
FROM $REGISTRY/graphscope/vineyard-dev:$BUILDER_VERSION AS builder

RUN mkdir artifacts && \
    cd artifacts && \
    mkdir bin lib lib64 && \
    cp $GRAPHSCOPE_HOME/bin/vineyard-graph-loader bin/ && \
    cp -P $GRAPHSCOPE_HOME/lib/lib* lib/ && \
    cp -P $GRAPHSCOPE_HOME/lib64/lib* lib64/ && \
    tar czf artifacts.tar.gz ./*

FROM centos:7 AS runtime
COPY --from=builder /home/graphscope/artifacts/artifacts.tar.gz /root/artifacts.tar.gz
COPY --from=builder /opt/openmpi /opt/openmpi
COPY --from=builder /opt/vineyard /opt/vineyard
RUN tar xzf /root/artifacts.tar.gz -C /usr/local/ && rm /root/artifacts.tar.gz

ENV LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/local/lib:/usr/local/lib64

RUN yum install -y sudo libunwind-devel libgomp && \
    yum clean all -y --enablerepo='*' && \
    rm -rf /var/cache/yum
RUN sudo chmod a+wrx /tmp

RUN useradd -m graphscope -u 1001 \
    && echo 'graphscope ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers
USER graphscope
WORKDIR /home/graphscope

RUN sudo mkdir -p /var/log/graphscope \
  && sudo chown -R graphscope:graphscope /var/log/graphscope
