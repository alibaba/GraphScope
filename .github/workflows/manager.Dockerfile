FROM registry.cn-hongkong.aliyuncs.com/graphscope/graphscope-runtime:latest
RUN mkdir -p /home/maxgraph
# copy binary from builder
COPY ./interactive_engine/src/assembly/target/0.0.1-SNAPSHOT.tar.gz /home/maxgraph/
COPY ./interactive_engine/src/instance-manager/target/0.0.1-SNAPSHOT.tar.gz /home/maxgraph/instance-0.0.1-SNAPSHOT.tar.gz
RUN tar -xf /home/maxgraph/0.0.1-SNAPSHOT.tar.gz -C /home/maxgraph
RUN tar -xf /home/maxgraph/instance-0.0.1-SNAPSHOT.tar.gz -C /home/maxgraph
# copy bin and config from builder
RUN mkdir -p /home/maxgraph/{bin,conf}
COPY ./interactive_engine/bin/giectl /home/maxgraph/bin/giectl
COPY ./interactive_engine/conf/* /home/maxgraph/conf/

ENV GRAPHSCOPE_HOME=/home/maxgraph
ENV GRAPHSCOPE_RUNTIME=/tmp/graphscope

WORKDIR /home/maxgraph/
