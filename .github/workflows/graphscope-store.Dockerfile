FROM registry.cn-hongkong.aliyuncs.com/graphscope/graphscope-runtime:latest

RUN mkdir -p /home/maxgraph/{bin,config}
COPY ./opt/graphscope/ /usr/local/
COPY ./maxgraph /home/maxgraph/maxgraph
COPY ./ready_probe.sh /tmp/ready_probe.sh
COPY ./interactive_engine/bin/giectl /home/maxgraph/bin/giectl
COPY ./interactive_engine/config/*  /home/maxgraph/config/

ENV GRAPHSCOPE_HOME=/usr/local
ENV GRAPHSCOPE_RUNTIME=/tmp/graphscope

WORKDIR /home/maxgraph/