FROM registry.cn-hongkong.aliyuncs.com/graphscope/graphscope-runtime:latest

COPY ./opt/graphscope/ /usr/local/
COPY ./maxgraph /usr/local/maxgraph
COPY ./ready_probe.sh /tmp/ready_probe.sh
COPY ./interactive_engine/bin/giectl /usr/local/bin/giectl
COPY ./interactive_engine/conf/*  /usr/local/conf/

ENV GRAPHSCOPE_HOME=/usr/local
