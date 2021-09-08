FROM registry.cn-hongkong.aliyuncs.com/graphscope/graphscope-runtime:latest

COPY ./opt/vineyard/ /usr/local/
COPY ./maxgraph /usr/local/maxgraph
COPY ./ready_probe.sh /tmp/ready_probe.sh

ENV GRAPHSCOPE_HOME=/usr/local
