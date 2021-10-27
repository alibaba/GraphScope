FROM registry.cn-hongkong.aliyuncs.com/graphscope/graphscope-vineyard:v0.3.1

COPY ./maxgraph /usr/local/maxgraph
COPY ./ready_probe.sh /tmp/ready_probe.sh

ENV GRAPHSCOPE_HOME=/usr/local
