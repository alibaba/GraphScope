FROM registry.cn-hongkong.aliyuncs.com/graphscope/graphscope-runtime:latest

RUN mkdir -p /home/maxgraph 
COPY ./opt/graphscope/ /usr/local/
COPY ./maxgraph /home/maxgraph/maxgraph
COPY ./ready_probe.sh /tmp/ready_probe.sh

WORKDIR /home/maxgraph/
