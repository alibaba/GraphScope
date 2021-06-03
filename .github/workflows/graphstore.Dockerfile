FROM registry.cn-hongkong.aliyuncs.com/graphscope/graphscope-runtime:latest

COPY ./opt/graphscope/ /usr/local/
COPY maxgraph.tar.gz /tmp/maxgraph.tar.gz

RUN mkdir -p /home/maxgraph \
    && tar -zxf /tmp/maxgraph.tar.gz -C /home/maxgraph
    && rm -f /tmp/maxgraph.tar.gz

WORKDIR /home/maxgraph/