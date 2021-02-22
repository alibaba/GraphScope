FROM registry.cn-hongkong.aliyuncs.com/graphscope/graphscope-runtime:latest

ARG profile=release

COPY ./opt/graphscope/ /usr/local/
RUN cd /usr/local/dist/ && pip3 install ./*.whl

RUN mkdir -p /home/maxgraph
ENV VINEYARD_IPC_SOCKET /home/maxgraph/data/vineyard/vineyard.sock
COPY ./interactive_engine/src/executor/target/release/executor /home/maxgraph/executor

COPY ./interactive_engine/src/executor/store/log4rs.yml /home/maxgraph/log4rs.yml
RUN mkdir -p /home/maxgraph/native
ENV LD_LIBRARY_PATH $LD_LIBRARY_PATH:/home/maxgraph/native

# enable debugging
ENV RUST_BACKTRACE=1

# copy start script from builder
RUN mkdir -p /home/maxgraph/config
COPY ./interactive_engine/deploy/docker/dockerfile/executor-entrypoint.sh /home/maxgraph/executor-entrypoint.sh
COPY ./interactive_engine/deploy/docker/dockerfile/executor.vineyard.properties /home/maxgraph/config/executor.application.properties

RUN mkdir -p /root/maxgraph
COPY ./interactive_engine/deploy/docker/dockerfile/set_config.sh /root/maxgraph/set_config.sh
COPY ./interactive_engine/deploy/docker/dockerfile/kill_process.sh /root/maxgraph/kill_process.sh
