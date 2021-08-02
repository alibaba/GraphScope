FROM registry.cn-hongkong.aliyuncs.com/graphscope/graphscope-runtime:latest

ARG profile=release

COPY ./opt/graphscope/ /usr/local/
RUN cd /usr/local/dist/ && pip3 install ./*.whl

RUN mkdir -p /home/maxgraph/{bin,config,conf}
ENV VINEYARD_IPC_SOCKET /home/maxgraph/data/vineyard/vineyard.sock
COPY ./interactive_engine/src/executor/target/release/executor /home/maxgraph/bin/executor
COPY ./interactive_engine/bin/giectl /home/maxgraph/bin/giectl
RUN chmod a+x /home/maxgraph/bin/giectl


COPY ./interactive_engine/src/executor/store/log4rs.yml /home/maxgraph/log4rs.yml
COPY ./interactive_engine/src/executor/store/log4rs.yml /home/maxgraph/conf/log4rs.yml
RUN mkdir -p /home/maxgraph/native
ENV LD_LIBRARY_PATH $LD_LIBRARY_PATH:/home/maxgraph/native

# install mars
RUN pip3 install git+https://github.com/mars-project/mars.git@35b44ed56e031c252e50373b88b85bd9f454332e#egg=pymars[distributed]

# enable debugging
ENV RUST_BACKTRACE=1

# copy start script from builder
COPY ./interactive_engine/config/* /home/maxgraph/config/
ENV GRAPHSCOPE_HOME=/home/maxgraph
ENV GRAPHSCOPE_RUNTIME=/tmp/graphscope
