FROM registry.cn-hongkong.aliyuncs.com/graphscope/hqps-server-base:v0.0.4
ARG CI=false

# change bash as default
SHELL ["/bin/bash", "-c"]

# install graphscope
RUN cd /home/graphscope/ && git clone -b main --single-branch https://github.com/alibaba/GraphScope.git && \
    cd GraphScope/flex && mkdir build && cd build && cmake .. -DBUILD_DOC=OFF && sudo make -j install

# install graphscope GIE
RUN . /home/graphscope/.cargo/env && cd /home/graphscope/GraphScope/interactive_engine/compiler && make build
