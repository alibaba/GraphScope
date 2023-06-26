FROM hqps-server-base:latest

RUN git clone https://github.com/zhanglei1949/GraphScope.git -b hqps-flex --single-branch && pushd GraphScope/flex && \
    mkdir build && cd build && cmake .. && make -j && make install && popd && cd GraphScope/interactive_engine/compiler && bash -c ". $HOME/.cargo/env && make build"

ENV GIE_HOME=$HOME/GraphScope/interactive_engine/