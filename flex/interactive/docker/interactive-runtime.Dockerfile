FROM ubuntu:20.04
ARG CI=false

# change bash as default
SHELL ["/bin/bash", "-c"]

FROM ubuntu:20.04 AS base_image

# install sudo
RUN apt-get update && apt-get install -y sudo

# Add graphscope user with user id 1001
RUN useradd -m graphscope -u 1001 && \
    echo 'graphscope ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers

# g++ + jre 500MB
RUN apt-get update && apt-get -y install locales g++-9 cmake openjdk-11-jre-headless && \
    locale-gen en_US.UTF-8 && apt-get clean -y && sudo rm -rf /var/lib/apt/lists/* 
ENV LANG en_US.UTF-8
ENV LANGUAGE en_US:en
ENV LC_ALL en_US.UTF-8

# shanghai zoneinfo
ENV TZ=Asia/Shanghai
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# basic dependencies

# builder 
FROM base_image AS builder

RUN apt-get update && apt-get install -y protobuf-compiler libprotobuf-dev maven git vim curl \
    wget python3 make libc-ares-dev doxygen python3-pip net-tools curl default-jdk \
    libgoogle-glog-dev libopenmpi-dev libboost-all-dev libyaml-cpp-dev libprotobuf-dev libcrypto++-dev openssl

RUN cd /tmp && sudo apt-get install -y -V ca-certificates lsb-release wget && \
    curl -o apache-arrow-apt-source-latest.deb https://apache.jfrog.io/artifactory/arrow/$(lsb_release --id --short | tr 'A-Z' 'a-z')/apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb && \
    sudo apt-get install -y ./apache-arrow-apt-source-latest.deb && \
    sudo apt-get update && sudo apt-get install -y libarrow-dev=6.0.1-1

# install libgrape-lite
RUN cd /tmp && \
    git clone https://github.com/alibaba/libgrape-lite.git -b v0.3.2 --single-branch && cd libgrape-lite && \
    mkdir build && cd build && cmake .. -DBUILD_LIBGRAPELITE_TESTS=OFF -DCMAKE_INSTALL_PREFIX=/opt/flex && make -j && make install && rm -rf /tmp/libgrape-lite

# install hiactor
RUN cd /tmp && git clone https://github.com/alibaba/hiactor.git -b v0.1.1 --single-branch && cd hiactor && \
    git submodule update --init --recursive && ./seastar/seastar/install-dependencies.sh && mkdir build && cd build && \
    cmake -DCMAKE_INSTALL_PREFIX=/opt/flex -DHiactor_DEMOS=OFF -DHiactor_TESTING=OFF -DHiactor_DPDK=OFF -DHiactor_CXX_DIALECT=gnu++17 -DSeastar_CXX_FLAGS="-DSEASTAR_DEFAULT_ALLOCATOR -mno-avx512" .. && \
    make -j && make install && rm -rf /tmp/hiactor

RUN chown -R graphscope:graphscope /opt/flex

# set home to graphscope user
ENV HOME=/home/graphscope
USER graphscope
WORKDIR /home/graphscope

SHELL ["/bin/bash", "-c"]

RUN curl -sf -L https://static.rust-lang.org/rustup.sh | \
  sh -s -- -y --profile minimal --default-toolchain=1.70.0 && \
  chmod +x "$HOME/.cargo/env" && \
  echo "$source $HOME/.cargo/env" >> ~/.bashrc && \
  . ${HOME}/.cargo/env && \
  cargo --version

# install flex
RUN . ${HOME}/.cargo/env && cargo --version && rustc --version &&  git clone https://github.com/GraphScope/GraphScope.git -b main --single-branch && cd GraphScope/flex && \
    mkdir build && cd build && cmake .. -DCMAKE_INSTALL_PREFIX=/opt/flex -DBUILD_DOC=OFF && make -j && make install && \
    cd ~/GraphScope/interactive_engine/ && mvn clean package -Pexperimental -DskipTests && \
    cd ~/GraphScope/interactive_engine/compiler && cp target/compiler-0.0.1-SNAPSHOT.jar /opt/flex/lib/ && \
    cp target/libs/*.jar /opt/flex/lib/ && \
    ls ~/GraphScope/interactive_engine/executor/ir && \
    cp ~/GraphScope/interactive_engine/executor/ir/target/release/libir_core.so /opt/flex/lib/ && \
    rm -rf ~/GraphScope 

from base_image as final_image


# copy builder's /opt/flex to final image
COPY --from=builder /opt/flex /opt/flex
# remov bin/run_app
RUN rm -rf /opt/flex/bin/run_app

#copy gflags
COPY --from=builder /usr/lib/x86_64-linux-gnu/libgflags*.so* /usr/lib/x86_64-linux-gnu/
# use ldd to list dynamic link dependencies for sync_server and copy to final image
COPY --from=builder /usr/lib/x86_64-linux-gnu/libglog*.so* /usr/lib/x86_64-linux-gnu/
COPY --from=builder /usr/lib/x86_64-linux-gnu/libyaml-cpp*.so* /usr/lib/x86_64-linux-gnu/
#copy mpi
COPY --from=builder /usr/lib/x86_64-linux-gnu/libmpi*.so* /usr/lib/x86_64-linux-gnu/
#copy 
# copy protobuf
COPY --from=builder /usr/lib/x86_64-linux-gnu/libprotobuf*.so* /usr/lib/x86_64-linux-gnu/
# copy boost program options and thread
COPY --from=builder /usr/lib/x86_64-linux-gnu/libboost_program_options*.so* /usr/lib/x86_64-linux-gnu/
COPY --from=builder /usr/lib/x86_64-linux-gnu/libboost_thread*.so* /usr/lib/x86_64-linux-gnu/
COPY --from=builder /usr/lib/x86_64-linux-gnu/libcrypto*.so* /usr/lib/x86_64-linux-gnu/
#copy openrte
COPY --from=builder /usr/lib/x86_64-linux-gnu/libopen-rte*.so* /usr/lib/x86_64-linux-gnu/
# copy hwloc
COPY --from=builder /usr/lib/x86_64-linux-gnu/libhwloc*.so* /usr/lib/x86_64-linux-gnu/
# copy libunwind
COPY --from=builder /usr/lib/x86_64-linux-gnu/libunwind*.so* /usr/lib/x86_64-linux-gnu/
# copy arrow
COPY --from=builder /usr/lib/x86_64-linux-gnu/libarrow.so.600 /usr/lib/x86_64-linux-gnu/
# copy open-pal
COPY --from=builder /usr/lib/x86_64-linux-gnu/libopen-pal*.so* /usr/lib/x86_64-linux-gnu/
# copy ltdl
COPY --from=builder /usr/lib/x86_64-linux-gnu/libltdl*.so* /usr/lib/x86_64-linux-gnu/
# copy event
COPY --from=builder /usr/lib/x86_64-linux-gnu/libevent*.so* /usr/lib/x86_64-linux-gnu/
# copy utf8proc
COPY --from=builder /usr/lib/x86_64-linux-gnu/libutf8proc*.so* /usr/lib/x86_64-linux-gnu/
# copy re2
COPY --from=builder /usr/lib/x86_64-linux-gnu/libre2*.so* /usr/lib/x86_64-linux-gnu/
# copy snappy
COPY --from=builder /usr/lib/x86_64-linux-gnu/libsnappy*.so* /usr/lib/x86_64-linux-gnu/
# copy glog headers
COPY --from=builder /usr/include/glog /usr/include/glog
# copy arrow headers
COPY --from=builder /usr/include/arrow /usr/include/arrow
# copy glags headers
COPY --from=builder /usr/include/gflags /usr/include/gflags
# link g++-9 to g++
RUN sudo ln -sf /usr/bin/g++-9 /usr/bin/g++
# copy openmpi headers to /usr/include
COPY --from=builder /usr/lib/x86_64-linux-gnu/openmpi/include/ /opt/flex/include
# copy boost headers
COPY --from=builder /usr/include/boost /usr/include/boost
# copy protobuf headers
COPY --from=builder /usr/include/google /usr/include/google
# copy yaml cpp headers
COPY --from=builder /usr/include/yaml-cpp /usr/include/yaml-cpp

# remove llvm seastar.a
RUN sudo rm -rf /usr/lib/x86_64-linux-gnu/libLLVM*.so* && sudo rm -rf /opt/flex/lib/libseastar.a && \
    sudo rm -rf /usr/lib/x86_64-linux-gnu/lib/libcuda.so && \
    sudo rm -rf /usr/lib/x86_64-linux-gnu/lib/libcudart.so && \
    sudo rm -rf /usr/lib/x86_64-linux-gnu/lib/libicudata.so*


# set home to graphscope user
ENV HOME=/home/graphscope
USER graphscope
WORKDIR /home/graphscope

RUN chmod +x /opt/flex/bin/*

RUN sudo ln -sf /opt/flex/bin/* /usr/local/bin/ \
  && sudo ln -sfn /opt/flex/include/* /usr/local/include/ \
  && sudo ln -sf -r /opt/flex/lib/* /usr/local/lib \
  && sudo ln -sf /opt/flex/lib64/*so* /usr/local/lib64

ENV LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:/opt/flex/lib/:/usr/lib/


# Change to graphscope user
USER graphscope
WORKDIR /home/graphscope
