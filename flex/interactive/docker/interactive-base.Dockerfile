FROM ubuntu:20.04

ENV DEBIAN_FRONTEND=noninteractive

# change bash as default
SHELL ["/bin/bash", "-c"]

# install sudo
RUN apt-get update && apt-get install -y sudo

# Add graphscope user with user id 1001
RUN useradd -m graphscope -u 1001 && \
    echo 'graphscope ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers

# g++ + jre 500MB
RUN apt-get update && apt-get -y install locales g++ cmake openjdk-11-jre-headless && \
    locale-gen en_US.UTF-8 && apt-get clean -y && sudo rm -rf /var/lib/apt/lists/* 
ENV LANG en_US.UTF-8
ENV LANGUAGE en_US:en
ENV LC_ALL en_US.UTF-8

# shanghai zoneinfo
ENV TZ=Asia/Shanghai
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

RUN apt-get update && apt-get install -y protobuf-compiler libprotobuf-dev maven git vim curl \
    wget python3 make libc-ares-dev doxygen python3-pip net-tools curl default-jdk nlohmann-json3-dev \
    libgoogle-glog-dev libopenmpi-dev libboost-all-dev libyaml-cpp-dev libprotobuf-dev libcrypto++-dev openssl

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
