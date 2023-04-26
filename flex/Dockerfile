FROM ubuntu:20.04
ARG CI=false

# change bash as default
SHELL ["/bin/bash", "-c"]


RUN apt update && apt -y install locales && locale-gen en_US.UTF-8
ENV LANG en_US.UTF-8
ENV LANGUAGE en_US:en
ENV LC_ALL en_US.UTF-8

# shanghai zoneinfo
ENV TZ=Asia/Shanghai
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# install dependencies
RUN apt install -y \
      ninja-build ragel libhwloc-dev libnuma-dev libpciaccess-dev vim wget \
      git g++ libgoogle-glog-dev cmake libopenmpi-dev default-jdk libcrypto++-dev \
      libboost-all-dev libxml2-dev
RUN apt install -y xfslibs-dev libgnutls28-dev liblz4-dev maven openssl pkg-config \
      libsctp-dev gcc make python3 systemtap-sdt-dev libtool libyaml-cpp-dev \
      libc-ares-dev stow libfmt-dev diffutils valgrind doxygen python3-pip net-tools

# install libgrape-lite
RUN cd /root && \
    git clone https://github.com/alibaba/libgrape-lite.git && \
    cd libgrape-lite && git checkout 976544ef7a9777ed93088459638ff87154e2109d && \
    mkdir build && cd build && cmake .. && make -j && make install

RUN cp /usr/local/lib/libgrape-lite.so /usr/lib/libgrape-lite.so

RUN git clone https://github.com/alibaba/hiactor.git && cd hiactor && git checkout e16949ca53 && \
    git submodule update --init --recursive && ./seastar/seastar/install-dependencies.sh && mkdir build && cd build && \
    cmake -DHiactor_DEMOS=OFF -DHiactor_TESTING=OFF -DHiactor_DPDK=OFF -DHiactor_CXX_DIALECT=gnu++17 -DSeastar_CXX_FLAGS="-DSEASTAR_DEFAULT_ALLOCATOR -mno-avx512" .. && \
    make -j && make install
