FROM ubuntu:20.04
ARG CI=false

# change bash as default
SHELL ["/bin/bash", "-c"]


RUN apt-get update && apt-get -y install locales && locale-gen en_US.UTF-8
ENV LANG en_US.UTF-8
ENV LANGUAGE en_US:en
ENV LC_ALL en_US.UTF-8

# shanghai zoneinfo
ENV TZ=Asia/Shanghai
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# install dependencies
RUN apt-get update && apt-get install -y vim wget \
      git g++ libgoogle-glog-dev cmake libopenmpi-dev default-jdk \
      libboost-all-dev
RUN apt-get install -y maven openssl \
      gcc make python3 libyaml-cpp-dev \
      libc-ares-dev doxygen python3-pip net-tools curl libncurses5-dev

# install libgrape-lite
RUN cd /tmp && \
    git clone https://github.com/alibaba/libgrape-lite.git -b v0.3.2 --single-branch && cd libgrape-lite && \
    mkdir build && cd build && cmake .. && make -j && make install && rm -rf /tmp/libgrape-lite

RUN cd /tmp && git clone https://github.com/alibaba/hiactor.git -b v0.1.1 --single-branch && cd hiactor && \
    git submodule update --init --recursive && ./seastar/seastar/install-dependencies.sh && mkdir build && cd build && \
    cmake -DHiactor_DEMOS=OFF -DHiactor_TESTING=OFF -DHiactor_DPDK=OFF -DHiactor_CXX_DIALECT=gnu++17 -DSeastar_CXX_FLAGS="-DSEASTAR_DEFAULT_ALLOCATOR -mno-avx512" .. && \
    make -j && make install && rm -rf /tmp/hiactor

#install protobuf
RUN apt-get install -y protobuf-compiler libprotobuf-dev

#install arrow
RUN cd /tmp && apt-get install -y -V ca-certificates lsb-release wget && \
    curl -o apache-arrow-apt-source-latest.deb https://apache.jfrog.io/artifactory/arrow/$(lsb_release --id --short | tr 'A-Z' 'a-z')/apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb && \
    apt-get install -y ./apache-arrow-apt-source-latest.deb && \
    apt-get update && apt-get install -y libarrow-dev=8.0.0-1
    
RUN apt-get install -y sudo

# Add graphscope user with user id 1001
RUN useradd -m graphscope -u 1001 && \
    echo 'graphscope ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers

# Change to graphscope user
USER graphscope
WORKDIR /home/graphscope

# set HOME
ENV HOME /home/graphscope

RUN curl -sf -L https://static.rust-lang.org/rustup.sh | \
  sh -s -- -y --profile minimal --default-toolchain=1.70.0 && \
  chmod +x "$HOME/.cargo/env" && \
  echo "$source $HOME/.cargo/env" >> ~/.bashrc && \
  source "$HOME/.cargo/env" && \
  echo "1.70.0" > rust-toolchain && \
  bash -c "rustup component add rustfmt"
