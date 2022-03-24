FROM ubuntu:20.04

# shanghai zoneinfo
ENV TZ=Asia/Shanghai
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone 

# apt install dependencies
# openjdk-8-jdk perl maven for GIE
RUN apt-get update -y && apt install -y \
    ca-certificates ccache cmake curl etcd git \
    libbrotli-dev libbz2-dev libcurl4-openssl-dev libdouble-conversion-dev libevent-dev libgflags-dev \
    libboost-all-dev libgoogle-glog-dev libgrpc-dev libgrpc++-dev libgtest-dev libgsasl7-dev \
    libtinfo5 libkrb5-dev liblz4-dev libprotobuf-dev librdkafka-dev libre2-dev libsnappy-dev \
    libssl-dev libunwind-dev libutf8proc-dev libxml2-dev libz-dev libzstd-dev lsb-release maven openjdk-8-jdk \
    perl protobuf-compiler-grpc python3-pip sudo telnet uuid-dev vim wget zip zlib1g-dev && \
  rm -fr /var/lib/apt/lists/*

# rust
RUN cd /tmp && \
  wget --no-verbose https://golang.org/dl/go1.15.5.linux-amd64.tar.gz && \
  tar -C /usr/local -xzf go1.15.5.linux-amd64.tar.gz && \
  curl -sf -L https://static.rust-lang.org/rustup.sh | \
      sh -s -- -y --profile minimal --default-toolchain 1.48.0 && \
  echo "source ~/.cargo/env" >> ~/.bashrc

# java
ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
ENV PATH=${JAVA_HOME}/bin:${PATH}:/usr/local/go/bin:/usr/local/zookeeper/bin
ENV LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:/usr/local/lib

# gremlin
RUN mkdir -p /tmp && cd /tmp && wget https://archive.apache.org/dist/tinkerpop/3.4.2/apache-tinkerpop-gremlin-console-3.4.2-bin.zip && \
    unzip apache-tinkerpop-gremlin-console-3.4.2-bin.zip

SHELL ["/bin/bash", "-c"]

RUN source /root/.cargo/env && \
    rustup install stable && rustup default stable && rustup component add rustfmt && \
    echo "source ~/.cargo/env" >> ~/.bashrc
