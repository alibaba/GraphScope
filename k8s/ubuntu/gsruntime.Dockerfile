# the graphscope-runtime image is based on ubuntu-20, including all necessary runtime
# dependencies for graphscope.

FROM ubuntu:20.04

# shanghai zoneinfo
ENV TZ=Asia/Shanghai
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# apt install dependencies
# openjdk-8-jdk perl maven for GIE
RUN apt update -y && apt install -y \
    ca-certificates ccache cmake curl etcd git \
    libbrotli-dev libbz2-dev libcurl4-openssl-dev libevent-dev libgflags-dev \
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

# zetcd
RUN export PATH=/usr/local/go/bin:${PATH} && go get github.com/etcd-io/zetcd/cmd/zetcd && \
 cp /root/go/bin/zetcd /usr/local/bin/zetcd

# apache arrow 3.0.0
RUN wget https://apache.jfrog.io/artifactory/arrow/$(lsb_release --id --short | tr 'A-Z' 'a-z')/apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb && \
    apt install -y -V ./apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb && \
    apt update -y && \
    apt install -y libarrow-dev=3.0.0-1 libarrow-python-dev=3.0.0-1 && \
    rm ./apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb

# zookeeper
RUN wget https://archive.apache.org/dist/zookeeper/zookeeper-3.4.14/zookeeper-3.4.14.tar.gz && \
    tar xf zookeeper-3.4.14.tar.gz -C /usr/local/ && \
    cd /usr/local && ln -s zookeeper-3.4.14 zookeeper && \
    mkdir -p /usr/local/zookeeper/data && \
    mkdir -p /usr/local/zookeeper/logs && \
    cp /usr/local/zookeeper/conf/zoo_sample.cfg /usr/local/zookeeper/conf/zoo.cfg

# pip dependencies
RUN pip3 install -U pip && \
  pip3 --no-cache-dir install auditwheel daemons grpcio-tools gremlinpython hdfs3 fsspec oss2 s3fs ipython kubernetes \
    libclang networkx==2.4 numpy pandas parsec pycryptodome pyorc pytest scipy scikit_learn wheel && \
  pip3 --no-cache-dir install Cython --pre -U

ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
ENV PATH=${JAVA_HOME}/bin:${PATH}:/usr/local/go/bin:/usr/local/zookeeper/bin
ENV LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:/usr/local/lib

# for programming output
ENV LC_ALL=C
