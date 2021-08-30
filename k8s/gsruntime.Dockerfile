# the graphscope-runtime image is based on centos-7, including all necessary runtime
# dependencies for graphscope.

FROM centos:7

# yum install gcc7, cat not merge in one layer
RUN yum install -y centos-release-scl && \
    yum clean all && \
    rm -fr /var/cache/yum
RUN yum install -y devtoolset-7-gcc-* \
                   devtoolset-7-libasan-devel.x86_64 && \
    yum clean all && \
    rm -fr /var/cache/yum
RUN echo "source scl_source enable devtoolset-7" >> /etc/bashrc && source /etc/bashrc
SHELL ["/usr/bin/scl", "enable", "devtoolset-7"]

RUN yum install -y epel-release && \
    yum clean all && \
    rm -fr /var/cache/yum

RUN yum install -y https://packages.endpoint.com/rhel/7/os/x86_64/endpoint-repo-1.7-1.x86_64.rpm

# yum install dependencies
RUN yum install -y autoconf automake double-conversion-devel git \
        libcurl-devel libevent-devel libgsasl-devel librdkafka-devel libunwind-devel.x86_64 \
        libuuid-devel libxml2-devel libzip libzip-devel m4 minizip minizip-devel sudo \
        make net-tools openssl-devel python3-devel rsync telnet tools unzip vim wget which zip bind-utils && \
    yum clean all && \
    rm -fr /var/cache/yum

# cmake3
RUN cd /tmp && \
    wget https://github.com/Kitware/CMake/releases/download/v3.19.1/cmake-3.19.1-Linux-x86_64.sh && \
    bash cmake-3.19.1-Linux-x86_64.sh --prefix=/usr --skip-license && \
    cd /tmp && \
    rm -rf /tmp/cmake-3.19.1-Linux-x86_64.sh

# pip dependencies
RUN pip3 install -U pip && \
    pip3 --no-cache-dir install auditwheel daemons grpcio-tools gremlinpython hdfs3 fsspec oss2 s3fs ipython kubernetes \
        libclang networkx==2.4 numpy pandas parsec pycryptodome pyorc pytest scipy scikit_learn wheel && \
    pip3 --no-cache-dir install Cython --pre -U

ENV LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:/usr/local/lib:/usr/local/lib64
ENV PATH=${PATH}:/usr/local/bin
ENV CC=/opt/rh/devtoolset-7/root/usr/bin/gcc
ENV CXX=/opt/rh/devtoolset-7/root/usr/bin/g++

# apache arrow v1.0.1
RUN cd /tmp && \
    wget https://github.com/apache/arrow/archive/apache-arrow-1.0.1.tar.gz && \
    tar zxvf apache-arrow-1.0.1.tar.gz && \
    cd arrow-apache-arrow-1.0.1 && \
    mkdir build && \
    cd build && \
    cmake ../cpp \
        -DARROW_COMPUTE=ON \
        -DARROW_WITH_UTF8PROC=OFF \
        -DARROW_CSV=ON \
        -DARROW_CUDA=OFF \
        -DARROW_DATASET=OFF \
        -DARROW_FILESYSTEM=ON \
        -DARROW_FLIGHT=OFF \
        -DARROW_GANDIVA=OFF \
        -DARROW_GANDIVA_JAVA=OFF \
        -DARROW_HDFS=OFF \
        -DARROW_HIVESERVER2=OFF \
        -DARROW_JSON=OFF \
        -DARROW_ORC=OFF \
        -DARROW_PARQUET=OFF \
        -DARROW_PLASMA=OFF \
        -DARROW_PLASMA_JAVA_CLIENT=OFF \
        -DARROW_PYTHON=ON \
        -DARROW_S3=OFF \
        -DARROW_WITH_BZ2=OFF \
        -DARROW_WITH_ZLIB=OFF \
        -DARROW_WITH_LZ4=OFF \
        -DARROW_WITH_SNAPPY=OFF \
        -DARROW_WITH_ZSTD=OFF \
        -DARROW_WITH_BROTLI=OFF \
        -DARROW_IPC=ON \
        -DARROW_BUILD_BENCHMARKS=OFF \
        -DARROW_BUILD_EXAMPLES=OFF \
        -DARROW_BUILD_INTEGRATION=OFF \
        -DARROW_BUILD_UTILITIES=OFF \
        -DARROW_BUILD_TESTS=OFF \
        -DARROW_ENABLE_TIMING_TESTS=OFF \
        -DARROW_FUZZING=OFF \
        -DARROW_USE_ASAN=OFF \
        -DARROW_USE_TSAN=OFF \
        -DARROW_USE_UBSAN=OFF \
        -DARROW_JEMALLOC=OFF \
        -DARROW_BUILD_SHARED=ON \
        -DARROW_BUILD_STATIC=OFF && \
    make -j`nproc` && \
    make install && \
    cd /tmp && \
    rm -fr /tmp/arrow-apache-arrow-1.0.1 /tmp/apache-arrow-1.0.1.tar.gz

# boost v1.73.0
RUN cd /tmp && \
    wget https://boostorg.jfrog.io/artifactory/main/release/1.73.0/source/boost_1_73_0.tar.gz && \
    tar zxf boost_1_73_0.tar.gz && \
    cd boost_1_73_0 && \
    ./bootstrap.sh && \
    ./b2 install link=shared runtime-link=shared variant=release threading=multi || true && \
    cd /tmp && \
    rm -fr /tmp/boost_1_73_0 /tmp/boost_1_73_0.tar.gz

# gflags v2.2.2
RUN cd /tmp && \
    wget https://github.com/gflags/gflags/archive/v2.2.2.tar.gz && \
    tar zxvf v2.2.2.tar.gz && \
    cd gflags-2.2.2 && \
    mkdir build && \
    cd build && \
    cmake .. -DBUILD_SHARED_LIBS=ON && \
    make -j`nproc` && \
    make install && \
    cd /tmp && \
    rm -fr /tmp/v2.2.2.tar.gz /tmp/gflags-2.2.2

# glog v0.4.0
RUN cd /tmp && \
    wget https://github.com/google/glog/archive/v0.4.0.tar.gz && \
    tar zxvf v0.4.0.tar.gz && \
    cd glog-0.4.0 && \
    mkdir build && \
    cd build && \
    cmake .. -DBUILD_SHARED_LIBS=ON && \
    make -j`nproc` && \
    make install && \
    cd /tmp && \
    rm -fr /tmp/v0.4.0.tar.gz /tmp/glog-0.4.0 

# googletest v1.10.0
RUN cd /tmp && \
    wget https://github.com/google/googletest/archive/release-1.10.0.tar.gz && \
    tar zxvf release-1.10.0.tar.gz && \
    cd googletest-release-1.10.0 && \
    mkdir build && \
    cd build && \
    cmake .. -DBUILD_SHARED_LIBS=ON && \
    make -j`nproc` && \
    make install && \
    cd /tmp && \
    rm -fr /tmp/release-1.10.0.tar.gz /tmp/googletest-release-1.10.0

# protobuf v.3.13.0
RUN cd /tmp && \
    wget https://github.com/protocolbuffers/protobuf/releases/download/v3.13.0/protobuf-all-3.13.0.tar.gz && \
    tar zxvf protobuf-all-3.13.0.tar.gz && \
    cd protobuf-3.13.0 && \
    ./configure --enable-shared --disable-static && \
    make -j`nproc` && \
    make install && \
    ldconfig && \
    cd /tmp && \
    rm -fr /tmp/protobuf-all-3.13.0.tar.gz /tmp/protobuf-3.13.0

# zlib v1.2.11
RUN cd /tmp && \
    wget https://github.com/madler/zlib/archive/v1.2.11.tar.gz && \
    tar zxvf v1.2.11.tar.gz && \
    cd zlib-1.2.11 && \
    mkdir build && \
    cd build && \
    cmake .. -DBUILD_SHARED_LIBS=ON && \
    make -j`nproc` && \
    make install && \
    cd /tmp && \
    rm -fr /tmp/v1.2.11.tar.gz /tmp/zlib-1.2.11

# grpc v1.33.1
RUN cd /tmp && \
    git clone --depth 1 --branch v1.33.1 https://github.com/grpc/grpc.git && \
    cd grpc && \
    git submodule update --init && \
    mkdir build && \
    cd build && \
    cmake .. -DBUILD_SHARED_LIBS=ON \
        -DgRPC_INSTALL=ON \
        -DgRPC_BUILD_TESTS=OFF \
        -DgRPC_BUILD_CSHARP_EXT=OFF \
        -DgRPC_BUILD_GRPC_CSHARP_PLUGIN=OFF \
        -DgRPC_BUILD_GRPC_NODE_PLUGIN=OFF \
        -DgRPC_BUILD_GRPC_OBJECTIVE_C_PLUGIN=OFF \
        -DgRPC_BUILD_GRPC_PHP_PLUGIN=OFF \
        -DgRPC_BUILD_GRPC_PYTHON_PLUGIN=OFF \
        -DgRPC_BUILD_GRPC_RUBY_PLUGIN=OFF \
        -DgRPC_BACKWARDS_COMPATIBILITY_MODE=ON \
        -DgRPC_PROTOBUF_PROVIDER=package \
        -DgRPC_ZLIB_PROVIDER=package \
        -DgRPC_SSL_PROVIDER=package && \
    make -j`nproc` && \
    make install && \
    cd /tmp && \
    rm -fr /tmp/grpc

# etcd v3.4.13
RUN cd /tmp && \
    mkdir -p /tmp/etcd-download-test && \
    export ETCD_VER=v3.4.13 && \
    export DOWNLOAD_URL=https://github.com/etcd-io/etcd/releases/download && \
    curl -L ${DOWNLOAD_URL}/${ETCD_VER}/etcd-${ETCD_VER}-linux-amd64.tar.gz -o /tmp/etcd-${ETCD_VER}-linux-amd64.tar.gz && \
    tar xzvf /tmp/etcd-${ETCD_VER}-linux-amd64.tar.gz -C /tmp/etcd-download-test --strip-components=1 && \
    mv /tmp/etcd-download-test/etcd /usr/local/bin/ && \
    mv /tmp/etcd-download-test/etcdctl /usr/local/bin/ && \
    cd /tmp && \
    rm -fr /tmp/etcd-${ETCD_VER}-linux-amd64.tar.gz /tmp/etcd-download-test

# fmt v7.0.3, required by folly
RUN cd /tmp && \
    wget https://github.com/fmtlib/fmt/archive/7.0.3.tar.gz && \
    tar zxvf 7.0.3.tar.gz && \
    cd fmt-7.0.3/ && \
    mkdir build && \
    cd build && \
    cmake .. -DBUILD_SHARED_LIBS=ON && \
    make install -j && \
    cd /tmp && \
    rm -fr /tmp/7.0.3.tar.gz /tmp/fmt-7.0.3

# folly v2020.10.19.00
RUN cd /tmp && \
    wget https://github.com/facebook/folly/archive/v2020.10.19.00.tar.gz && \
    tar zxvf v2020.10.19.00.tar.gz && \
    cd folly-2020.10.19.00 && mkdir _build && \
    cd _build && \
    cmake -DBUILD_SHARED_LIBS=ON -DCMAKE_POSITION_INDEPENDENT_CODE=ON .. && \
    make install -j && \
    cd /tmp && \
    rm -fr /tmp/v2020.10.19.00.tar.gz /tmp/folly-2020.10.19.00

# kubectl v1.19.2
RUN cd /tmp && export KUBE_VER=v1.19.2 && \
    curl -LO https://storage.googleapis.com/kubernetes-release/release/${KUBE_VER}/bin/linux/amd64/kubectl && \
    chmod +x ./kubectl && \
    cd /tmp && \
    mv ./kubectl /usr/local/bin/kubectl

# openmpi v4.0.5
RUN cd /tmp && \
    wget https://download.open-mpi.org/release/open-mpi/v4.0/openmpi-4.0.5.tar.gz && \
    tar zxvf openmpi-4.0.5.tar.gz && \
    cd openmpi-4.0.5 && ./configure --enable-mpi-cxx && \
    make -j`nproc` && \
    make install && \
    cd /tmp && \
    rm -fr /tmp/openmpi-4.0.5 /tmp/openmpi-4.0.5.tar.gz

# install hdfs runtime library
RUN cd /tmp && \
    git clone https://github.com/7br/libhdfs3-downstream.git && \
    cd libhdfs3-downstream/libhdfs3 && \
    mkdir -p /tmp/libhdfs3-downstream/libhdfs3/build && \
    cd /tmp/libhdfs3-downstream/libhdfs3/build && \
    cmake .. -DBUILD_SHARED_LIBS=ON \
             -DBUILD_HDFS3_TESTS=OFF && \
    make install -j && \
    cd /tmp && \
    rm -rf /tmp/libhdfs3-downstream

# GIE RUNTIME

# Install java and maven
RUN yum install -y perl java-1.8.0-openjdk-devel && \
    yum clean all && \
    rm -fr /var/cache/yum

# install hadoop
RUN cd /tmp && \
    wget https://archive.apache.org/dist/hadoop/core/hadoop-2.8.4/hadoop-2.8.4.tar.gz && \
    tar zxf hadoop-2.8.4.tar.gz -C /usr/local && \
    rm -rf hadoop-2.8.4.tar.gz

ENV JAVA_HOME /usr/lib/jvm/java
ENV HADOOP_HOME /usr/local/hadoop-2.8.4
ENV HADOOP_CONF_DIR $HADOOP_HOME/etc/hadoop
ENV HADOOP_COMMON_LIB_NATIVE_DIR $HADOOP_HOME/lib/native

ENV PATH $PATH:$HADOOP_HOME/bin

RUN bash -l -c 'echo export CLASSPATH="$($HADOOP_HOME/bin/hdfs classpath --glob)" >> /etc/bashrc'

# Prepare and set workspace
RUN mkdir -p /tmp/maven /usr/share/maven/ref \
    && curl -fsSL -o /tmp/apache-maven.tar.gz https://apache.osuosl.org/maven/maven-3/3.6.3/binaries/apache-maven-3.6.3-bin.tar.gz \
    && tar -xzf /tmp/apache-maven.tar.gz -C /usr/share/maven --strip-components=1 \
    && rm -f /tmp/apache-maven.tar.gz \
    && ln -s /usr/share/maven/bin/mvn /usr/bin/mvn \
    && export LD_LIBRARY_PATH=$(echo "$LD_LIBRARY_PATH" | sed "s/::/:/g") \
    && wget http://mirrors.ustc.edu.cn/gnu/libc/glibc-2.18.tar.gz \
    && tar -zxf glibc-2.18.tar.gz \
    && cd glibc-2.18 \
    && mkdir build && cd build \
    && ../configure --prefix=/usr \
    && make -j4 \
    && make install 

# patchelf for wheel packaging
RUN cd /tmp && \
    git clone --depth=1 https://github.com/NixOS/patchelf.git && \
    cd patchelf && \
    ./bootstrap.sh && \
    ./configure && \
    make install -j && \
    rm -rf patchelf/

# shanghai zoneinfo
ENV TZ=Asia/Shanghai
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && \
    echo '$TZ' > /etc/timezone

# for programming output
RUN localedef -c -f UTF-8 -i en_US en_US.UTF-8
ENV LANG='en_US.UTF-8' LANGUAGE='en_US:en' LC_ALL='en_US.UTF-8'

ENV PATH=${PATH}:/usr/local/go/bin
ENV RUST_BACKTRACE=1

# change user: graphscope
RUN useradd -m graphscope -u 1001 \
    && echo 'graphscope ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers
USER graphscope
WORKDIR /home/graphscope
