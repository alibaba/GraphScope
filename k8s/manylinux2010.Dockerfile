# the graphscope-manylinux2010 image is based on manylinux2010, including all necessary
# dependencies for graphscope's wheel package.

FROM quay.io/pypa/manylinux2010_x86_64:2020-12-06-3a8b363

SHELL ["/usr/bin/scl", "enable", "devtoolset-8"]

# yum install dependencies
RUN yum install -y autoconf m4 double-conversion-devel git \
        libcurl-devel libevent-devel libgsasl-devel librdkafka-devel libunwind-devel.x86_64 \
        libuuid-devel libxml2-devel libzip libzip-devel minizip minizip-devel \
        make net-tools python3-devel rsync telnet tools unzip vim wget which zip && \
    yum clean all && \
    rm -fr /var/cache/yum && \
    cd /tmp && \
    wget https://github.com/Kitware/CMake/releases/download/v3.19.1/cmake-3.19.1-Linux-x86_64.sh && \
    bash cmake-3.19.1-Linux-x86_64.sh --prefix=/usr --skip-license && \
    cd /tmp && \
    rm -rf /tmp/cmake-3.19.1-Linux-x86_64.sh

ENV LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:/usr/local/lib:/usr/local/lib64
ENV PATH=${PATH}:/usr/local/bin

# openssl 1.1.1
RUN cd /tmp && \
    wget https://github.com/openssl/openssl/archive/OpenSSL_1_1_1h.tar.gz && \
    tar zxvf OpenSSL_1_1_1h.tar.gz && \
    cd openssl-OpenSSL_1_1_1h && \
    ./config -fPIC -shared && \
    make -j && \
    make install -j || true && \
    cd /tmp && \
    rm -rf /tmp/OpenSSL_1_1_1h.tar.gz /tmp/openssl-OpenSSL_1_1_1h

# gflags v2.2.2
RUN cd /tmp && \
    wget https://github.com/gflags/gflags/archive/v2.2.2.tar.gz && \
    tar zxvf v2.2.2.tar.gz && \
    cd gflags-2.2.2 && \
    mkdir build && \
    cd build && \
    cmake .. -DBUILD_SHARED_LIBS=ON && \
    make -j && \
    make install && \
    cd /tmp && \
    rm -fr /tmp/v2.2.2.tar.gz /tmp/gflags-2.2.2

# zlib v1.2.11
RUN cd /tmp && \
    wget https://github.com/madler/zlib/archive/v1.2.11.tar.gz && \
    tar zxvf v1.2.11.tar.gz && \
    cd zlib-1.2.11 && \
    mkdir build && \
    cd build && \
    cmake .. -DBUILD_SHARED_LIBS=ON && \
    make -j && \
    make install && \
    cd /tmp && \
    rm -fr /tmp/v1.2.11.tar.gz /tmp/zlib-1.2.11

# glog v0.4.0
RUN cd /tmp && \
    wget https://github.com/google/glog/archive/v0.4.0.tar.gz && \
    tar zxvf v0.4.0.tar.gz && \
    cd glog-0.4.0 && \
    mkdir build && \
    cd build && \
    cmake .. -DBUILD_SHARED_LIBS=ON && \
    make -j && \
    make install && \
    cd /tmp && \
    rm -fr /tmp/v0.4.0.tar.gz /tmp/glog-0.4.0

# protobuf v.3.13.0
RUN cd /tmp && \
    wget https://github.com/protocolbuffers/protobuf/releases/download/v3.13.0/protobuf-all-3.13.0.tar.gz && \
    tar zxvf protobuf-all-3.13.0.tar.gz && \
    cd protobuf-3.13.0 && \
    ./configure --enable-shared --disable-static && \
    make -j && \
    make install && \
    ldconfig && \
    cd /tmp && \
    rm -fr /tmp/protobuf-all-3.13.0.tar.gz /tmp/protobuf-3.13.0

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
        -DgRPC_SSL_PROVIDER=package \
        -DOPENSSL_ROOT_DIR=/usr/local \
        -DCMAKE_CXX_FLAGS="-fpermissive" && \
    make -j && \
    make install && \
    cd /tmp && \
    rm -fr /tmp/grpc

# patchelf
RUN cd /tmp && \
    git clone --depth=1 https://github.com/NixOS/patchelf.git && \
    cd patchelf && \
    ./bootstrap.sh && \
    ./configure && \
    make install -j && \
    rm -rf patchelf/

# install python deps for all
RUN for py in cp36-cp36m cp37-cp37m cp38-cp38 cp39-cp39; \
    do \
        echo "Installing deps for $py"; \
        /opt/python/$py/bin/pip3 install -U pip auditwheel grpcio grpcio_tools numpy wheel; \
    done
