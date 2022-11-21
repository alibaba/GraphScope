#!/bin/bash
set -euxo pipefail

echo "Working directory is ${WORKDIR:="/tmp"}"

# cmake 3.24.3
echo "Installing cmake"
cd ${WORKDIR} && \
    wget -q https://github.com/Kitware/CMake/releases/download/v3.24.3/cmake-3.24.3-linux-x86_64.sh && \
    bash cmake-3.24.3-linux-x86_64.sh --prefix=/usr/local --skip-license && \
    rm -rf ${WORKDIR}/cmake-3.24.3-linux-x86_64.sh

# install openmpi v4.1.4
echo "Installing openmpi"
cd ${WORKDIR} && \
    wget -q https://download.open-mpi.org/release/open-mpi/v4.1/openmpi-4.1.4.tar.gz && \
    tar zxvf openmpi-4.1.4.tar.gz && \
    cd openmpi-4.1.4 && \
    ./configure --enable-mpi-cxx --disable-dlopen --prefix=/usr/local  && \
    make -j$(nproc) && \
    make install && \
    rm -rf ${WORKDIR}/openmpi-4.1.4 ${WORKDIR}/openmpi-4.1.4.tar.gz

# GLOG 0.6.0
echo "Installing glog"
cd ${WORKDIR} && \
    wget -q https://github.com/google/glog/archive/v0.6.0.tar.gz && \
    tar zxvf v0.6.0.tar.gz && \
    cd glog-0.6.0 && \
    cmake . -DBUILD_SHARED_LIBS=ON && \
    make -j$(nproc) && \
    make install && \
    rm -rf ${WORKDIR}/v0.6.0.tar.gz ${WORKDIR}/glog-0.6.0

# apache arrow v9.0.0
echo "Installing apache-arrow"
cd ${WORKDIR} && \
    wget -q https://github.com/apache/arrow/archive/apache-arrow-9.0.0.tar.gz && \
    tar zxvf apache-arrow-9.0.0.tar.gz && \
    cd arrow-apache-arrow-9.0.0 && \
    cmake ./cpp \
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
        -DARROW_PYTHON=OFF \
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
    make -j$(nproc) && \
    make install && \
    rm -rf ${WORKDIR}/arrow-apache-arrow-9.0.0 ${WORKDIR}/apache-arrow-9.0.0.tar.gz

# gflags v2.2.2
echo "Installing gflags"
cd ${WORKDIR} && \
    wget -q https://github.com/gflags/gflags/archive/v2.2.2.tar.gz && \
    tar zxvf v2.2.2.tar.gz && \
    cd gflags-2.2.2 && \
    cmake . -DBUILD_SHARED_LIBS=ON && \
    make -j$(nproc) && \
    make install && \
    rm -rf ${WORKDIR}/v2.2.2.tar.gz ${WORKDIR}/gflags-2.2.2

# Boost 1.74.0, required by vineyard
echo "Installing boost"
cd ${WORKDIR} && \
    wget -q https://boostorg.jfrog.io/artifactory/main/release/1.74.0/source/boost_1_74_0.tar.gz && \
    tar zxf boost_1_74_0.tar.gz && \
    cd boost_1_74_0 && \
    ./bootstrap.sh --with-libraries=system,filesystem,context,program_options,regex,thread,random,chrono,atomic,date_time && \
    ./b2 install link=shared runtime-link=shared variant=release threading=multi && \
    rm -rf ${WORKDIR}/boost_1_74_0 ${WORKDIR}/boost_1_74_0.tar.gz

# OpenSSL 1.1.1, required by vineyard
echo "Installing openssl"
cd ${WORKDIR} && \
    wget -q https://github.com/openssl/openssl/archive/OpenSSL_1_1_1h.tar.gz && \
    tar zxvf OpenSSL_1_1_1h.tar.gz && \
    cd openssl-OpenSSL_1_1_1h && \
    ./config -fPIC -shared && \
    make -j$(nproc) && \
    make install && \
    rm -rf ${WORKDIR}/OpenSSL_1_1_1h.tar.gz ${WORKDIR}/openssl-OpenSSL_1_1_1h

# zlib v1.2.11, required by vineyard
echo "Installing zlib"
cd ${WORKDIR} && \
    wget -q https://github.com/madler/zlib/archive/v1.2.11.tar.gz && \
    tar zxvf v1.2.11.tar.gz && \
    cd zlib-1.2.11 && \
    cmake . -DBUILD_SHARED_LIBS=ON && \
    make -j$(nproc) && \
    make install && \
    rm -rf ${WORKDIR}/v1.2.11.tar.gz ${WORKDIR}/zlib-1.2.11

# protobuf v.3.13.0
echo "Installing protobuf"
cd ${WORKDIR} && \
    wget -q https://github.com/protocolbuffers/protobuf/releases/download/v21.9/protobuf-all-21.9.tar.gz && \
    tar zxvf protobuf-all-21.9.tar.gz && \
    cd protobuf-21.9 && \
    ./configure --enable-shared --disable-static && \
    make -j$(nproc) && \
    make install && \
    ldconfig && \
    rm -rf ${WORKDIR}/protobuf-all-21.9.tar.gz ${WORKDIR}/protobuf-21.9

# grpc v1.49.1
echo "Installing grpc"
cd ${WORKDIR} && \
    git clone --depth 1 --branch v1.49.1 https://github.com/grpc/grpc.git && \
    cd grpc && \
    git submodule update --init && \
    cmake . -DBUILD_SHARED_LIBS=ON \
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
    make -j$(nproc) && \
    make install && \
    rm -rf ${WORKDIR}/grpc
