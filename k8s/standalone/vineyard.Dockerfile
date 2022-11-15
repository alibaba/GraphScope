FROM centos:7.9.2009

RUN yum install -y centos-release-scl-rh perl which sudo wget libunwind-devel && \
    INSTALL_PKGS="devtoolset-10-gcc-c++ rh-python38-python-pip" && \
    yum install -y --setopt=tsflags=nodocs $INSTALL_PKGS && \
    rpm -V $INSTALL_PKGS && \
    yum -y clean all --enablerepo='*' && \
    rm -rf /var/cache/yum

SHELL [ "/usr/bin/scl", "enable", "devtoolset-10", "rh-python38" ]

RUN python3 -m pip install libclang etcd-distro

ENV LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:/usr/local/lib:/usr/local/lib64

COPY ./download /tmp/

RUN cd /tmp && \
    wget -q https://github.com/Kitware/CMake/releases/download/v3.19.1/cmake-3.19.1-Linux-x86_64.sh && \
    bash cmake-3.19.1-Linux-x86_64.sh --prefix=/usr --skip-license && \
    rm -f /tmp/cmake-3.19.1-Linux-x86_64.sh

# install openmpi v4.0.5 to /opt/openmpi and link to /usr/local
RUN cd /tmp && \
    wget -q https://download.open-mpi.org/release/open-mpi/v4.0/openmpi-4.0.5.tar.gz && \
    tar zxvf openmpi-4.0.5.tar.gz && \
    cd openmpi-4.0.5 && \
    ./configure --enable-mpi-cxx --disable-dlopen --prefix=/usr/local  && \
    make -j`nproc` && \
    make install && \
    rm -rf /tmp/openmpi-4.0.5 /tmp/openmpi-4.0.5.tar.gz

# GLOG
RUN cd /tmp && \
    wget -q https://github.com/google/glog/archive/v0.4.0.tar.gz && \
    tar zxvf v0.4.0.tar.gz && \
    cd glog-0.4.0 && \
    cmake . -DBUILD_SHARED_LIBS=ON && \
    make -j`nproc` && \
    make install && \
    rm -rf /tmp/v0.4.0.tar.gz /tmp/glog-0.4.0

# libgrape-lite, required by vineyard
RUN cd /tmp && \
    git clone -b master https://github.com/alibaba/libgrape-lite.git --depth=1 && \
    cd libgrape-lite && \
    cmake . && \
    make -j`nproc` && \
    make install && \
    rm -rf /tmp/libgrape-lite

# apache arrow v7.0.0
RUN cd /tmp && \
    wget -q https://github.com/apache/arrow/archive/apache-arrow-7.0.0.tar.gz && \
    tar zxvf apache-arrow-7.0.0.tar.gz && \
    cd arrow-apache-arrow-7.0.0 && \
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
    make -j`nproc` && \
    make install && \
    rm -rf /tmp/arrow-apache-arrow-7.0.0 /tmp/apache-arrow-7.0.0.tar.gz


# gflags v2.2.2
RUN cd /tmp && \
    wget -q https://github.com/gflags/gflags/archive/v2.2.2.tar.gz && \
    tar zxvf v2.2.2.tar.gz && \
    cd gflags-2.2.2 && \
    cmake . -DBUILD_SHARED_LIBS=ON && \
    make -j`nproc` && \
    make install && \
    rm -rf /tmp/v2.2.2.tar.gz /tmp/gflags-2.2.2

# Boost 1.73.1, required by vineyard
RUN cd /tmp && \
    wget -q https://boostorg.jfrog.io/artifactory/main/release/1.73.0/source/boost_1_73_0.tar.gz && \
    tar zxf boost_1_73_0.tar.gz && \
    cd boost_1_73_0 && \
    ./bootstrap.sh --with-libraries=system,filesystem,context,program_options,regex,thread,random,chrono,atomic,date_time && \
    ./b2 install link=shared runtime-link=shared variant=release threading=multi && \
    rm -rf /tmp/boost_1_73_0 /tmp/boost_1_73_0.tar.gz
    # bcp --boost=./download/boost_1_73_0 system filesystem context program_options regex thread random chrono atomic date_time boost

# OpenSSL 1.1.1, required by vineyard
RUN cd /tmp && \
    wget -q https://github.com/openssl/openssl/archive/OpenSSL_1_1_1h.tar.gz && \
    tar zxvf OpenSSL_1_1_1h.tar.gz && \
    cd openssl-OpenSSL_1_1_1h && \
    ./config -fPIC -shared && \
    make -j`nproc` && \
    make install && \
    rm -rf /tmp/OpenSSL_1_1_1h.tar.gz /tmp/openssl-OpenSSL_1_1_1h

# zlib v1.2.11, required by vineyard
RUN cd /tmp && \
    wget -q https://github.com/madler/zlib/archive/v1.2.11.tar.gz && \
    tar zxvf v1.2.11.tar.gz && \
    cd zlib-1.2.11 && \
    cmake . -DBUILD_SHARED_LIBS=ON && \
    make -j`nproc` && \
    make install && \
    rm -rf /tmp/v1.2.11.tar.gz /tmp/zlib-1.2.11

# protobuf v.3.13.0
RUN cd /tmp && \
    wget -q https://github.com/protocolbuffers/protobuf/releases/download/v3.13.0/protobuf-all-3.13.0.tar.gz && \
    tar zxvf protobuf-all-3.13.0.tar.gz && \
    cd protobuf-3.13.0 && \
    ./configure --enable-shared --disable-static && \
    make -j`nproc` && \
    make install && \
    ldconfig && \
    rm -rf /tmp/protobuf-all-3.13.0.tar.gz /tmp/protobuf-3.13.0

# grpc v1.33.1
RUN cd /tmp && \
    git clone --depth 1 --branch v1.33.1 https://github.com/grpc/grpc.git && \
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
    make -j`nproc` && \
    make install && \
    rm -rf /tmp/grpc

# Vineyard
RUN cd /tmp && \
    git clone -b v0.10.2 https://github.com/v6d-io/v6d.git --depth=1 && \
    cd v6d && \
    git submodule update --init && \
    cmake . -DCMAKE_INSTALL_PREFIX=/usr/local \
        -DBUILD_VINEYARD_TESTS=OFF \
        -DBUILD_VINEYARD_PYTHON_BINDINGS=OFF && \
    make -j`nproc` && \
    make install && \
    rm -rf /tmp/v6d

RUN strip /usr/local/bin/vineyardd /usr/local/lib/libvineyard*
RUN rm -rf /tmp/*

RUN useradd -m graphscope -u 1001 \
    && echo 'graphscope ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers

# shanghai zoneinfo
ENV TZ=Asia/Shanghai
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && \
    echo '$TZ' > /etc/timezone

# for programming output
RUN localedef -c -f UTF-8 -i en_US en_US.UTF-8
ENV LANG='en_US.UTF-8' LANGUAGE='en_US:en' LC_ALL='en_US.UTF-8'

# Enable rh-python, devtoolsets-10 binary
COPY entrypoint.sh /usr/bin/entrypoint.sh
RUN chmod +x /usr/bin/entrypoint.sh
ENTRYPOINT [ "/usr/bin/entrypoint.sh" ]
