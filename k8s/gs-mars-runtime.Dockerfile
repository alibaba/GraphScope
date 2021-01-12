# the graphscope-runtime image is based on centos-7, including all necessary runtime
# dependencies for graphscope.

FROM reg.docker.alibaba-inc.com/mars/mars:v2.1

COPY gsa-deps.tar.gz /tmp/gsa-deps.tar.gz

RUN cd /tmp && tar xf /tmp/gsa-deps.tar.gz

RUN yum install -y --branch=current alicpp-gcc710-gcc.x86_64 && yum clean all && rm -fr /var/cache/yum

# yum install dependencies
RUN yum install -y autoconf automake \
        libcurl-devel libevent-devel libgsasl-devel libunwind-devel.x86_64 \
        libuuid-devel libxml2-devel libzip libzip-devel m4 minizip minizip-devel \
        make openssl-devel rsync telnet tools unzip wget which zip && \
    yum clean all && \
    rm -fr /var/cache/yum

# pip dependencies
RUN /opt/conda/bin/pip --no-cache-dir install auditwheel daemons grpcio-tools gremlinpython hdfs3 oss2 ipython kubernetes \
        libclang networkx==2.4 pandas parsec pycrypto pyorc pytest scipy  wheel && \
    /opt/conda/bin/pip --no-cache-dir install Cython --pre -U && \
    /opt/conda/bin/conda install gtest gflags glog zlib openmpi fmt double-conversion && \
    /opt/conda/bin/conda install -c conda-forge librdkafka && \
    /opt/conda/bin/conda clean -tipsy

RUN cd /tmp/gsa-deps && \
    bash cmake-3.19.1-Linux-x86_64.sh --prefix=/usr --skip-license

ENV LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:/usr/local/lib:/usr/local/lib64:/opt/conda/lib
ENV PATH=/usr/ali/alicpp/built/gcc-7.1.0/gcc-7.1.0/bin:${PATH}:/usr/local/bin:
ENV CPLUS_INCLUDE_PATH=/usr/ali/alicpp/built/gcc-7.1.0/gcc-7.1.0/include/c++/7.1.0:/opt/conda/include:${CPLUS_INCLUDE_PATH}
ENV LD_LIBRARY_PATH=/usr/ali/alicpp/built/gcc-7.1.0/gcc-7.1.0/lib:/usr/ali/alicpp/built/gcc-7.1.0/gcc-7.1.0/lib64:${LD_LIBRARY_PATH}
ENV CC=/usr/ali/alicpp/built/gcc-7.1.0/gcc-7.1.0/bin/gcc
ENV CXX=/usr/ali/alicpp/built/gcc-7.1.0/gcc-7.1.0/bin/g++

# apache arrow v1.0.1
RUN cd /tmp/gsa-deps && \
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
    make install


# boost v1.72.0
RUN cd /tmp/gsa-deps && \
    tar zxvf boost_1_72_0.tar.gz && \
    cd boost_1_72_0 && \
    ./bootstrap.sh && \
    ./b2 install link=shared runtime-link=shared variant=release threading=multi || true

# grpc v1.33.1
RUN cd /tmp/gsa-deps && \
    tar zxvf grpc.tar.gz &7 \
    cd grpc && \
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
        -DCMAKE_PREFIX_PATH=/opt/conda && \
    make -j`nproc` && \
    make install

# etcd v3.4.13
RUN cd /tmp/gsa-deps && \
    mkdir -p /tmp/gsa-deps/etcd-download-test && \
    export ETCD_VER=v3.4.13 && \
    tar xzvf /tmp/gsa-deps/etcd-${ETCD_VER}-linux-amd64.tar.gz -C /tmp/gsa-deps/etcd-download-test --strip-components=1 && \
    mv /tmp/gsa-deps/etcd-download-test/etcd /usr/local/bin/ && \
    mv /tmp/gsa-deps/etcd-download-test/etcdctl /usr/local/bin/


# folly v2020.10.19.00
RUN cd /tmp/gsa-deps && \
    tar zxvf folly-v2020.10.19.tar.gz && \
    cd folly-2020.10.19.00 && mkdir _build && \
    cd _build && \
    cmake -DBUILD_SHARED_LIBS=ON -DCMAKE_POSITION_INDEPENDENT_CODE=ON \
    -DCMAKE_PREFIX_PATH=/opt/conda .. && \
    make install -j


# kubectl v1.19.2
RUN cd /tmp/gsa-deps && \
    chmod +x ./kubectl && \
    mv ./kubectl /usr/local/bin/kubectl


RUN cd /tmp && \
    git clone https://github.com/alibaba/libgrape-lite.git && \
    cd libgrape-lite && \
    mkdir build && \
    cd build && \
    cmake .. -DCMAKE_INSTALL_PREFIX=/opt/graphscope && \
    make -j`nproc` && \
    make install && \
    cd /tmp && \
    git clone https://github.com/alibaba/libvineyard.git && \
    cd libvineyard && \
    git submodule update --init && \
    mkdir -p /tmp/libvineyard/build && \
    cd /tmp/libvineyard/build && \
    cmake .. -DCMAKE_PREFIX_PATH=/opt/graphscope \
             -DCMAKE_INSTALL_PREFIX=/opt/graphscope \
             -DBUILD_VINEYARD_PYPI_PACKAGES=ON \
             -DBUILD_SHARED_LIBS=ON \
             -DBUILD_VINEYARD_IO_OSS=ON && \
    make install vineyard_client_python -j && \
    cd /tmp/libvineyard && \
    python3 setup.py bdist_wheel && \
    cd dist && \
    /opt/conda/bin/pip install *.whl

# clean up
RUN rm /tmp/gsa-deps /tmp/gsa-deps.tar.gz

# for programming output
ENV LC_ALL=C
