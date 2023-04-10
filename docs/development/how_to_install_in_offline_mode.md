# Install GraphScope in offline mode

Some users may face the problem that they need to install GraphScope on an environment that has no internet access.

We provide some sample solutions to address that problems.

### Install from wheels

Users could download the all the wheels along with its dependencies of graphscope for offline usage.

1. Create a requirements.txt file that lists all of the packages you need. You can do this by running the following command:

    `echo "graphscope" > requirements.txt`

2. Download the wheels for all of the packages listed in the requirements.txt file. You can do this by running the following command:

    `pip download -r requirements.txt`

3. Copy the wheels to a folder on your computer that you can access offline.

4. When you need to use the packages offline, you can install them by running the following command:

    `pip install --no-index --find-links=./wheels .`

This will install all of the packages listed in the `requirements.txt` file from the wheels that you have downloaded.

### Install from source

We give an example to start with a basic operating system, download all dependencies for offline use, and the compile options to compile and install dependencies and graphscope analytical engine.

The operating system: CentOS 7

Dependencies:

- GCC 8.3.1
- make 3.82
- which  (boost dependency)
- perl  (boost dependency)
- Python 3.8  (vineyard dependency)
- cmake v3.24.3
- openmpi 4.0.5
- gflags 2.2.2
- glog 0.6.0
- apache-arrow 10.0.1
- boost 1.74.0
- openssl 1.1.1
- zlib 1.2.11
- protobuf 21.9
- grpc 1.49.1
- libgrape-lite master
- vineyard 0.13.5
- libclang (python package)

Dependency location for downloading:
- [Third party dependencies](https://graphscope.oss-cn-beijing.aliyuncs.com/dependencies/dependencies.tar.gz)
- [libgrape-lite](https://graphscope.oss-cn-beijing.aliyuncs.com/dependencies/libgrape-lite.tar.gz)

And you could get the source code of GraphScope on [github](https://github.com/alibaba/graphscope)


#### Download dependencies and GraphScope

```bash
curl -LO https://graphscope.oss-cn-beijing.aliyuncs.com/dependencies/dependencies.tar.gz
curl -LO https://graphscope.oss-cn-beijing.aliyuncs.com/dependencies/libgrape-lite.tar.gz
git clone https://github.com/alibaba/graphscope
tar czf graphscope.tar.gz graphscope
```

#### Copy all files to the destination environment and extract them to a location

```
mkdir -p workdir
tar xzf dependencies.tar.gz -C workdir/
tar xzf libgrape-lite.tar.gz -C workdir/dependencies/
tar xzf graphscope.tar.gz -C workdir/dependencies/
```

## Install basic packages and gcc-g++, python

```bash
yum install centos-release-scl-rh -y
yum install which perl make devtoolset-8-gcc-c++ rh-python38-python-pip -y
source /opt/rh/devtoolset-8/enable
source /opt/rh/rh-python38/enable
```



## Install third-party dependencies and GraphScope Analytical Engine

```bash
#!/bin/bash
set -euxo pipefail
# This directory is the path to the `dependencies` directory in the previous step
export DIR=/data/dependencies
# This would be the install prefix of dependencies and GraphScope
export PREFIX=/data/install

mkdir -p ${PREFIX}
export PATH=${PREFIX}/bin/:${PATH}/
export LD_LIBRARY_PATH=${LD_LIBRARY_PATH:-""}:${PREFIX}/lib:${PREFIX}/lib64

# Install libclang
python3 -m pip install ${DIR}/libclang-16.0.0-py2.py3-none-manylinux2010_x86_64.whl

# Install cmake
bash ${DIR}/cmake-3.24.3-linux-x86_64.sh --skip-license --prefix=${PREFIX}

# Install gflags
cd ${DIR}/gflags-2.2.2
cmake . -DBUILD_SHARED_LIBS=ON \
    -DCMAKE_PREFIX_PATH="${PREFIX}" \
    -DCMAKE_INSTALL_PREFIX="${PREFIX}"
make -j && make install

# Install glog
cd ${DIR}/glog-0.6.0
cmake . -DBUILD_SHARED_LIBS=ON \
    -DCMAKE_PREFIX_PATH=${PREFIX} \
    -DCMAKE_INSTALL_PREFIX=${PREFIX}
make -j && make install

# Install boost
cd ${DIR}/boost_1_74_0
./bootstrap.sh --prefix=${PREFIX} --with-libraries=system,filesystem,context,program_options,regex,thread,random,chrono,atomic,date_time
./b2 install link=shared runtime-link=shared variant=release threading=multi

# Install openssl
cd ${DIR}/openssl-OpenSSL_1_1_1h
./config --prefix="${PREFIX}" -fPIC -shared
make -j
make install

# Install apache-arrow
cd ${DIR}/arrow-apache-arrow-10.0.1
mkdir -p build && cd build
cmake ../cpp \
    -DCMAKE_PREFIX_PATH=${PREFIX} \
    -DCMAKE_INSTALL_PREFIX=${PREFIX} \
    -DARROW_COMPUTE=ON \
    -DARROW_WITH_UTF8PROC=OFF \
    -DARROW_CSV=ON \
    -DARROW_FILESYSTEM=ON \
    -DARROW_IPC=ON \
    -DARROW_BUILD_BENCHMARKS=OFF \
    -DARROW_BUILD_EXAMPLES=OFF \
    -DARROW_BUILD_INTEGRATION=OFF \
    -DARROW_BUILD_UTILITIES=OFF \
    -DARROW_BUILD_TESTS=OFF \
    -DARROW_ENABLE_TIMING_TESTS=OFF \
    -DARROW_FUZZING=OFF \
    -DARROW_JEMALLOC=OFF \
    -DARROW_BUILD_SHARED=ON \
    -DARROW_BUILD_STATIC=OFF
make -j && make install

# Install openmpi
cd ${DIR}/openmpi-4.0.5
./configure --enable-mpi-cxx --disable-dlopen --prefix=${PREFIX}
make -j$(nproc)
make install

# Install protobuf
cd ${DIR}/protobuf-21.9
./configure --prefix="${PREFIX}" --enable-shared --disable-static
make -j$(nproc)
make install

# Install zlib
cd ${DIR}/zlib-1.2.11
cmake . -DCMAKE_INSTALL_PREFIX="${PREFIX}" \
    -DCMAKE_PREFIX_PATH="${PREFIX}" \
    -DBUILD_SHARED_LIBS=ON
make -j$(nproc)
make install

# Install grpc
cd ${DIR}/grpc
cmake . -DCMAKE_INSTALL_PREFIX="${PREFIX}" \
    -DCMAKE_PREFIX_PATH="${PREFIX}" \
    -DBUILD_SHARED_LIBS=ON \
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
    -DOPENSSL_ROOT_DIR="${PREFIX}" \
    -DCMAKE_CXX_FLAGS="-fpermissive"
make -j
make install

# Install libgrape-lite
cd ${DIR}/libgrape-lite
cmake . -DCMAKE_PREFIX_PATH="${PREFIX}" \
    -DCMAKE_INSTALL_PREFIX="${PREFIX}"
make -j
make install

# Install vineyard
cd ${DIR}/v6d-0.13.4
cmake . -DCMAKE_PREFIX_PATH="${PREFIX}" \
    -DCMAKE_INSTALL_PREFIX="${PREFIX}" \
    -DBUILD_VINEYARD_TESTS=OFF \
    -DBUILD_SHARED_LIBS=ON \
    -DBUILD_VINEYARD_PYTHON_BINDINGS=OFF
make -j4
make install

# Install graphscope
cd ${DIR}/graphscope/analytical_engine
mkdir -p build && cd build
cmake ..  -DCMAKE_PREFIX_PATH=${PREFIX} \
    -DCMAKE_INSTALL_PREFIX=${PREFIX} \
    -DNETWORKX=OFF \
    -DENABLE_JAVA_SDK=OFF \
    -DBUILD_TESTS=OFF
make -j
make install

echo "Install complete, you could find the binary file in the ${PREFIX}/bin"
```
