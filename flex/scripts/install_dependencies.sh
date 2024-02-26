#!/bin/bash
set -e
# accept at most one argument, parallelism
if [ $# -gt 1 ]; then
    echo "Usage: $0 [parallelism]"
    exit 1
fi
parallelism=$(nproc)
if [ $# -eq 1 ]; then
    parallelism=$1
fi
echo "parallelism: $parallelism"

sudo apt update -y

sudo apt install -y \
      ninja-build ragel libhwloc-dev libnuma-dev libpciaccess-dev vim wget curl \
      git g++ libunwind-dev libgoogle-glog-dev cmake libopenmpi-dev default-jdk libcrypto++-dev \
      libboost-all-dev libxml2-dev protobuf-compiler libprotobuf-dev libncurses5-dev
sudo apt install -y xfslibs-dev libgnutls28-dev liblz4-dev maven openssl pkg-config \
      libsctp-dev gcc make python3 systemtap-sdt-dev libtool libyaml-cpp-dev \
      libc-ares-dev stow libfmt-dev diffutils valgrind doxygen python3-pip net-tools graphviz

pushd /tmp
git clone https://github.com/alibaba/libgrape-lite.git
cd libgrape-lite
git checkout v0.3.2
mkdir build && cd build && cmake .. -DBUILD_LIBGRAPELITE_TESTS=OFF
make -j ${parallelism} && sudo make install
sudo cp /usr/local/lib/libgrape-lite.so /usr/lib/libgrape-lite.so
popd && rm -rf /tmp/libgrape-lite

pushd /tmp && sudo apt-get install -y -V ca-certificates lsb-release wget
curl -o apache-arrow-apt-source-latest.deb https://apache.jfrog.io/artifactory/arrow/$(lsb_release --id --short | tr 'A-Z' 'a-z')/apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb
sudo apt-get install -y ./apache-arrow-apt-source-latest.deb
sudo apt-get update && sudo apt-get install -y libarrow-dev=8.0.0-1
popd && rm -rf /tmp/apache-arrow-apt-source-latest.deb

pushd /tmp
git clone https://github.com/alibaba/hiactor.git -b v0.1.1 --single-branch
cd hiactor && git submodule update --init --recursive
sudo bash ./seastar/seastar/install-dependencies.sh
mkdir build && cd build
cmake -DHiactor_DEMOS=OFF -DHiactor_TESTING=OFF -DHiactor_DPDK=OFF -DHiactor_CXX_DIALECT=gnu++17 -DSeastar_CXX_FLAGS="-DSEASTAR_DEFAULT_ALLOCATOR -mno-avx512" ..
make -j ${parallelism} && sudo make install
popd && rm -rf /tmp/hiactor

sudo sh -c 'echo "fs.aio-max-nr = 1048576" >> /etc/sysctl.conf'
sudo sysctl -p /etc/sysctl.conf