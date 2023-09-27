apt update -y

apt install -y \
      ninja-build ragel libhwloc-dev libnuma-dev libpciaccess-dev vim wget curl \
      git g++ libgoogle-glog-dev cmake libopenmpi-dev default-jdk libcrypto++-dev \
      libboost-all-dev libxml2-dev
apt install -y xfslibs-dev libgnutls28-dev liblz4-dev maven openssl pkg-config \
      libsctp-dev gcc make python3 systemtap-sdt-dev libtool libyaml-cpp-dev \
      libc-ares-dev stow libfmt-dev diffutils valgrind doxygen python3-pip net-tools graphviz

git clone https://github.com/alibaba/libgrape-lite.git
cd libgrape-lite
git checkout v0.3.2
mkdir build && cd build && cmake ..
make -j && make install
cp /usr/local/lib/libgrape-lite.so /usr/lib/libgrape-lite.so

pushd /tmp && apt-get install -y -V ca-certificates lsb-release wget
curl -o apache-arrow-apt-source-latest.deb https://apache.jfrog.io/artifactory/arrow/$(lsb_release --id --short | tr 'A-Z' 'a-z')/apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb
apt-get install -y ./apache-arrow-apt-source-latest.deb
apt-get update && apt-get install -y libarrow-dev=6.0.1-1
popd

cd ../..
git clone https://github.com/alibaba/hiactor.git
cd hiactor && git checkout e16949ca53
git submodule update --init --recursive
./seastar/seastar/install-dependencies.sh
mkdir build && cd build
cmake -DHiactor_DEMOS=OFF -DHiactor_TESTING=OFF -DHiactor_DPDK=OFF -DHiactor_CXX_DIALECT=gnu++17 -DSeastar_CXX_FLAGS="-DSEASTAR_DEFAULT_ALLOCATOR -mno-avx512" ..
make -j && make install

echo "fs.aio-max-nr = 1048576" >> /etc/sysctl.conf
sysctl -p /etc/sysctl.conf