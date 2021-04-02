#!/usr/bin/env bash
#
# A script to install dependencies for user to run GraphScope on local.

set -e
set -x
set -o pipefail

graphscope_home="$( cd "$(dirname "$0")/.." >/dev/null 2>&1 ; pwd -P )"

is_in_wsl=false && [[ ! -z "${IS_WSL}" || ! -z "${WSL_DISTRO_NAME}" ]] && is_in_wsl=true

##########################
# Install packages
# Globals:
#   platform
# Arguments:
#   None
##########################

# https://unix.stackexchange.com/questions/6345/how-can-i-get-distribution-name-and-version-number-in-a-simple-shell-script
function get_os_version() {
  if [ -f /etc/os-release ]; then
    # freedesktop.org and systemd
    . /etc/os-release
    platform="${NAME}"
    os_version="${VERSION_ID}"
  elif type lsb_release >/dev/null 2>&1; then
    # linuxbase.org
    platform=$(lsb_release -si)
    os_version=$(lsb_release -sr)
  elif [ -f /etc/lsb-release ]; then
    # For some versions of Debian/Ubuntu without lsb_release command
    . /etc/lsb-release
    platform="${DISTRIB_ID}"
    os_version="${DISTRIB_RELEASE}"
  elif [ -f /etc/debian_version ]; then
    # Older Debian/Ubuntu/etc.
    platform=Debian
    os_version=$(cat /etc/debian_version)
  elif [ -f /etc/centos-release ]; then
    # Older Red Hat, CentOS, etc.
    platform=CentOS
    os_version=$(cat /etc/centos-release | sed 's/.* \([0-9]\).*/\1/')
  else
    # Fall back to uname, e.g. "Linux <version>", also works for BSD, Darwin, etc.
    platform=$(uname -s)
    os_version=$(uname -r)
  fi
}

function check_os_compatibility() {
  if [[ "${is_in_wsl}" == true && -z "${WSL_INTEROP}" ]]; then
    echo "GraphScope not support to run on WSL1, please use WSL2."
    exit 1
  fi

  if [[ "${platform}" != *"Ubuntu"* ]]; then
    echo "This script is only available on Ubuntu"
    exit 1
  fi

  if [[ "${platform}" == *"Ubuntu"* && "$(echo ${os_version} | sed 's/\([0-9]\)\([0-9]\).*/\1\2/')" -lt "18" ]]; then
    echo "This script requires Ubuntu 18 or greater."
    exit 1
  fi

  echo "$(date '+%Y-%m-%d %H:%M:%S') preparing environment on '${platform}' '${os_version}'"
}

function check_dependencies_version() {
  # python
  if ! hash python3; then
    echo "Python3 is not installed"
    exit 1
  fi
  ver=$(python3 -V 2>&1 | sed 's/.* \([0-9]\).\([0-9]\).*/\1\2/')
  if [ "$ver" -lt "36" ]; then
    echo "GraphScope requires python 3.6 or greater."
    exit 1
  fi
}

function install_dependencies() {
  echo "$(date '+%Y-%m-%d %H:%M:%S') install dependencies."
  if [[ "${platform}" == *"Ubuntu"* ]]; then
    sudo apt-get update -y
    sudo apt install -y ca-certificates ccache cmake curl etcd libbrotli-dev \
      libbz2-dev libcurl4-openssl-dev libdouble-conversion-dev libevent-dev libgflags-dev \
      libboost-all-dev libgoogle-glog-dev libgrpc-dev libgrpc++-dev libgtest-dev libgsasl7-dev \
      libtinfo5 libkrb5-dev liblz4-dev libprotobuf-dev librdkafka-dev libre2-dev libsnappy-dev \
      libssl-dev libunwind-dev libutf8proc-dev libxml2-dev libz-dev libzstd-dev lsb-release maven \
      openjdk-8-jdk perl protobuf-compiler-grpc python3-pip uuid-dev wget zip zlib1g-dev

    # install apache-arrow
    wget https://apache.bintray.com/arrow/$(lsb_release --id --short | tr 'A-Z' 'a-z')/apache-arrow-archive-keyring-latest-$(lsb_release --codename --short).deb
    sudo apt install -y -V ./apache-arrow-archive-keyring-latest-$(lsb_release --codename --short).deb
    sudo apt update
    sudo apt install -y libarrow-dev=1.0.1-1 libarrow-python-dev=1.0.1-1

    # install zookeeper
    wget https://archive.apache.org/dist/zookeeper/zookeeper-3.4.14/zookeeper-3.4.14.tar.gz
    tar xf zookeeper-3.4.14.tar.gz -C /tmp/
    cp /tmp/zookeeper-3.4.14/conf/zoo_sample.cfg /tmp/zookeeper-3.4.14/conf/zoo.cfg
    sudo ln -s /tmp/zookeeper-3.4.14 /usr/local/zookeeper

    # rust
    wget --no-verbose https://golang.org/dl/go1.15.5.linux-amd64.tar.gz
    sudo tar -C /usr/local -xzf go1.15.5.linux-amd64.tar.gz
    curl -sf -L https://static.rust-lang.org/rustup.sh | sudo sh -s -- -y --profile minimal --default-toolchain 1.48.0
    source ~/.cargo/env

    # install python packages for vineyard codegen
    pip3 install -U pip --user
    pip3 install libclang parsec setuptools wheel twine --user
  fi

  check_dependencies_version
}

function install_libgrape-lite() {
  export LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:/usr/local/lib
  git clone -b master --single-branch --depth=1 https://github.com/alibaba/libgrape-lite.git /tmp/libgrape-lite
  pushd /tmp/ibgrape-lite
  mkdir build && cd build
  cmake ..
  make -j`nproc`
  sudo make install
  popd
}

function install_vineyard() {
  export LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:/usr/local/lib
  git clone -b v0.1.14 --single-branch --depth=1 https://github.com/alibaba/libvineyard.git /tmp/libvineyard
  pushd /tmp/libvineyard
  git submodule update --init
  mkdir build && cd build
  cmake .. -DBUILD_VINEYARD_PYPI_PACKAGES=ON -DBUILD_SHARED_LIBS=ON -DBUILD_VINEYARD_IO_OSS=ON
  make -j`nproc`
  make vineyard_client_python -j`nproc`
  sudo make install
  python3 setup.py bdist_wheel
  pip3 install ./dist/*.whl
  popd
}

function build_graphscope() {
  # build GraphScope GAE
  export LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:/usr/local/lib
  cd ${graphscope_home}
  mkdir analytical_engine/build && pushd analytical_engine/build
  cmake ..
  make -j`nproc`
  sudo make install
  popd

  # build GraphScope GIE
  export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
  export PATH=${JAVA_HOME}/bin:${PATH}:/usr/local/go/bin:/usr/local/zookeeper/bin:/usr/share/maven/bin
  export GRAPHSCOPE_PREFIX=/tmp/graphscope_prefix
  # build frontend coordinator graph-manager
  pushd interactive_engine
  mvn clean package -DskipTests -Pjava-release --quiet
  popd
  # build executor
  pushd interactive_engine/src/executor
  cargo build --all --release
  popd
  # copy dependencies into GRAPHSCOPE_PREFIX
  mkdir -p ${GRAPHSCOPE_PREFIX}/pid ${GRAPHSCOPE_PREFIX}/logs
  # copy mvn package
  cp ./interactive_engine/src/instance-manager/target/0.0.1-SNAPSHOT.tar.gz ${GRAPHSCOPE_PREFIX}/0.0.1-instance-manager-SNAPSHOT.tar.gz
  cp ./interactive_engine/src/assembly/target/0.0.1-SNAPSHOT.tar.gz ${GRAPHSCOPE_PREFIX}/0.0.1-SNAPSHOT.tar.gz
  tar -xf ${GRAPHSCOPE_PREFIX}/0.0.1-SNAPSHOT.tar.gz -C ${GRAPHSCOPE_PREFIX}/
  tar -xf ${GRAPHSCOPE_PREFIX}/0.0.1-instance-manager-SNAPSHOT.tar.gz -C ${GRAPHSCOPE_PREFIX}/
  # coordinator
  mkdir -p ${GRAPHSCOPE_PREFIX}/coordinator
  cp -r ./interactive_engine/src/coordinator/target ${GRAPHSCOPE_PREFIX}/coordinator/
  # frontend
  mkdir -p ${GRAPHSCOPE_PREFIX}/frontend/frontendservice
  cp -r ./interactive_engine/src/frontend/frontendservice/target ${GRAPHSCOPE_PREFIX}/frontend/frontendservice/
  # executor
  mkdir -p ${GRAPHSCOPE_PREFIX}/conf
  cp ./interactive_engine/src/executor/target/release/executor ${GRAPHSCOPE_PREFIX}/bin/executor
  cp ./interactive_engine/src/executor/store/log4rs.yml ${GRAPHSCOPE_PREFIX}/conf/log4rs.yml

  # install GraphScope client
  export WITH_LEARNING_ENGINE=ON
  pushd python
  pip3 install -U setuptools
  pip3 install -r requirements.txt -r requirements-dev.txt
  python3 setup.py bdist_wheel
  pip3 install -U ./dist/*.whl
  popd

  # install GraphScope coordinator
  pushd coordinator
  pip3 install -r requirements.txt -r requirements-dev.txt
  python3 setup.py bdist_wheel
  pip3 install -U ./dist/*.whl
  popd
}


get_os_version

check_os_compatibility

install_dependencies

install_libgrape-lite

install_vienyard

build_graphscope

echo "The script has successfully prepared an local environment for GraphScope."
echo "Now you are ready to have fun with GraphScope."

set +x
set +e
set +o pipefail
