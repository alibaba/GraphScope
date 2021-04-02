#!/usr/bin/env bash
#
# A script to install dependencies of GraphScope.

set -e
set -x
set -o pipefail

is_in_wsl=false && [[ ! -z "${IS_WSL}" || ! -z "${WSL_DISTRO_NAME}" ]] && is_in_wsl=true

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
    sudo ln -s /tmp/zookeeper-3.4.14 /usr/local/zookeeper || true

    # rust
    wget --no-verbose https://golang.org/dl/go1.15.5.linux-amd64.tar.gz
    sudo tar -C /usr/local -xzf go1.15.5.linux-amd64.tar.gz
    curl -sf -L https://static.rust-lang.org/rustup.sh | sh -s -- -y --profile minimal --default-toolchain 1.48.0
    # source ~/.cargo/env

    # install python packages for vineyard codegen
    pip3 install -U pip --user
    pip3 install libclang parsec setuptools wheel twine --user
  fi

  check_dependencies_version
}

get_os_version

check_os_compatibility

install_dependencies

echo "The script has successfully install dependencies for GraphScope."

set +x
set +e
set +o pipefail
