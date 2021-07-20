#!/usr/bin/env bash
#
# A script to install dependencies of GraphScope.

set -e
set -o pipefail

# color
readonly RED="\033[0;31m"
readonly YELLOW="\033[1;33m"
readonly NC="\033[0m" # No Color
# emoji
readonly MUSCLE="\U1f4aa"

readonly GRAPE_BRANCH="master" # libgrape-lite branch
readonly V6D_BRANCH="main-v0.2.5" # vineyard branch
readonly LLVM_VERSION=9 # llvm version we use in Darwin platform

readonly SOURCE_DIR="$( cd "$(dirname $0)/.." >/dev/null 2>&1 ; pwd -P )"
readonly NUM_PROC=$( $(command -v nproc &> /dev/null) && echo $(nproc) || echo $(sysctl -n hw.physicalcpu) )
IS_IN_WSL=false && [[ ! -z "${IS_WSL}" || ! -z "${WSL_DISTRO_NAME}" ]] && IS_IN_WSL=true
readonly IS_IN_WSL
INSTALL_PREFIX=/usr/local
PLATFORM=
OS_VERSION=
VERBOSE=false

##########################
# Output useage information.
# Globals:
#   None
# Arguments:
#   None
##########################
usage() {
cat <<END
  A script to install dependencies of GraphScope or deploy GraphScope locally.

  Usage: deploy_local [options] [command]

  Options:

    --prefix <path>      install prefix of GraphScope, default is /usr/local
    --verbose            ouput the debug logging information
    -h, --help           output help information

  Commands:

    install_deps         install dependencies of GraphScope
    deploy               deploy GraphScope locally
END
}

err() {
  echo -e "[$(date +'%Y-%m-%dT%H:%M:%S%z')]: [${RED}ERROR${NC}] $*" >&2
}

warning() {
  echo -e "[$(date +'%Y-%m-%dT%H:%M:%S%z')]: [${YELLOW}WARNING${NC}] $*" >&1
}

log() {
  echo -e "[$(date +'%Y-%m-%dT%H:%M:%S%z')]: [${MUSCLE}] $*" >&1
}

##########################
# Get os platform and version
# Globals:
#   PLATFORM
#   OS_VERSION
# Arguments:
#   None
# Refer:
# https://unix.stackexchange.com/questions/6345/how-can-i-get-distribution-name-and-version-number-in-a-simple-shell-script
##########################
get_os_version() {
  if [ -f /etc/os-release ]; then
    # freedesktop.org and systemd
    . /etc/os-release
    PLATFORM="${NAME}"
    OS_VERSION="${VERSION_ID}"
  elif type lsb_release >/dev/null 2>&1; then
    # linuxbase.org
    PLATFORM=$(lsb_release -si)
    OS_VERSION=$(lsb_release -sr)
  elif [ -f /etc/lsb-release ]; then
    # For some versions of Debian/Ubuntu without lsb_release command
    . /etc/lsb-release
    PLATFORM="${DISTRIB_ID}"
    OS_VERSION="${DISTRIB_RELEASE}"
  elif [ -f /etc/debian_version ]; then
    # Older Debian/Ubuntu/etc.
    PLATFORM=Debian
    OS_VERSION=$(cat /etc/debian_version)
  elif [ -f /etc/centos-release ]; then
    # Older Red Hat, CentOS, etc.
    PLATFORM=CentOS
    OS_VERSION=$(cat /etc/centos-release | sed 's/.* \([0-9]\).*/\1/')
  else
    # Fall back to uname, e.g. "Linux <version>", also works for BSD, Darwin, etc.
    PLATFORM=$(uname -s)
    OS_VERSION=$(uname -r)
  fi
  readonly PLATFORM
  readonly OS_VERSION
}

##########################
# Check the compatibility of platform and script.
# Globals:
#   None
# Arguments:
#   None
# Outputs:
#   Writes error message to stderr
# Returns:
#   non-zero on error.
##########################
check_os_compatibility() {
  if [[ "${is_in_wsl}" == true && -z "${WSL_INTEROP}" ]]; then
    err "The platform is WSL1. GraphScope not support to run on WSL1, please use WSL2."
    exit 1
  fi

  if [[ "${PLATFORM}" != *"Ubuntu"* && "${PLATFORM}" != *"Darwin"* ]]; then
    err "The platform is not Ubuntu or MacOS. This script is only available on Ubuntu/MacOS"
    exit 1
  fi

  if [[ "${PLATFORM}" == *"Ubuntu"* && "$(echo ${OS_VERSION} | sed 's/\([0-9]\)\([0-9]\).*/\1\2/')" -lt "20" ]]; then
    err "The version of Ubuntu is ${OS_VERSION}. This script requires Ubuntu 20 or greater."
    exit 1
  fi

  log "Runing on ${PLATFORM} ${OS_VERSION}"
}

##########################
# Check the dependencies of install_deps command.
# Globals:
#   None
# Arguments:
#   None
# Outputs:
#   Writes error message to stderr
# Returns:
#   non-zero on error.
##########################
check_dependencies_version() {
  log "Checking dependencies of install_deps command."

  if ! hash sudo; then
    err "sudo is not installed."
    exit 1
  fi
  # python3
  if ! hash python3; then
    err "Python3 is not installed."
    exit 1
  fi
  ver=$(python3 -V 2>&1 | sed 's/.* \([0-9]\).\([0-9]\).*/\1\2/')
  if [ "${ver}" -lt "36" ]; then
    err "GraphScope requires python 3.6 or greater."
    exit 1
  fi

  if [[ "${PLATFORM}" == *"Darwin"* ]]; then
    if ! hash brew; then
      err "Homebrew is not installed. Please install Homebrew: https://docs.brew.sh/Installation."
      exit 1
    fi
  fi
}

##########################
# Check the dependencies of deploy command.
# Globals:
#   None
# Arguments:
#   None
# Outputs:
#   Writes error message to stderr
# Returns:
#   non-zero on error.
##########################
check_dependencies_of_deploy() {
  log "Checking dependencies of deploy command."

  err_msg="could not be found, you can install it manually, or via install_deps command."
  if ! hash sudo; then
    err "sudo is not installed."
    exit 1
  fi
  if ! command -v python3 &> /dev/null; then
    err "Python3 is not installed."
    exit 1
  fi
  ver=$(python3 -V 2>&1 | sed 's/.* \([0-9]\).\([0-9]\).*/\1\2/')
  if [ "${ver}" -lt "36" ]; then
    err "GraphScope requires python 3.6 or greater. Current version is ${python3 -V}"
    exit 1
  fi
  # java
  if ! command -v java &> /dev/null; then
    err "JDK ${err_msg}. GraphScope require jdk8."
    exit 1
  fi
  # if "${ver}" != "8"; then
  #   err "GraphScope requires jdk8. Current version is jdk${ver}."
  #   exit 1
  # fi
  if ! command -v mvn &> /dev/null; then
    echo "maven ${err_msg}"
    exit 1
  fi
  if ! command -v cargo &> /dev/null; then
    echo "cargo ${err_msg} or source ~/.cargo/env"
    exit 1
  fi
  if ! command -v go &> /dev/null; then
    echo "go ${err_msg}"
    exit 1
  fi
  if [[ "${PLATFORM}" == *"Darwin"* ]]; then
    if ! command -v clang &> /dev/null; then
      echo "clang ${err_msg}. GraphScope require clang >=8, <=10."
      exit 1
    fi
    ver=$(clang -v 2>&1 | head -n 1 | sed 's/.* \([0-9]*\)\..*/\1/')
    if [[ "${ver}" -lt "8" || "${ver}" -gt "10" ]]; then
      echo "GraphScope requires clang >=8, <=10   MacOS. Current version is ${ver}."
      exit 1
    fi
  fi
}

##########################
# Write out the related environment export statements to file.
# Globals:
#   PLATFORM
#   SOURCE_DIR
#   LLVM_VERSION
# Arguments:
#   None
# Outputs:
#   output environment export statements to file.
##########################
write_envs_config() {
  rm ${SOURCE_DIR}/gs_env || true
  if [[ "${PLATFORM}" == *"Darwin"* ]]; then
    {
      echo "export CC=/usr/local/opt/llvm@${LLVM_VERSION}/bin/clang"
      echo "export CXX=/usr/local/opt/llvm@${LLVM_VERSION}/bin/clang++"
      echo "export LDFLAGS=-L/usr/local/opt/llvm@${LLVM_VERSION}/lib"
      echo "export CPPFLAGS=-I/usr/local/opt/llvm@${LLVM_VERSION}/include"
      echo "export OPENSSL_ROOT_DIR=/usr/local/opt/openssl"
      echo "export OPENSSL_LIBRARIES=/usr/local/opt/openssl/lib"
      echo "export OPENSSL_SSL_LIBRARY=/usr/local/opt/openssl/lib/libssl.dylib"
      echo "export JAVA_HOME=/Library/Java/JavaVirtualMachines/adoptopenjdk-8.jdk/Contents/Home"
      echo "export PATH=/usr/local/opt/gnu-sed/libexec/gnubin:/usr/local/opt/llvm@${LLVM_VERSION}/bin:\${JAVA_HOME}/bin:\$PATH:/usr/local/zookeeper/bin"
    } >> ${SOURCE_DIR}/gs_env
  else
    {
      echo "export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64"
      echo "export PATH=\${JAVA_HOME}/bin:/usr/local/go/bin:$HOME/.cargo/bin:\$PATH:/usr/local/zookeeper/bin"
    } >> ${SOURCE_DIR}/gs_env
  fi
}

##########################
# Install denpendencies of GraphScope.
# Globals:
#   PLATFORM
#   SOURCE_DIR
# Arguments:
#   None
# Outputs:
#   output log to stdout, output error to stderr.
##########################
install_dependencies() {
  if [[ "${PLATFORM}" == *"Ubuntu"* ]]; then
    sudo apt-get update -y
    sudo apt install -y ca-certificates ccache cmake curl etcd libbrotli-dev \
      libbz2-dev libcurl4-openssl-dev libdouble-conversion-dev libevent-dev libgflags-dev \
      libboost-all-dev libgoogle-glog-dev libgrpc-dev libgrpc++-dev libgtest-dev libgsasl7-dev \
      libtinfo5 libkrb5-dev liblz4-dev libprotobuf-dev librdkafka-dev libre2-dev libsnappy-dev \
      libssl-dev libunwind-dev libutf8proc-dev libxml2-dev libz-dev libzstd-dev lsb-release maven \
      openjdk-8-jdk perl protobuf-compiler-grpc python3-pip uuid-dev wget zip zlib1g-dev

    log "Installing Go and rust."
    wget --no-verbose https://golang.org/dl/go1.15.5.linux-amd64.tar.gz -P /tmp
    sudo tar -C /usr/local -xzf /tmp/go1.15.5.linux-amd64.tar.gz
    curl -sf -L https://static.rust-lang.org/rustup.sh | sh -s -- -y --profile minimal --default-toolchain 1.48.0
    rm -fr /tmp/go1.15.5.linux-amd64.tar.gz

    write_envs_config
    source ${SOURCE_DIR}/gs_env

    log "Builing and installing apache-arrow."
    wget https://apache.jfrog.io/artifactory/arrow/$(lsb_release --id --short | tr 'A-Z' 'a-z')/apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb
    sudo apt install -y -V ./apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb
    sudo apt update -y
    sudo apt install -y libarrow-dev=3.0.0-1 libarrow-python-dev=3.0.0-1

    log "Installing fmt."
    wget https://github.com/fmtlib/fmt/archive/7.0.3.tar.gz -P /tmp
    tar xf /tmp/7.0.3.tar.gz -C /tmp/
    pushd /tmp/fmt-7.0.3
    mkdir build && cd build
    cmake .. -DBUILD_SHARED_LIBS=ON
    make -j${NUM_PROC}
    sudo make install
    popd
    rm -fr /tmp/7.0.3.tar.gz /tmp/fmt-7.0.3
  elif [[ "${PLATFORM}" == *"Darwin"* ]]; then
    brew install cmake double-conversion etcd protobuf apache-arrow openmpi \
                 boost glog gflags zstd snappy lz4 openssl@1.1 libevent fmt \
                 autoconf maven gnu-sed llvm@${LLVM_VERSION} wget

    # GraphScope require jdk8
    brew tap adoptopenjdk/openjdk
    brew install --cask adoptopenjdk8

    write_envs_config
    source ${SOURCE_DIR}/gs_env
  fi

  log "Installing folly."
  wget https://github.com/facebook/folly/archive/v2020.10.19.00.tar.gz -P /tmp
  tar xf /tmp/v2020.10.19.00.tar.gz -C /tmp/
  pushd /tmp/folly-2020.10.19.00
  mkdir _build && cd _build
  cmake -DBUILD_SHARED_LIBS=ON -DCMAKE_POSITION_INDEPENDENT_CODE=ON ..
  make -j${NUM_PROC}
  sudo make install
  popd
  rm -fr /tmp/v2020.10.19.00.tar.gz /tmp/folly-2020.10.19.00

  log "Installing zookeeper."
  wget https://archive.apache.org/dist/zookeeper/zookeeper-3.4.14/zookeeper-3.4.14.tar.gz -P /tmp
  tar xf /tmp/zookeeper-3.4.14.tar.gz -C /tmp/
  cp /tmp/zookeeper-3.4.14/conf/zoo_sample.cfg /tmp/zookeeper-3.4.14/conf/zoo.cfg
  sudo cp -r /tmp/zookeeper-3.4.14 /usr/local/zookeeper || true
  rm -fr /tmp/zookeeper-3.4.14*

  log "Installing python packages for vineyard codegen."
  pip3 install -U pip --user
  pip3 install grpcio-tools libclang parsec setuptools wheel twine --user
}

##########################
# Install libgrape-lite.
# Globals:
#   NUM_PROC
# Arguments:
#   None
# Outputs:
#   output log to stdout, output error to stderr.
##########################
install_libgrape-lite() {
  log "Building and installing libgrape-lite."
  export LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:/usr/local/lib
  git clone -b ${GRAPE_BRANCH} --single-branch --depth=1 \
      https://github.com/alibaba/libgrape-lite.git /tmp/libgrape-lite
  pushd /tmp/libgrape-lite
  mkdir build && cd build
  cmake ..
  make -j${NUM_PROC}
  sudo make install
  popd
  rm -fr /tmp/libgrape-lite
}

##########################
# Install vineyard.
# Globals:
#   PLATFORM
#   NUM_PROC
# Arguments:
#   None
# Outputs:
#   output log to stdout, output error to stderr.
##########################
install_vineyard() {
  log "Building and installing vineyard."
  git clone -b ${V6D_BRANCH} --single-branch --depth=1 \
      https://github.com/alibaba/libvineyard.git /tmp/libvineyard
  pushd /tmp/libvineyard
  git submodule update --init
  mkdir build && pushd build
  if [[ "${PLATFORM}" == *"Darwin"* ]]; then
    cmake .. -DBUILD_VINEYARD_PYTHON_BINDINGS=ON -DBUILD_SHARED_LIBS=ON \
             -DBUILD_VINEYARD_IO_OSS=ON -DBUILD_VINEYARD_TESTS=OFF
  else
    cmake .. -DBUILD_VINEYARD_PYPI_PACKAGES=ON -DBUILD_SHARED_LIBS=ON \
             -DBUILD_VINEYARD_IO_OSS=ON -DBUILD_VINEYARD_TESTS=OFF
  fi
  make -j${NUM_PROC}
  make vineyard_client_python -j${NUM_PROC}
  sudo make install
  popd

  # install vineyard-python
  python3 setup.py bdist_wheel
  pip3 install -U ./dist/*.whl --user

  # install vineyard-io
  pushd modules/io
  rm -rf build/lib.* build/bdist.*
  python3 setup.py bdist_wheel
  pip3 install -U ./dist/*.whl --user
  popd

  popd
  rm -fr /tmp/libvineyard
}

##########################
# Install GraphScope.
# Globals:
#   SOURCE_DIR
#   INSTALL_PREFIX
#   NUM_PROC
# Arguments:
#   None
# Outputs:
#   output log to stdout, output error to stderr.
##########################
install_graphscope() {
  log "Deploying GraphScope."
  pushd ${SOURCE_DIR}

  if [[ "${PLATFORM}" == *"Darwin"* ]]; then
    make install WITH_LEARNING_ENGINE=OFF INSTALL_PREFIX=${INSTALL_PREFIX}
  else
    make install WITH_LEARNING_ENGINE=ON INSTALL_PREFIX=${INSTALL_PREFIX}
  fi
  popd
}

##########################
# Main function for install_deps command.
# Globals:
#   VERBOSE
#   SOURCE_DIR
# Arguments:
#   None
# Outputs:
#   output log to stdout, output error to stderr.
##########################
install_deps() {
  if [ ${VERBOSE} = true ]; then
    set -x
  fi
  get_os_version

  check_os_compatibility

  check_dependencies_version

  install_dependencies

  succ_msg="Install dependencies successfully. The script had output the related
  environments to ${SOURCE_DIR}/gs_env.\nPlease run 'source ${SOURCE_DIR}/gs_env'
  before run deploy command."
  log ${succ_msg}
  if [ ${VERBOSE} = true ]; then
    set +x
  fi
}

##########################
# Main function for deploy command.
# Globals:
#   VERBOSE
#   INSTALL_PREFIX
# Arguments:
#   None
# Outputs:
#   output log to stdout, output error to stderr.
##########################
deploy() {
  if [ ${VERBOSE} = true ]; then
    set -x
  fi
  get_os_version

  check_os_compatibility

  check_dependencies_of_deploy

  install_libgrape-lite

  install_vineyard

  install_graphscope

  succ_msg="Deploy GraphScope successfully. Finally you need to run
  'export GRAPHSCOPE_PREFIX=${INSTALL_PREFIX}'\n
  Hope you have fun with GraphScope."
  log ${succ_msg}
  if [ ${VERBOSE} = true ]; then
    set +x
  fi
}

set -e
set -o pipefail

# parse argv
# TODO(acezen): when option and command is not illegal, warning and output usage.
# TODO(acezen): now the option need to specify before command, that's not user-friendly.
while test $# -ne 0; do
  arg=$1; shift
  case $arg in
    -h|--help) usage; exit ;;
    --prefix) INSTALL_PREFIX=$1; readonly INSTALL_PREFIX; shift ;;
    --verbose) VERBOSE=true; readonly VERBOSE; ;;
    install_deps) install_deps; exit;;
    deploy) deploy; exit;;
    *)
      ;;
  esac
done

set +e
set +o pipefail
