#!/usr/bin/env bash
#
# A script to install dependencies of GraphScope.

set -e
set -o pipefail

# color
readonly RED="\033[0;31m"
readonly YELLOW="\033[1;33m"
readonly GREEN="'\e[0;32m'"...
readonly NC="\033[0m" # No Color

readonly GRAPE_BRANCH="master" # libgrape-lite branch
readonly V6D_BRANCH="main-v0.2.5" # vineyard branch
readonly LLVM_VERSION=9 # llvm version we use in Darwin platform

readonly SOURCE_DIR="$( cd "$(dirname $0)/.." >/dev/null 2>&1 ; pwd -P )"
readonly NUM_PROC=$( $(command -v nproc &> /dev/null) && echo $(nproc) || echo $(sysctl -n hw.physicalcpu) )
IS_IN_WSL=false && [[ ! -z "${IS_WSL}" || ! -z "${WSL_DISTRO_NAME}" ]] && IS_IN_WSL=true
readonly IS_IN_WSL
INSTALL_PREFIX=/usr/local
BASIC_PACKGES_TO_INSTALL=
PLATFORM=
OS_VERSION=
VERBOSE=false
packages_to_install=()

err() {
  echo -e "${RED}[$(date +'%Y-%m-%dT%H:%M:%S%z')]: [ERROR] $*${NC}" >&2
}

warning() {
  echo -e "${YELLOW}[$(date +'%Y-%m-%dT%H:%M:%S%z')]: [WARNING] $*${NC}" >&1
}

log() {
  echo -e "[$(date +'%Y-%m-%dT%H:%M:%S%z')]: $*" >&1
}

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

##########################
# Get OS platform and version
# Globals:
#   PLATFORM
#   OS_VERSION
# Reference:
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
##########################
check_os_compatibility() {
  if [[ "${is_in_wsl}" == true && -z "${WSL_INTEROP}" ]]; then
    err "GraphScope does not support to run on WSL1, please use WSL2."
    exit 1
  fi

  if [[ "${PLATFORM}" != *"Ubuntu"* && "${PLATFORM}" != *"CentOS"* && "${PLATFORM}" != *"Darwin"* ]]; then
    err "The script is only support platforms of Ubuntu/CentOS/macOS"
    exit 1
  fi

  if [[ "${PLATFORM}" == *"Ubuntu"* && "$(echo ${OS_VERSION} | sed 's/\([0-9]\)\([0-9]\).*/\1\2/')" -lt "20" ]]; then
    err "The version of Ubuntu is ${OS_VERSION}. This script requires Ubuntu 20 or greater."
    exit 1
  fi

  if [[ "${PLATFORM}" == *"CentOS"* && "${OS_VERSION}" -lt "8" ]]; then
    err "The version of CentOS is ${OS_VERSION}. This script requires CentOS 8 or greater."
    exit 1
  fi

  log "Runing on ${PLATFORM} ${OS_VERSION}"
}

init_basic_packages() {
  if [[ "${PLATFORM}" == *"Ubuntu"* ]]; then
    BASIC_PACKGES_TO_INSTALL=(
      curl
      libbrotli-dev
      libbz2-dev
      libcurl4-openssl-dev
      libdouble-conversion-dev
      protobuf-compiler-grpc
      libevent-dev
      libgflags-dev
      libgoogle-glog-dev
      libgrpc-dev
      libgrpc++-dev
      libgtest-dev
      libgsasl7-dev
      libtinfo5
      libkrb5-dev
      liblz4-dev
      libprotobuf-dev
      libre2-dev
      libsnappy-dev
      libssl-dev
      libunwind-dev
      libutf8proc-dev
      libxml2-dev
      libz-dev
      libzstd-dev
      lsb-release
      zlib1g-dev
      uuid-dev
      wget
      zip
      perl
    )
  elif [[ "${PLATFORM}" == *"CentOS"* ]]; then
    BASIC_PACKGES_TO_INSTALL=(
      autoconf
      automake
      double-conversion-devel
      git
      zlib-devel
      libcurl-devel
      libevent-devel
      libgsasl-devel
      libunwind-devel
      libuuid-devel
      libxml2-devel
      libzip
      libzip-devel
      m4
      minizip
      minizip-devel
      make
      net-tools
      openssl-devel
      unzip
      wget
      which
      zip
      bind-utils
      perl
      fmt-devel
      libarchive
      gflags-devel
      glog-devel
      gtest-devel
      rust
    )
  else
    BASIC_PACKGES_TO_INSTALL=(
      double-conversion
      etcd
      protobuf
      openmpi
      glog
      gflags
      zstd
      snappy
      lz4
      openssl
      libevent
      fmt
      autoconf
      gnu-sed
      wget
    )
  fi
  readonly BASIC_PACKGES_TO_INSTALL
}


##########################
# Check the dependencies of deploy command.
##########################
check_dependencies() {
  log "Checking dependencies of deploy."

  # python3
  if ! command -v python3 &> /dev/null; then
    packages_to_install+=(python3)
  else
    ver=$(python3 -V 2>&1 | sed 's/.* \([0-9]\).\([0-9]\).*/\1\2/')
    if [ "${ver}" -lt "36" ]; then
      packages_to_install+=(python3)
    fi
  fi

  # cmake
  if ! command -v cmake &> /dev/null; then
    packages_to_install+=(cmake)
  else
    ver=$(cmake --version 2>&1 | awk -F ' ' '/version/ {print $3}')
    if [[ "${ver}" < "3.1" ]]; then
      packages_to_install+=(cmake)
    fi
  fi

  # java
  if [[ "${PLATFORM}" == *"Darwin"* ]]; then
    if [ ! -f "/usr/libexec/java_home" ]; then
      packages_to_install+=(jdk8)
    elif ! /usr/libexec/java_home -v 1.8 &> /dev/null; then
      packages_to_install+=(jdk8)
    fi
  else
    if ! command -v java &> /dev/null; then
      if [[ "${PLATFORM}" == *"Ubuntu"* ]]; then
        packages_to_install+=(openjdk-8-jdk)
      elif [[ "${PLATFORM}" == *"CentOS"* ]]; then
        packages_to_install+=(java-1.8.0-openjdk-devel)
      else
        packages_to_install+=(jdk8)
      fi
    else
      ver=$(java -version 2>&1 | awk -F '"' '/version/ {print $2}' | awk -F '.' '{print $2}')
      if [[ "${ver}" != "8" ]]; then
        if [[ "${PLATFORM}" == *"Ubuntu"* ]]; then
          packages_to_install+=(openjdk-8-jdk)
        elif [[ "${PLATFORM}" == *"CentOS"* ]]; then
          packages_to_install+=(java-1.8.0-openjdk-devel)
        fi
      fi
    fi
  fi

  # boost
  if [[ "${PLATFORM}" == *"Ubuntu"* ]]; then
    if [ ! -f "/usr/include/boost/version.hpp" ]; then
      packages_to_install+=(libboost-all-dev)
    else
      ver=$(grep "#define BOOST_VERSION" /usr/include/boost/version.hpp | cut -d' ' -f3)
      if [[ "${ver}" -lt "106600" ]]; then
        packages_to_install+=(libboost-all-dev)
      fi
    fi
  elif [[ "${PLATFORM}" == *"CentOS"* ]]; then
    if [ ! -f "/usr/include/boost/version.hpp" ]; then
      packages_to_install+=(boost-devel)
    else
      ver=$(grep "#define BOOST_VERSION" /usr/include/boost/version.hpp | cut -d' ' -f3)
      if [[ "${ver}" -lt "106600" ]]; then
        packages_to_install+=(boost-devel)
      fi
    fi
  else
    if [ ! -f "/usr/local/include/boost/version.hpp" ]; then
      packages_to_install+=(boost)
    else
      ver=$(grep "#define BOOST_VERSION" /usr/local/include/boost/version.hpp | cut -d' ' -f3)
      if [[ "${ver}" -lt "106600" ]]; then
        packages_to_install+=(boost)
      fi
    fi
  fi

  # apache arrow
  if [[ "${PLATFORM}" == *"Darwin"* ]]; then
    if [ ! -f "/usr/local/include/arrow/api.h" ]; then
      packages_to_install+=(apache-arrow)
    fi
  else
    if [ ! -f "/usr/include/arrow/api.h" ]; then
      packages_to_install+=(apache-arrow)
    fi
  fi

  # maven
  if ! command -v mvn &> /dev/null; then
    packages_to_install+=(maven)
  fi

  # rust
  if ! command -v rustc &> /dev/null; then
    packages_to_install+=(rust)
  fi

  # golang
  if ! command -v go &> /dev/null; then
    if [[ "${PLATFORM}" == *"CentOS"* ]]; then
      packages_to_install+=(golang)
    else
      packages_to_install+=(go)
    fi
  fi

  # clang in Darwin
  if [[ "${PLATFORM}" == *"Darwin"* ]]; then
    if ! command -v clang &> /dev/null; then
      packages_to_install+=("llvm@${LLVM_VERSION}")
    else
      ver=$(clang -v 2>&1 | head -n 1 | sed 's/.* \([0-9]*\)\..*/\1/')
      if [[ "${ver}" -lt "7" || "${ver}" -gt "10" ]]; then
        packages_to_install+=("llvm@${LLVM_VERSION}")
      fi
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
  if [ if "${SOURCE_DIR}/gs_env" ]; then
    warning "Found gs_env exists, remove the environmen config file and generate a new one."
  fi

  if [[ "${PLATFORM}" == *"Darwin"* ]]; then
    {
      if [[ "${packages_to_install[@]}" =~ "llvm@${LLVM_VERSION}" ]]; then
        # packages_to_install contains llvm
        echo "export CC=/usr/local/opt/llvm@${LLVM_VERSION}/bin/clang"
        echo "export CXX=/usr/local/opt/llvm@${LLVM_VERSION}/bin/clang++"
        echo "export LDFLAGS=-L/usr/local/opt/llvm@${LLVM_VERSION}/lib"
        echo "export CPPFLAGS=-I/usr/local/opt/llvm@${LLVM_VERSION}/include"
        echo "export PATH=/usr/local/opt/llvm@${LLVM_VERSION}/bin:\$PATH"
      fi
      echo "export JAVA_HOME=\$(/usr/libexec/java_home -v 1.8)"
      echo "export PATH=/usr/local/opt/gnu-sed/libexec/gnubin:\${JAVA_HOME}/bin:\$PATH:/usr/local/zookeeper/bin"
      echo "export OPENSSL_ROOT_DIR=/usr/local/opt/openssl"
      echo "export OPENSSL_LIBRARIES=/usr/local/opt/openssl/lib"
      echo "export OPENSSL_SSL_LIBRARY=/usr/local/opt/openssl/lib/libssl.dylib"
    } >> ${SOURCE_DIR}/gs_env
  elif [[ "${PLATFORM}" == *"Ubuntu"* ]]; then
    {
      echo "export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64"
      echo "export PATH=\${JAVA_HOME}/bin:/usr/local/go/bin:\$HOME/.cargo/bin:\$PATH:/usr/local/zookeeper/bin"
    } >> ${SOURCE_DIR}/gs_env
  else
    {
      echo "export LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:/usr/local/lib:/usr/local/lib64"
      echo "export JAVA_HOME=/usr/lib/jvm/java"
      echo "export PATH=\${JAVA_HOME}/bin:/usr/local/go/bin:\$HOME/.cargo/bin:/usr/local/bin:\$PATH:/usr/local/zookeeper/bin"
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

  # install dependencies for specific platforms.
  if [[ "${PLATFORM}" == *"Ubuntu"* ]]; then
    sudo apt-get update -y

    if [[ "${packages_to_install[@]}" =~ "go" ]]; then
      # packages_to_install contains go
      log "Installing Go."
      wget --no-verbose https://golang.org/dl/go1.15.5.linux-amd64.tar.gz -P /tmp
      sudo tar -C /usr/local -xzf /tmp/go1.15.5.linux-amd64.tar.gz
      rm -fr /tmp/go1.15.5.linux-amd64.tar.gz
      # remove go from packages_to_install
      packages_to_install=("${packages_to_install[@]/go}")
    fi
    if [[ "${packages_to_install[@]}" =~ "rust" ]]; then
      # packages_to_install contains rust
      log "Installing rust."
      curl -sf -L https://static.rust-lang.org/rustup.sh | sh -s -- -y --profile minimal --default-toolchain 1.48.0
      # remove rust from packages_to_install
      packages_to_install=("${packages_to_install[@]/rust}")
    fi

    if [[ "${packages_to_install[@]}" =~ "apache-arrow" ]]; then
      log "Installing apache-arrow."
      wget https://apache.jfrog.io/artifactory/arrow/$(lsb_release --id --short | tr 'A-Z' 'a-z')/apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb
      sudo apt install -y -V ./apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb
      sudo apt update -y
      sudo apt install -y libarrow-dev=3.0.0-1 libarrow-python-dev=3.0.0-1
      # remove apache-arrow from packages_to_install
      packages_to_install=("${packages_to_install[@]/apache-arrow}")
    fi

    log "Installing packages ${BASIC_PACKGES_TO_INSTALL[*]} ${packages_to_install[*]}"
    sudo apt install -y ${BASIC_PACKGES_TO_INSTALL[*]} ${packages_to_install[*]}

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

  elif [[ "${PLATFORM}" == *"CentOS"* ]]; then
    sudo dnf install -y https://download-ib01.fedoraproject.org/pub/epel/epel-release-latest-8.noarch.rpm

    sudo dnf -y install gcc gcc-c++
    sudo dnf config-manager --set-enabled epel || :
    sudo dnf config-manager --set-enabled powertools || :

    if [[ "${packages_to_install[@]}" =~ "apache-arrow" ]]; then
      log "Installing apache-arrow."
      sudo dnf install -y epel-release || sudo dnf install -y https://dl.fedoraproject.org/pub/epel/epel-release-latest-$(cut -d: -f5 /etc/system-release-cpe | cut -d. -f1).noarch.rpm
      sudo dnf install -y https://apache.jfrog.io/artifactory/arrow/centos/$(cut -d: -f5 /etc/system-release-cpe | cut -d. -f1)/apache-arrow-release-latest.rpm
      sudo subscription-manager repos --enable codeready-builder-for-rhel-$(cut -d: -f5 /etc/system-release-cpe | cut -d. -f1)-$(arch)-rpms || :
      sudo dnf --enablerepo=epel install -y arrow-devel
      # remove apache-arrow from packages_to_install
      packages_to_install=("${packages_to_install[@]/apache-arrow}")
    fi

    log "Installing packages ${BASIC_PACKGES_TO_INSTALL[*]} ${packages_to_install[*]}"
    sudo dnf -y install ${BASIC_PACKGES_TO_INSTALL[*]} ${packages_to_install[*]}

    log "Installing protobuf v.3.13.0"
    wget https://github.com/protocolbuffers/protobuf/releases/download/v3.13.0/protobuf-all-3.13.0.tar.gz -P /tmp
    tar zxvf /tmp/protobuf-all-3.13.0.tar.gz -C /tmp/
    pushd /tmp/protobuf-3.13.0
    ./configure --enable-shared --disable-static
    make -j${NUM_PROC}
    sudo make install && ldconfig
    popd
    rm -fr /tmp/protobuf-all-3.13.0.tar.gz /tmp/protobuf-3.13.0

    log "Installing grpc v1.33.1"
    git clone --depth 1 --branch v1.33.1 https://github.com/grpc/grpc.git /tmp/grpc
    pushd /tmp/grpc
    git submodule update --init
    mkdir build && cd build
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
        -DgRPC_SSL_PROVIDER=package
    make -j${NUM_PROC}
    sudo make install
    popd
    rm -fr /tmp/grpc

    log "Installing etcd v3.4.13"
    mkdir -p /tmp/etcd-download-test
    export ETCD_VER=v3.4.13 && \
    export DOWNLOAD_URL=https://github.com/etcd-io/etcd/releases/download && \
    curl -L ${DOWNLOAD_URL}/${ETCD_VER}/etcd-${ETCD_VER}-linux-amd64.tar.gz -o /tmp/etcd-${ETCD_VER}-linux-amd64.tar.gz
    tar xzvf /tmp/etcd-${ETCD_VER}-linux-amd64.tar.gz -C /tmp/etcd-download-test --strip-components=1
    sudo mv /tmp/etcd-download-test/etcd /usr/local/bin/
    sudo mv /tmp/etcd-download-test/etcdctl /usr/local/bin/
    rm -fr /tmp/etcd-${ETCD_VER}-linux-amd64.tar.gz /tmp/etcd-download-test

    log "Installing openmpi v4.0.5"
    wget https://download.open-mpi.org/release/open-mpi/v4.0/openmpi-4.0.5.tar.gz -P /tmp
    tar zxvf /tmp/openmpi-4.0.5.tar.gz -C /tmp
    pushd /tmp/openmpi-4.0.5 && ./configure --enable-mpi-cxx
    make -j${NUM_PROC}
    sudo make install
    popd
    rm -fr /tmp/openmpi-4.0.5 /tmp/openmpi-4.0.5.tar.gz

  elif [[ "${PLATFORM}" == *"Darwin"* ]]; then
    if [[ "${packages_to_install[@]}" =~ "jdk8" ]]; then
      # packages_to_install contains jdk8
      log "Installing adoptopenjdk8."
      brew tap adoptopenjdk/openjdk
      brew install --cask adoptopenjdk8
      # remove jdk8 from packages_to_install
      packages_to_install=("${packages_to_install[@]/jdk8}")
    fi

    log "Installing packages ${BASIC_PACKGES_TO_INSTALL[*]} ${packages_to_install[*]}"
    brew install ${BASIC_PACKGES_TO_INSTALL[*]} ${packages_to_install[*]}

    export OPENSSL_ROOT_DIR=/usr/local/opt/openssl
    export OPENSSL_LIBRARIES=/usr/local/opt/openssl/lib
    export OPENSSL_SSL_LIBRARY=/usr/local/opt/openssl/lib/libssl.dylib
    if [[ "${packages_to_install[@]}" =~ "llvm${LLVM_VERSION}" ]]; then
      export CC=/usr/local/opt/llvm@${LLVM_VERSION}/bin/clang
      export CXX=/usr/local/opt/llvm@${LLVM_VERSION}/bin/clang++
      export LDFLAGS=-L/usr/local/opt/llvm@${LLVM_VERSION}/lib
      export CPPFLAGS=-I/usr/local/opt/llvm@${LLVM_VERSION}/include
    fi
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

  log "Output environments config file ${SOURCE_DIR}/gs_env"
  write_envs_config
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
  log "Build GraphScope."
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

  init_basic_packages

  check_dependencies

  install_dependencies

  succ_msg="${GREEN}Install dependencies successfully. The script had output the related
  environments to ${SOURCE_DIR}/gs_env.\nPlease run 'source ${SOURCE_DIR}/gs_env'
  before run deploy command.${NC}"
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

  check_dependencies

  if [ "${packages_to_install}" != "" ]; then
    err_msg="The dependence of GraphScope is not satisfied. These packages
            [${packages_to_install[*]}] are not installed or version not compatible."
    err ${err_msg}
    exit 1
  fi

  install_libgrape-lite

  install_vineyard

  install_graphscope

  succ_msg="${GREEN}GraphScope has been built successfully and installed on ${INSTALL_PREFIX}. \n
  Please manually run \n
  'export GRAPHSCOPE_PREFIX=${INSTALL_PREFIX}'\n
  to before using GraphScope via Python client, enjoy!\n${NC}"
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
