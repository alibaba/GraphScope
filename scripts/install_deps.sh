#!/usr/bin/env bash
#
# A script to install dependencies of GraphScope.
# TODO: check dependencies revise
# TODO: install depedencies faster

set -e
set -o pipefail

# color
readonly RED="\033[0;31m"
readonly YELLOW="\033[1;33m"
readonly GREEN="\033[0;32m"
readonly NC="\033[0m" # No Color

readonly GRAPE_BRANCH="master" # libgrape-lite branch
readonly V6D_VERSION="0.3.11"  # vineyard version
readonly V6D_BRANCH="v0.3.11" # vineyard branch

readonly OUTPUT_ENV_FILE="${HOME}/.graphscope_env"
IS_IN_WSL=false && [[ ! -z "${IS_WSL}" || ! -z "${WSL_DISTRO_NAME}" ]] && IS_IN_WSL=true
readonly IS_IN_WSL
DEPS_PREFIX="/usr/local"
BASIC_PACKGES_TO_INSTALL=
PLATFORM=
OS_VERSION=
VERBOSE=false
CN_MIRROR=false
packages_to_install=()
install_folly=false

err() {
  echo -e "${RED}[$(date +'%Y-%m-%dT%H:%M:%S%z')]: [ERROR] $*${NC}" >&2
}

warning() {
  echo -e "${YELLOW}[$(date +'%Y-%m-%dT%H:%M:%S%z')]: [WARNING] $*${NC}" >&1
}

log() {
  echo -e "[$(date +'%Y-%m-%dT%H:%M:%S%z')]: $*" >&1
}

succ() {
  echo -e "${GREEN}[$(date +'%Y-%m-%dT%H:%M:%S%z')]: $*${NC}" >&1
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

  A script to install dependencies of GraphScope.

  Usage: install_deps options

  Options:
    -h, --help           Print help information
    --verbose            Print the debug logging information
    --k8s                Install the dependencies for running GraphScope on k8s locally
    --dev                Install the dependencies for build GraphScope on local
    --cn                 Use tsinghua mirror for brew when install dependencies on macOS
END
}

##########################
# Check dir exists and remove it.
##########################
check_and_remove_dir() {
  if [[ -d $1 ]]; then
    log "Found $1 exists, remove it."
    rm -fr $1
  fi
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
    OS_VERSION=$(sed 's/.* \([0-9]\).*/\1/' < /etc/centos-release)
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
      build-essential
      wget
      curl
      lsb-release
      libbrotli-dev
      libbz2-dev
      libclang-dev
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
      librdkafka-dev
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
      zip
      perl
      python3-pip
      git
    )
  elif [[ "${PLATFORM}" == *"CentOS"* ]]; then
    BASIC_PACKGES_TO_INSTALL=(
      autoconf
      automake
      clang-devel
      double-conversion-devel
      git
      zlib-devel
      libcurl-devel
      libevent-devel
      libgsasl-devel
      librdkafka-devel
      libunwind-devel
      libuuid-devel
      libxml2-devel
      libzip
      libzip-devel
      m4
      minizip
      minizip-devel
      net-tools
      openssl-devel
      unzip
      which
      zip
      bind-utils
      perl
      libarchive
      gflags-devel
      glog-devel
      gtest-devel
      gcc
      gcc-c++
      make
      wget
      curl
    )
  else
    BASIC_PACKGES_TO_INSTALL=(
      coreutils
      double-conversion
      protobuf
      glog
      gflags
      grpc
      zstd
      snappy
      lz4
      openssl
      libevent
      librdkafka
      autoconf
      wget
      libomp
    )
  fi
  readonly BASIC_PACKGES_TO_INSTALL
}


##########################
# Check the dependencies of deploy command.
##########################
check_dependencies() {
  log "Checking dependencies for building GraphScope."

  # check python3 >= 3.6
  if ! command -v python3 &> /dev/null ||
     [[ "$(python3 -V 2>&1 | sed 's/.* \([0-9]\).\([0-9]\).*/\1\2/')" -lt "36" ]]; then
    if [[ "${PLATFORM}" == *"CentOS"* ]]; then
      packages_to_install+=(python3-devel)
    else
      packages_to_install+=(python3)
    fi
  fi

  # check cmake >= 3.1
  if $(! command -v cmake &> /dev/null) || \
     [[ "$(cmake --version 2>&1 | awk -F ' ' '/version/ {print $3}')" < "3.1" ]]; then
    packages_to_install+=(cmake)
  fi

  # check java
  if [[ "${PLATFORM}" == *"Darwin"* ]]; then
    if [[ ! -z "${JAVA_HOME}" ]]; then
      declare -r java_version=$(${JAVA_HOME}/bin/javac -version 2>&1 | awk -F ' ' '{print $2}' | awk -F '.' '{print $1}')
      if [[ "${java_version}" -lt "8" ]] || [[ "${java_version}" -gt "15" ]]; then
        warning "Found the java version is ${java_version}, do not meet the requirement of GraphScope."
        warning "Would install jdk 11 instead and reset the JAVA_HOME"
        JAVA_HOME=""  # reset JAVA_HOME to jdk11
        packages_to_install+=(openjdk@11)
      fi
    else
      if [[ ! -f "/usr/libexec/java_home" ]] || \
         ! /usr/libexec/java_home -v11 &> /dev/null; then
        packages_to_install+=(openjdk@11)
      fi
    fi
  else
    if $(! command -v javac &> /dev/null) || \
       [[ "$(javac -version 2>&1 | awk -F ' ' '{print $2}' | awk -F '.' '{print $1}')" -lt "7" ]]; then
      if [[ "${PLATFORM}" == *"Ubuntu"* ]]; then
        packages_to_install+=(default-jdk)
      else
        packages_to_install+=(java-11-openjdk-devel)  # CentOS
      fi
    fi
  fi

  # check boost >= 1.66
  if [[ ( ! -f "/usr/include/boost/version.hpp" || \
        "$(grep "#define BOOST_VERSION" /usr/include/boost/version.hpp | cut -d' ' -f3)" -lt "106600" ) && \
     ( ! -f "/usr/local/include/boost/version.hpp" || \
       "$(grep "#define BOOST_VERSION" /usr/local/include/boost/version.hpp | cut -d' ' -f3)" -lt "106600" ) && \
     ( ! -f "/opt/homebrew/include/boost/version.hpp" || \
       "$(grep "#define BOOST_VERSION" /opt/homebrew/include/boost/version.hpp | cut -d' ' -f3)" -lt "106600" ) ]]; then
    case "${PLATFORM}" in
      *"Ubuntu"*)
        packages_to_install+=(libboost-all-dev)
        ;;
      *"CentOS"*)
        packages_to_install+=(boost-devel)
        ;;
      *)
        packages_to_install+=(boost)
        ;;
    esac
  fi

  # check apache-arrow
  if [[ ! -f "/usr/local/include/arrow/api.h" && ! -f "/usr/include/arrow/api.h" &&
        ! -f "/opt/homebrew/include/arrow/api.h" ]]; then
    packages_to_install+=(apache-arrow)
  fi

  # check maven
  if ! command -v mvn &> /dev/null; then
    packages_to_install+=(maven)
  fi

  # check rust > 1.52.0
  if ( ! command -v rustup &> /dev/null || \
    [[ "$(rustc --V | awk -F ' ' '{print $2}')" < "1.52.0" ]] ) && \
     ( ! command -v ${HOME}/.cargo/bin/rustup &> /dev/null || \
    [[ "$(${HOME}/.cargo/bin/rustc --V | awk -F ' ' '{print $2}')" < "1.52.0" ]] ); then
    packages_to_install+=(rust)
  fi

  # check go < 1.16 (reason: vertion 1.16 can't install zetcd)
  if $(! command -v go &> /dev/null) || \
     [[ "$(go version 2>&1 | awk -F '.' '{print $2}' | awk -F ' ' '{print $1}')" -ge "16" ]]; then
    if [[ "${PLATFORM}" == *"CentOS"* ]]; then
      packages_to_install+=(golang)
    else
      packages_to_install+=(go)
    fi
  fi

  # check etcd
  if ! command -v etcd &> /dev/null; then
    packages_to_install+=(etcd)
  fi

  # check mpi
  if ! command -v mpiexec &> /dev/null; then
    if [[ "${PLATFORM}" == *"Ubuntu"* ]]; then
      packages_to_install+=(libopenmpi-dev)
    else
      packages_to_install+=(openmpi)
    fi
  fi

  # check folly
  if [[ ! -f "/usr/local/include/folly/dynamic.h" && ! -f "/opt/homebrew/include/folly/dynamic.h" ]]; then
    packages_to_install+=(folly)
  fi

  # check zetcd
  if ! command -v zetcd &> /dev/null && ! command -v ${HOME}/go/bin/zetcd &> /dev/null && \
     ! command -v /usr/local/go/bin/zetcd &> /dev/null; then
    packages_to_install+=(zetcd)
  fi

  # check c++ compiler
  if [[ "${PLATFORM}" == *"Darwin"* ]]; then
    if [[ ! -z "$(brew info llvm 2>&1 | grep 'Not installed')" ]];then
        packages_to_install+=("llvm")
    fi
  else
    if ! command -v g++ &> /dev/null; then
      if [[ "${PLATFORM}" == *"Ubuntu"* ]]; then
        packages_to_install+=(build-essential)
      else
        packages_to_install+=(gcc gcc-c++)
      fi
    fi
  fi
}

##########################
# Write out the related environment export statements to file.
# Globals:
#   PLATFORM
# Arguments:
#   None
# Outputs:
#   output environment export statements to file.
##########################
write_envs_config() {
  if [ -f "${OUTPUT_ENV_FILE}" ]; then
    warning "Found ${OUTPUT_ENV_FILE} exists, remove the environmen config file and generate a new one."
    rm -fr ${OUTPUT_ENV_FILE}
  fi

  if [[ "${PLATFORM}" == *"Darwin"* ]]; then
    if [[ "$(uname -m)" == *"x86_64"* ]]; then
      declare -r homebrew_prefix=/usr/local
    else
      # Apple Silicon: packages are installed under /opt/homebrew by default
      declare -r homebrew_prefix=/opt/homebrew
    fi
    {
      echo "export CC=${homebrew_prefix}/opt/llvm/bin/clang"
      echo "export CXX=${homebrew_prefix}/opt/llvm/bin/clang++"
      echo "export CPPFLAGS=-I${homebrew_prefix}/opt/llvm/include"
      echo "export PATH=${homebrew_prefix}/opt/llvm/bin:\$PATH"
      if [ -z "${JAVA_HOME}" ]; then
        echo "export JAVA_HOME=\$(/usr/libexec/java_home -v11)"
      fi
      echo "export PATH=\$HOME/.cargo/bin:\${JAVA_HOME}/bin:/usr/local/go/bin:\$PATH"
      echo "export PATH=\$(go env GOPATH)/bin:\$PATH"
      echo "export OPENSSL_ROOT_DIR=${homebrew_prefix}/opt/openssl"
      echo "export OPENSSL_LIBRARIES=${homebrew_prefix}/opt/openssl/lib"
      echo "export OPENSSL_SSL_LIBRARY=${homebrew_prefix}/opt/openssl/lib/libssl.dylib"
    } >> ${OUTPUT_ENV_FILE}

  elif [[ "${PLATFORM}" == *"Ubuntu"* ]]; then
    {
      echo "export LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:/usr/local/lib:/usr/local/lib64"
      if [ -z "${JAVA_HOME}" ]; then
        echo "export JAVA_HOME=/usr/lib/jvm/default-java"
      fi
      echo "export PATH=\${JAVA_HOME}/bin:\$HOME/.cargo/bin:/usr/local/go/bin:\$PATH"
      echo "export PATH=\$(go env GOPATH)/bin:\$PATH"
    } >> ${OUTPUT_ENV_FILE}
  else
    {
      echo "export LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:/usr/local/lib:/usr/local/lib64"
      if [ -z "${JAVA_HOME}" ]; then
        echo "export JAVA_HOME=/usr/lib/jvm/java"
      fi
      echo "export PATH=\${JAVA_HOME}/bin:\$HOME/.cargo/bin:\$PATH:/usr/local/go/bin"
      echo "export PATH=\$(go env GOPATH)/bin:\$PATH"
    } >> ${OUTPUT_ENV_FILE}
  fi
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

  if [[ -f "/usr/local/include/grape/grape.h" ]]; then
    log "libgrape-lite already installed, skip."
    return 0
  fi

  check_and_remove_dir "/tmp/libgrape-lite"
  git clone -b ${GRAPE_BRANCH} --single-branch --depth=1 \
      https://github.com/alibaba/libgrape-lite.git /tmp/libgrape-lite
  pushd /tmp/libgrape-lite
  mkdir -p build && cd build
  cmake ..
  make -j$(nproc)
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
  if command -v /usr/local/bin/vineyardd &> /dev/null && \
     [[ "$(/usr/local/bin/vineyardd --version 2>&1 | awk -F ' ' '{print $3}')" == "${V6D_VERSION}" ]]; then
    log "vineyard ${V6D_VERSION} already installed, skip."
    return 0
  fi

  check_and_remove_dir "/tmp/libvineyard"
  git clone -b ${V6D_BRANCH} --single-branch --depth=1 \
      https://github.com/alibaba/libvineyard.git /tmp/libvineyard
  pushd /tmp/libvineyard
  git submodule update --init
  mkdir -p build && pushd build
  cmake .. -DCMAKE_INSTALL_PREFIX=${DEPS_PREFIX} \
           -DBUILD_SHARED_LIBS=ON \
           -DBUILD_VINEYARD_TESTS=OFF
  make -j$(nproc)
  sudo make install && popd
  popd

  rm -fr /tmp/libvineyard
}

##########################
# Install cppkafka.
# Globals:
#   PLATFORM
#   NUM_PROC
# Arguments:
#   None
# Outputs:
#   output log to stdout, output error to stderr.
##########################
install_cppkafka() {
  log "Building and installing cppkafka."

  if [[ -f "/usr/local/include/cppkafka/cppkafka.h" ]]; then
    log "cppkafka already installed, skip."
    return 0
  fi

  if [[ "${PLATFORM}" == *"Darwin"* ]]; then
    export LDFLAGS="-L/usr/local/opt/openssl@3/lib"
    export CPPFLAGS="-I/usr/local/opt/openssl@3/include"
  fi

  check_and_remove_dir "/tmp/cppkafka"
  git clone -b 0.4.0 --single-branch --depth=1 \
      https://github.com/mfontanini/cppkafka.git /tmp/cppkafka
  pushd /tmp/cppkafka
  git submodule update --init
  mkdir -p build && pushd build
  cmake .. && make -j$(nproc)
  sudo make install && popd
  popd

  rm -fr /tmp/cppkafka
}

##########################
# Install denpendencies of GraphScope.
# Globals:
#   PLATFORM
# Arguments:
#   None
# Outputs:
#   output log to stdout, output error to stderr.
##########################
install_dependencies() {

  # install dependencies for specific platforms.
  if [[ "${PLATFORM}" == *"Ubuntu"* ]]; then
    sudo apt-get update -y

    log "Installing packages ${BASIC_PACKGES_TO_INSTALL[*]}"
    sudo apt-get install -y ${BASIC_PACKGES_TO_INSTALL[*]}

    if [[ "${packages_to_install[*]}" =~ "go" ]]; then
      # packages_to_install contains go
      log "Installing Go."
      wget -c https://golang.org/dl/go1.15.5.linux-amd64.tar.gz -P /tmp
      sudo tar -C /usr/local -xzf /tmp/go1.15.5.linux-amd64.tar.gz
      rm -fr /tmp/go1.15.5.linux-amd64.tar.gz
      sudo ln -sf /usr/local/go/bin/go /usr/local/bin/go
      # remove go from packages_to_install
      packages_to_install=("${packages_to_install[@]/go}")
    fi
    if [[ "${packages_to_install[*]}" =~ "rust" ]]; then
      # packages_to_install contains rust
      log "Installing rust."
      curl -sf -L https://static.rust-lang.org/rustup.sh | sh -s -- -y --profile minimal --default-toolchain 1.54.0
      # remove rust from packages_to_install
      packages_to_install=("${packages_to_install[@]/rust}")
    fi

    if [[ "${packages_to_install[*]}" =~ "apache-arrow" ]]; then
      log "Installing apache-arrow."
      wget -c https://apache.jfrog.io/artifactory/arrow/$(lsb_release --id --short | tr 'A-Z' 'a-z')/apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb \
        -P /tmp/
      sudo apt install -y -V /tmp/apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb
      sudo apt update -y
      sudo apt install -y libarrow-dev=3.0.0-1 libarrow-python-dev=3.0.0-1
      # remove apache-arrow from packages_to_install
      packages_to_install=("${packages_to_install[@]/apache-arrow}")
    fi

    if [[ "${packages_to_install[*]}" =~ "folly" ]]; then
      install_folly=true  # set folly install flag
      # remove folly from packages_to_install
      packages_to_install=("${packages_to_install[@]/folly}")
    fi

    if [[ "${packages_to_install[*]}" =~ "zetcd" ]]; then
      log "Installing zetcd."
      export PATH=${PATH}:/usr/local/go/bin
      go get github.com/etcd-io/zetcd/cmd/zetcd
      sudo cp ${HOME}/go/bin/zetcd /usr/local/bin/zetcd
      # remove zetcd from packages_to_install
      packages_to_install=("${packages_to_install[@]/zetcd}")
    fi

    if [[ ! -z "${packages_to_install}" ]]; then
      log "Installing packages ${packages_to_install[*]}"
      sudo apt install -y ${packages_to_install[*]}
    fi

  elif [[ "${PLATFORM}" == *"CentOS"* ]]; then
    sudo dnf install -y dnf-plugins-core \
        https://download-ib01.fedoraproject.org/pub/epel/epel-release-latest-8.noarch.rpm

    sudo dnf config-manager --set-enabled epel
    sudo dnf config-manager --set-enabled powertools

    log "Instralling packages ${BASIC_PACKGES_TO_INSTALL[*]}"
    sudo dnf install -y ${BASIC_PACKGES_TO_INSTALL[*]}

    if [[ "${packages_to_install[*]}" =~ "apache-arrow" ]]; then
      log "Installing apache-arrow."
      sudo dnf install -y epel-release || sudo dnf install -y \
        https://dl.fedoraproject.org/pub/epel/epel-release-latest-$(cut -d: -f5 /etc/system-release-cpe | cut -d. -f1).noarch.rpm
      sudo dnf install -y https://apache.jfrog.io/artifactory/arrow/centos/$(cut -d: -f5 /etc/system-release-cpe | cut -d. -f1)/apache-arrow-release-latest.rpm
      sudo dnf install -y arrow-devel
      # remove apache-arrow from packages_to_install
      packages_to_install=("${packages_to_install[@]/apache-arrow}")
    fi

    if [[ "${packages_to_install[*]}" =~ "openmpi" ]]; then
      log "Installing openmpi v4.0.5"
      wget -c https://download.open-mpi.org/release/open-mpi/v4.0/openmpi-4.0.5.tar.gz -P /tmp
      check_and_remove_dir "/tmp/openmpi-4.0.5"
      tar zxvf /tmp/openmpi-4.0.5.tar.gz -C /tmp
      pushd /tmp/openmpi-4.0.5 && ./configure --enable-mpi-cxx
      make -j$(nproc)
      sudo make install
      popd
      rm -fr /tmp/openmpi-4.0.5 /tmp/openmpi-4.0.5.tar.gz
      packages_to_install=("${packages_to_install[@]/openmpi}")
    fi

    if [[ "${packages_to_install[*]}" =~ "zetcd" ]]; then
      log "Installing zetcd."
      export PATH=${PATH}:/usr/local/go/bin
      go get github.com/etcd-io/zetcd/cmd/zetcd
      sudo cp ${HOME}/go/bin/zetcd /usr/local/bin/zetcd
      # remove zetcd from packages_to_install
      packages_to_install=("${packages_to_install[@]/zetcd}")
    fi

    if [[ "${packages_to_install[*]}" =~ "etcd" ]]; then
      log "Installing etcd v3.4.13"
      check_and_remove_dir "/tmp/etcd-download-test"
      mkdir -p /tmp/etcd-download-test
      export ETCD_VER=v3.4.13 && \
      export DOWNLOAD_URL=https://github.com/etcd-io/etcd/releases/download && \
      curl -L ${DOWNLOAD_URL}/${ETCD_VER}/etcd-${ETCD_VER}-linux-amd64.tar.gz \
        -o /tmp/etcd-${ETCD_VER}-linux-amd64.tar.gz
      tar xzvf /tmp/etcd-${ETCD_VER}-linux-amd64.tar.gz \
        -C /tmp/etcd-download-test --strip-components=1
      sudo mv /tmp/etcd-download-test/etcd /usr/local/bin/
      sudo mv /tmp/etcd-download-test/etcdctl /usr/local/bin/
      rm -fr /tmp/etcd-${ETCD_VER}-linux-amd64.tar.gz /tmp/etcd-download-test
      packages_to_install=("${packages_to_install[@]/etcd}")
    fi

    if [[ "${packages_to_install[*]}" =~ "folly" ]]; then
      install_folly=true  # set folly install flag
      # remove folly from packages_to_install
      packages_to_install=("${packages_to_install[@]/folly}")
      # add fmt to packages_to_install
      packages_to_install+=(fmt-devel)
    fi

    if [[ "${packages_to_install[*]}" =~ "rust" ]]; then
      # packages_to_install contains rust
      log "Installing rust."
      curl -sf -L https://static.rust-lang.org/rustup.sh | sh -s -- -y --profile minimal --default-toolchain 1.54.0
      # remove rust from packages_to_install
      packages_to_install=("${packages_to_install[@]/rust}")
    fi

    if [[ ! -z "${packages_to_install}" ]]; then
      log "Installing packages ${packages_to_install[*]}"
      sudo dnf -y install  ${packages_to_install[*]}
    fi

    log "Installing protobuf v.3.13.0"
    wget -c https://github.com/protocolbuffers/protobuf/releases/download/v3.13.0/protobuf-all-3.13.0.tar.gz -P /tmp
    check_and_remove_dir "/tmp/protobuf-3.13.0"
    tar zxvf /tmp/protobuf-all-3.13.0.tar.gz -C /tmp/
    pushd /tmp/protobuf-3.13.0
    ./configure --enable-shared --disable-static
    make -j$(nproc)
    sudo make install && ldconfig
    popd
    rm -fr /tmp/protobuf-all-3.13.0.tar.gz /tmp/protobuf-3.13.0

    log "Installing grpc v1.33.1"
    if [[ -d "/tmp/grpc" ]]; then
      sudo rm -fr /tmp/grpc
    fi
    git clone --depth 1 --branch v1.33.1 https://github.com/grpc/grpc.git /tmp/grpc
    pushd /tmp/grpc
    git submodule update --init
    mkdir -p build && cd build
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
    make -j$(nproc)
    sudo make install
    popd
    rm -fr /tmp/grpc

  elif [[ "${PLATFORM}" == *"Darwin"* ]]; then
    if [[ ${CN_MIRROR} == true ]]; then
      # set brew to tsinghua mirror
      export HOMEBREW_BREW_GIT_REMOTE="https://mirrors.tuna.tsinghua.edu.cn/git/homebrew/brew.git"
      export HOMEBREW_CORE_GIT_REMOTE="https://mirrors.tuna.tsinghua.edu.cn/git/homebrew/homebrew-core.git"
      export HOMEBREW_BOTTLE_DOMAIN="https://mirrors.tuna.tsinghua.edu.cn/homebrew-bottles"
    fi
    log "Installing packages ${BASIC_PACKGES_TO_INSTALL[*]}"
    brew install ${BASIC_PACKGES_TO_INSTALL[*]}

    if [[ ${CN_MIRROR} == true && "${packages_to_install[*]}" =~ "openjdk@11" ]]; then
      # packages_to_install contains jdk
      log "Installing openjdk11."
      wget -c https://graphscope.oss-cn-beijing.aliyuncs.com/dependencies/OpenJDK11U-jdk_x64_mac_hotspot_11.0.13_8.tar.gz \
        -P /tmp
      sudo tar xf /tmp/OpenJDK11U-jdk_x64_mac_hotspot_11.0.13_8.tar.gz -C /Library/Java/JavaVirtualMachines/
      rm -fr /tmp/OpenJDK11U-jdk_x64_mac_hotspot_11.0.13_8.tar.gz
      # remove jdk from packages_to_install
      packages_to_install=("${packages_to_install[@]/openjdk@11}")
    fi

    if [[ "${packages_to_install[*]}" =~ "go" ]]; then
      # packages_to_install contains go
      log "Installing Go."
      wget -c https://dl.google.com/go/go1.15.15.darwin-amd64.pkg -P /tmp
      sudo installer -pkg /tmp/go1.15.15.darwin-amd64.pkg -target /
      rm -fr /tmp/go1.15.15.darwin-amd64.pkg
      sudo ln -sf /usr/local/go/bin/go /usr/local/bin/go
      # remove go from packages_to_install
      packages_to_install=("${packages_to_install[@]/go}")
    fi

    if [[ "${packages_to_install[*]}" =~ "zetcd" ]]; then
      log "Installing zetcd."
      export PATH=/usr/local/go/bin:${PATH}
      go get github.com/etcd-io/zetcd/cmd/zetcd
      sudo cp ${HOME}/go/bin/zetcd /usr/local/bin/zetcd
      # remove zetcd from packages_to_install
      packages_to_install=("${packages_to_install[@]/zetcd}")
    fi

    if [[ "${packages_to_install[*]}" =~ "rust" ]]; then
      # packages_to_install contains rust
      log "Installing rust."
      curl -sf -L https://static.rust-lang.org/rustup.sh | sh -s -- -y --profile minimal --default-toolchain 1.54.0
      # remove rust from packages_to_install
      packages_to_install=("${packages_to_install[@]/rust}")
    fi

    if [[ "${packages_to_install[*]}" =~ "maven" ]]; then
      # install maven ignore openjdk dependencies
      brew install --ignore-dependencies maven
      packages_to_install=("${packages_to_install[@]/maven}")
    fi


    if [[ ! -z "${packages_to_install}" ]]; then
      log "Installing packages ${packages_to_install[*]}"
      brew install ${packages_to_install[*]}
    fi



    if [[ "$(uname -m)" == "x86_64" ]]; then
      declare -r homebrew_prefix="/usr/local"
    else
      declare -r homebrew_prefix="/opt/homebrew"
    fi
    export OPENSSL_ROOT_DIR=${homebrew_prefix}/opt/openssl
    export OPENSSL_LIBRARIES=${homebrew_prefix}/opt/openssl/lib
    export OPENSSL_SSL_LIBRARY=${homebrew_prefix}/opt/openssl/lib/libssl.dylib
    export CC=${homebrew_prefix}/opt/llvm/bin/clang
    export CXX=${homebrew_prefix}/opt/llvm/bin/clang++
    export CPPFLAGS=-I${homebrew_prefix}/opt/llvm/include
  fi

  if [[ ${install_folly} == true ]]; then
    if [[ "${PLATFORM}" == *"Ubuntu"* ]]; then
      log "Installing fmt."
      wget -c https://github.com/fmtlib/fmt/archive/7.0.3.tar.gz -P /tmp
      check_and_remove_dir "/tmp/fmt-7.0.3"
      tar xf /tmp/7.0.3.tar.gz -C /tmp/
      pushd /tmp/fmt-7.0.3
      mkdir -p build && cd build
      cmake .. -DBUILD_SHARED_LIBS=ON
      make -j$(nproc)
      sudo make install
      popd
      rm -fr /tmp/7.0.3.tar.gz /tmp/fmt-7.0.3
    fi
    log "Installing folly."
    wget -c https://github.com/facebook/folly/archive/v2020.10.19.00.tar.gz -P /tmp
    check_and_remove_dir "/tmp/folly-2020.10.19.00"
    tar xf /tmp/v2020.10.19.00.tar.gz -C /tmp/
    pushd /tmp/folly-2020.10.19.00
    mkdir -p _build && cd _build
    cmake -DBUILD_SHARED_LIBS=ON -DCMAKE_POSITION_INDEPENDENT_CODE=ON ..
    make -j$(nproc)
    sudo make install
    popd
    rm -fr /tmp/v2020.10.19.00.tar.gz /tmp/folly-2020.10.19.00
  fi

  log "Installing python packages for vineyard codegen."
  pip3 install -U pip --user
  pip3 install grpcio-tools libclang parsec setuptools wheel twine --user

  install_libgrape-lite

  install_vineyard

  install_cppkafka

  log "Output environments config file ${OUTPUT_ENV_FILE}"
  write_envs_config
}


# Functions to install dependencies of k8s evironment.
check_os_compatibility_k8s() {
  if [[ "${IS_IN_WSL}" == true && -z "${WSL_INTEROP}" ]]; then
    err "The platform is WSL1. GraphScope not support to run on WSL1, please use WSL2."
    exit 1
  fi

  if [[ "${PLATFORM}" != *"Ubuntu"* && "${PLATFORM}" != *"CentOS"* ]]; then
    err "The platform is not Ubuntu or CentOs. This script is only available on Ubuntu/CentOS"
    exit 1
  fi

  if [[ "${PLATFORM}" == *"Ubuntu"* && "$(echo ${OS_VERSION} | sed 's/\([0-9]\)\([0-9]\).*/\1\2/')" -lt "18" ]]; then
    err "The version of Ubuntu is ${OS_VERSION}. this script requires Ubuntu 18 or greater."
    exit 1
  fi

  if [[ "${PLATFORM}" == *"CentOS"* && "${OS_VERSION}" -lt "7" ]]; then
    err "The version of CentOS is ${OS_VERSION}. this script requires CentOS 7 or greater."
    exit 1
  fi

  log "Preparing environment on ${PLATFORM} ${OS_VERSION}"
}

check_dependencies_version_k8s() {
  # python
  if ! hash python3; then
    err "Python3 is not installed"
    exit 1
  fi
  ver=$(python3 -V 2>&1 | sed 's/.* \([0-9]\).\([0-9]\).*/\1\2/')
  if [ "$ver" -lt "36" ]; then
    err "GraphScope requires python 3.6 or greater. Current version is ${python3 -V}"
    exit 1
  fi
}

install_dependencies_k8s() {
  log "Install dependencies."
  if [[ "${PLATFORM}" == *"Ubuntu"* ]]; then
    sudo apt-get update -y
    sudo apt-get install -y git
    sudo apt-get install -y docker.io
    sudo apt-get install -y conntrack curl lsof
    sudo apt-get install -y python3-pip
    sudo apt-get clean
  elif [[ "${PLATFORM}" == *"CentOS"* ]]; then
    sudo yum install -y git
    sudo yum install -y python3-pip
    sudo yum install -y yum-utils curl conntrack-tools lsof
    sudo yum-config-manager --add-repo https://download.docker.com/linux/centos/docker-ce.repo
    sudo yum install -y docker-ce docker-ce-cli containerd.io
    sudo yum clean all
  fi

  check_dependencies_version_k8s

  pip3 install -U pip --user
  pip3 install graphscope-client wheel --user

  log "Install kubectl."
  K8S_VERSION=$(curl -s https://storage.googleapis.com/kubernetes-release/release/stable.txt)

  curl -Lo kubectl https://storage.googleapis.com/kubernetes-release/release/"${K8S_VERSION}"/bin/linux/amd64/kubectl && \
  chmod +x kubectl && sudo mv kubectl /usr/local/bin/ && sudo ln /usr/local/bin/kubectl /usr/bin/kubectl || true

  log "Install kind."
  curl -Lo ./kind https://github.com/kubernetes-sigs/kind/releases/download/v0.10.0/kind-linux-amd64
  chmod +x kind && sudo mv kind /usr/local/bin/ && sudo ln /usr/local/bin/kind /usr/bin/kind || true
}

##########################
# Start docker daemon.
# Globals:
#   None
# Arguments:
#   None
# Returns:
#   0 if start successfully, non-zero on error.
##########################
start_docker() {
  log "Starting doker daemon."
  # start docker daemon if docker not running.
  if ! sudo docker info >/dev/null 2>&1; then
    if [[ "${IS_IN_WSL}" = false ]]; then
      sudo systemctl start docker
    else
      sudo dockerd > /dev/null&
    fi
  fi
  log "Docker started successfully."
}

##########################
# Launch kubenetes cluster with kind.
# Globals:
#   None
# Arguments:
#   None
# Returns:
#   0 if launched successfully, non-zero on error.
##########################
launch_k8s_cluster() {
  log "Launching k8s cluster"
  curl -Lo config-with-mounts.yaml https://kind.sigs.k8s.io/examples/config-with-mounts.yaml
  # mount $HOME dir to cluster container, which is kind-control-plane
  sed -i 's@/path/to/my/files/@'"${HOME}"'@g; s@/files@'"${HOME}"'@g' ./config-with-mounts.yaml  || true
  sudo kind create cluster --config config-with-mounts.yaml
  sudo chown -R "$(id -u)":"$(id -g)" "${HOME}"/.kube || true
  log "Cluster is launched successfully."
}

##########################
# Pull and load the GraphScope images to kind cluster.
# Globals:
#   None
# Arguments:
#   None
# Returns:
#   0 if successful, non-zero on error.
##########################
pull_images() {
  log "Pulling GraphScope images."
  image_tag=
  # image_tag need to consistent with graphscope client version
  if [ -z "${image_tag}" ]; then
    image_tag=$(python3 -c "import graphscope; print(graphscope.__version__)")
  fi
  readonly image_tag
  sudo docker pull registry.cn-hongkong.aliyuncs.com/graphscope/graphscope:${image_tag} || true
  sudo docker pull quay.io/coreos/etcd:v3.4.13 || true
  log "GraphScope images pulled successfully."

  log "Loading images into kind cluster."
  sudo kind load docker-image registry.cn-hongkong.aliyuncs.com/graphscope/graphscope:${image_tag} || true
  sudo kind load docker-image quay.io/coreos/etcd:v3.4.13 || true
  log "GraphScope images loaded into kind cluster successfully."
}

##########################
# Main function for installing dependencies of development.
# Globals:
#   VERBOSE
# Arguments:
#   None
# Outputs:
#   output log to stdout, output error to stderr.
##########################
install_deps_dev() {
  # parse args for install_deps_dev
  while test $# -ne 0; do
    arg=$1; shift
    case ${arg} in
      --verbose)         VERBOSE=true; readonly VERBOSE; ;;
      --cn)              CN_MIRROR=true; readonly CN_MIRROR; ;;
      --vineyard_prefix) DEPS_PREFIX=$1; readonly DEPS_PREFIX; shift ;;
      *)
        echo "unrecognized option '${arg}'"
        usage; exit;;
    esac
  done

  if [[ ${VERBOSE} == true ]]; then
    set -x
  fi

  get_os_version

  check_os_compatibility

  init_basic_packages

  check_dependencies

  install_dependencies

  succ_msg="The script has installed all dependencies for builing GraphScope, use commands:\n
  $ source ${OUTPUT_ENV_FILE}
  $ make graphscope\n
  to build and develop GraphScope."
  succ "${succ_msg}"
}

##########################
# Main function for installing dependencies of k8s environment.
# Globals:
#   VERBOSE
# Arguments:
#   None
# Outputs:
#   output log to stdout, output error to stderr.
##########################
install_deps_k8s() {
  OVERWRITE=false
  while test $# -ne 0; do
    arg=$1; shift
    case ${arg} in
      --verbose) VERBOSE=true; readonly VERBOSE; ;;
      --overwrite) OVERWRITE=true; readonly OVERWRITE; ;;
      *)
        echo "unrecognized option '${arg}'"
        usage; exit;;
    esac
  done

  if [[ ${VERBOSE} == true ]]; then
    set -x
  fi

  if [[ ${OVERWRITE} = false && -f "${HOME}/.kube/config" ]]; then
    warning_msg="We found existing kubernetes config, seems that you already
    have a ready kubernetes cluster. If you do want to reset the kubernetes
    environment, please retry with '--overwrite' option."
    warning "${warning_msg}"
    exit 0
  fi

  get_os_version

  check_os_compatibility_k8s

  install_dependencies_k8s

  start_docker

  launch_k8s_cluster

  pull_images

  succ_msg="The script has prepared a local k8s cluster with kind to run GraphScope in distributed mode,
  Now you are ready to have fun with GraphScope."
  succ ${succ_msg}
}

if test $# -eq 0; then
  usage; exit 1;
fi

# parse argv
while test $# -ne 0; do
  arg=$1; shift
  case ${arg} in
    -h|--help)        usage; exit ;;
    --verbose)        VERBOSE=true; readonly VERBOSE; ;;
    --cn)             CN_MIRROR=true; readonly CN_MIRROR; ;;
    --dev) install_deps_dev "$@"; exit;;
    --k8s) install_deps_k8s "$@"; exit;;
    *)
      echo "unrecognized option '${arg}'"
      usage; exit 1;;
  esac
done
