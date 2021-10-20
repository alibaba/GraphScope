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
readonly V6D_VERSION="0.3.1"  # vineyard version
readonly V6D_BRANCH="v0.3.1" # vineyard branch
readonly LLVM_VERSION=9  # llvm version we use in Darwin platform

readonly SOURCE_DIR="$( cd "$(dirname $0)/.." >/dev/null 2>&1 ; pwd -P )"
readonly NUM_PROC=$( $(command -v nproc &> /dev/null) && echo $(nproc) || echo $(sysctl -n hw.physicalcpu) )
readonly OUTPUT_ENV_FILE="${HOME}/.graphscope_env"
IS_IN_WSL=false && [[ ! -z "${IS_WSL}" || ! -z "${WSL_DISTRO_NAME}" ]] && IS_IN_WSL=true
readonly IS_IN_WSL
INSTALL_PREFIX=/opt/graphscope
BASIC_PACKGES_TO_INSTALL=
PLATFORM=
OS_VERSION=
VERBOSE=false
BUILD_TYPE=release
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

  A script to install dependencies of GraphScope or deploy GraphScope locally.

  Usage: deploy_local [options] [command]

  Options:
    -h, --help           Print help information

  Commands:
    install_deps         Install the dependencies of GraphScope
    build_and_deploy     Build and deploy GraphScope locally

  Run 'deploy_local COMMAND --help' for more information on a command.
END
}

install_deps_usage() {
cat <<END

  Install dependencies of GraphScope.

  Usage: deploy_local install_deps [option]

  Options:
    --help              Print usage information
    --verbose           Print the debug logging information
END
}

build_and_deploy_usage() {
cat <<END

  Build and deploy GraphScope locally

  Usage: deploy_local build_and_deploy [option]

  Options:
    --help              Print usage information
    --verbose           Print the debug logging information
    --build_type        release or debug
    --prefix <path>     Install prefix of GraphScope, default is /opt/graphscope
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
      double-conversion
      protobuf
      glog
      gflags
      zstd
      snappy
      lz4
      openssl
      libevent
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
        packages_to_install+=(jdk)
      fi
    else
      if [[ ! -f "/usr/libexec/java_home" ]] || \
         ! /usr/libexec/java_home -v11 &> /dev/null; then
        packages_to_install+=(jdk)
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
       "$(grep "#define BOOST_VERSION" /usr/local/include/boost/version.hpp | cut -d' ' -f3)" -lt "106600" ) ]]; then
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
  if [[ ! -f "/usr/local/include/arrow/api.h" && ! -f "/usr/include/arrow/api.h" ]]; then
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
    [[ "$(rustc --V | awk -F ' ' '{print $2}')" < "1.52.0" ]] ); then
    packages_to_install+=(rust)
  fi

  # check go < 1.16 (vertion 1.16 can't install zetcd)
  # FIXME(weibin): version check is not universed.
  if $(! command -v go &> /dev/null) || \
     [[ "$(go version 2>&1 | awk -F '.' '{print $2}')" -ge "16" ]]; then
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
  # FIXME: if the brew already install folly, what should we do.
  # TODO(@weibin): remove the ci check after GraphScope support clang 12.
  if [[ -z "${CI}" ]] || ( [[ "${CI}" == "true" ]] && [[ "${PLATFORM}" != *"Darwin"* ]] ); then
    if [[ ! -f "/usr/local/include/folly/dynamic.h" ]]; then
      packages_to_install+=(folly)
    fi
  fi

  # check zetcd
  if ! command -v zetcd &> /dev/null && ! command -v ${HOME}/go/bin/zetcd &> /dev/null && \
     ! command -v /usr/local/go/bin/zetcd &> /dev/null; then
    packages_to_install+=(zetcd)
  fi

  # check c++ compiler
  if [[ "${PLATFORM}" == *"Darwin"* ]]; then
    if ! command -v clang &> /dev/null || \
       [[ "$(clang -v 2>&1 | head -n 1 | sed 's/.* \([0-9]*\)\..*/\1/')" -lt "8" ]] || \
       [[ "$(clang -v 2>&1 | head -n 1 | sed 's/.* \([0-9]*\)\..*/\1/')" -gt "10" ]]; then
      packages_to_install+=("llvm@${LLVM_VERSION}")
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
#   SOURCE_DIR
#   LLVM_VERSION
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
    {
      # FIXME: graphscope_env not correct when the script run mutiple times.
      if [[ "${packages_to_install[@]}" =~ "llvm@${LLVM_VERSION}" ]]; then
        # packages_to_install contains llvm
        echo "export CC=/usr/local/opt/llvm@${LLVM_VERSION}/bin/clang"
        echo "export CXX=/usr/local/opt/llvm@${LLVM_VERSION}/bin/clang++"
        echo "export LDFLAGS=-L/usr/local/opt/llvm@${LLVM_VERSION}/lib"
        echo "export CPPFLAGS=-I/usr/local/opt/llvm@${LLVM_VERSION}/include"
        echo "export PATH=/usr/local/opt/llvm@${LLVM_VERSION}/bin:\$PATH"
      fi
      if [ -z "${JAVA_HOME}" ]; then
        echo "export JAVA_HOME=\$(/usr/libexec/java_home -v11)"
      fi
      echo "export PATH=\$HOME/.cargo/bin:\${JAVA_HOME}/bin:/usr/local/go/bin:\$PATH"
      echo "export PATH=\$(go env GOPATH)/bin:\$PATH"
      echo "export OPENSSL_ROOT_DIR=/usr/local/opt/openssl"
      echo "export OPENSSL_LIBRARIES=/usr/local/opt/openssl/lib"
      echo "export OPENSSL_SSL_LIBRARY=/usr/local/opt/openssl/lib/libssl.dylib"
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

    log "Installing packages ${BASIC_PACKGES_TO_INSTALL[*]}"
    sudo apt-get install -y ${BASIC_PACKGES_TO_INSTALL[*]}

    if [[ "${packages_to_install[@]}" =~ "go" ]]; then
      # packages_to_install contains go
      log "Installing Go."
      wget -c https://golang.org/dl/go1.15.5.linux-amd64.tar.gz -P /tmp
      sudo tar -C /usr/local -xzf /tmp/go1.15.5.linux-amd64.tar.gz
      rm -fr /tmp/go1.15.5.linux-amd64.tar.gz
      sudo ln -sf /usr/local/go/bin/go /usr/local/bin/go
      # remove go from packages_to_install
      packages_to_install=("${packages_to_install[@]/go}")
    fi
    if [[ "${packages_to_install[@]}" =~ "rust" ]]; then
      # packages_to_install contains rust
      log "Installing rust."
      curl -sf -L https://static.rust-lang.org/rustup.sh | sh -s -- -y --profile minimal --default-toolchain 1.54.0
      # remove rust from packages_to_install
      packages_to_install=("${packages_to_install[@]/rust}")
    fi

    if [[ "${packages_to_install[@]}" =~ "apache-arrow" ]]; then
      log "Installing apache-arrow."
      wget -c https://apache.jfrog.io/artifactory/arrow/$(lsb_release --id --short | tr 'A-Z' 'a-z')/apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb \
        -P /tmp/
      sudo apt install -y -V /tmp/apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb
      sudo apt update -y
      sudo apt install -y libarrow-dev=3.0.0-1 libarrow-python-dev=3.0.0-1
      # remove apache-arrow from packages_to_install
      packages_to_install=("${packages_to_install[@]/apache-arrow}")
    fi

    if [[ "${packages_to_install[@]}" =~ "folly" ]]; then
      install_folly=true  # set folly install flag
      # remove folly from packages_to_install
      packages_to_install=("${packages_to_install[@]/folly}")
    fi

    if [[ "${packages_to_install[@]}" =~ "zetcd" ]]; then
      log "Installing zetcd."
      export PATH=${PATH}:/usr/local/go/bin
      go get github.com/etcd-io/zetcd/cmd/zetcd
      # remove zetcd from packages_to_install
      packages_to_install=("${packages_to_install[@]/zetcd}")
    fi

    log "Installing packages ${packages_to_install[*]}"
    sudo apt install -y ${packages_to_install[*]}

  elif [[ "${PLATFORM}" == *"CentOS"* ]]; then
    sudo dnf install -y dnf-plugins-core \
        https://download-ib01.fedoraproject.org/pub/epel/epel-release-latest-8.noarch.rpm

    sudo dnf config-manager --set-enabled epel
    sudo dnf config-manager --set-enabled powertools

    log "Instralling packages ${BASIC_PACKGES_TO_INSTALL[*]}"
    sudo dnf install -y ${BASIC_PACKGES_TO_INSTALL[*]}

    if [[ "${packages_to_install[@]}" =~ "apache-arrow" ]]; then
      log "Installing apache-arrow."
      sudo dnf install -y epel-release || sudo dnf install -y \
        https://dl.fedoraproject.org/pub/epel/epel-release-latest-$(cut -d: -f5 /etc/system-release-cpe | cut -d. -f1).noarch.rpm
      sudo dnf install -y https://apache.jfrog.io/artifactory/arrow/centos/$(cut -d: -f5 /etc/system-release-cpe | cut -d. -f1)/apache-arrow-release-latest.rpm
      sudo dnf install -y arrow-devel
      # remove apache-arrow from packages_to_install
      packages_to_install=("${packages_to_install[@]/apache-arrow}")
    fi

    if [[ "${packages_to_install[@]}" =~ "openmpi" ]]; then
      log "Installing openmpi v4.0.5"
      wget -c https://download.open-mpi.org/release/open-mpi/v4.0/openmpi-4.0.5.tar.gz -P /tmp
      check_and_remove_dir "/tmp/openmpi-4.0.5"
      tar zxvf /tmp/openmpi-4.0.5.tar.gz -C /tmp
      pushd /tmp/openmpi-4.0.5 && ./configure --enable-mpi-cxx
      make -j${NUM_PROC}
      sudo make install
      popd
      rm -fr /tmp/openmpi-4.0.5 /tmp/openmpi-4.0.5.tar.gz
      packages_to_install=("${packages_to_install[@]/openmpi}")
    fi

    if [[ "${packages_to_install[@]}" =~ "zetcd" ]]; then
      log "Installing zetcd."
      go get github.com/etcd-io/zetcd/cmd/zetcd
      # remove zetcd from packages_to_install
      packages_to_install=("${packages_to_install[@]/zetcd}")
    fi

    if [[ "${packages_to_install[@]}" =~ "etcd" ]]; then
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

    if [[ "${packages_to_install[@]}" =~ "folly" ]]; then
      install_folly=true  # set folly install flag
      # remove folly from packages_to_install
      packages_to_install=("${packages_to_install[@]/folly}")
      # add fmt to packages_to_install
      packages_to_install+=(fmt-devel)
    fi

    if [[ "${packages_to_install[@]}" =~ "rust" ]]; then
      # packages_to_install contains rust
      log "Installing rust."
      curl -sf -L https://static.rust-lang.org/rustup.sh | sh -s -- -y --profile minimal --default-toolchain 1.54.0
      # remove rust from packages_to_install
      packages_to_install=("${packages_to_install[@]/rust}")
    fi

    log "Installing packages ${packages_to_install[*]}"
    sudo dnf -y install  ${packages_to_install[*]}

    log "Installing protobuf v.3.13.0"
    wget -c https://github.com/protocolbuffers/protobuf/releases/download/v3.13.0/protobuf-all-3.13.0.tar.gz -P /tmp
    check_and_remove_dir "/tmp/protobuf-3.13.0"
    tar zxvf /tmp/protobuf-all-3.13.0.tar.gz -C /tmp/
    pushd /tmp/protobuf-3.13.0
    ./configure --enable-shared --disable-static
    make -j${NUM_PROC}
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
    make -j${NUM_PROC}
    sudo make install
    popd
    rm -fr /tmp/grpc

  elif [[ "${PLATFORM}" == *"Darwin"* ]]; then
    log "Installing packages ${BASIC_PACKGES_TO_INSTALL[*]}"
    brew install ${BASIC_PACKGES_TO_INSTALL[*]}

    if [[ "${packages_to_install[@]}" =~ "jdk" ]]; then
      # packages_to_install contains jdk8
      log "Installing adoptopenjdk11."
      wget -c https://github.com/adoptium/temurin11-binaries/releases/download/jdk-11.0.12%2B7/OpenJDK11U-jdk_x64_mac_hotspot_11.0.12_7.pkg \
        -P /tmp
      sudo installer -pkg /tmp/OpenJDK11U-jdk_x64_mac_hotspot_11.0.12_7.pkg -target /
      rm -fr /tmp/OpenJDK11U-jdk_x64_mac_hotspot_11.0.12_7.pkg
      # remove jdk8 from packages_to_install
      packages_to_install=("${packages_to_install[@]/jdk}")
    fi

    if [[ "${packages_to_install[@]}" =~ "go" ]]; then
      # packages_to_install contains go
      log "Installing Go."
      wget -c https://dl.google.com/go/go1.15.15.darwin-amd64.pkg -P /tmp
      sudo installer -pkg /tmp/go1.15.15.darwin-amd64.pkg -target /
      rm -fr /tmp/go1.15.15.darwin-amd64.pkg
      sudo ln -sf /usr/local/go/bin/go /usr/local/bin/go
      # remove go from packages_to_install
      packages_to_install=("${packages_to_install[@]/go}")
    fi

    if [[ "${packages_to_install[@]}" =~ "zetcd" ]]; then
      log "Installing zetcd."
      export PATH=/usr/local/go/bin:${PATH}
      go get github.com/etcd-io/zetcd/cmd/zetcd
      # remove zetcd from packages_to_install
      packages_to_install=("${packages_to_install[@]/zetcd}")
    fi

    if [[ "${packages_to_install[@]}" =~ "rust" ]]; then
      # packages_to_install contains rust
      log "Installing rust."
      curl -sf -L https://static.rust-lang.org/rustup.sh | sh -s -- -y --profile minimal --default-toolchain 1.54.0
      # remove rust from packages_to_install
      packages_to_install=("${packages_to_install[@]/rust}")
    fi

    if [[ "${packages_to_install[@]}" =~ "folly" ]]; then
      install_folly=true  # set folly install flag
      packages_to_install=("${packages_to_install[@]/folly}")
      packages_to_install+=(fmt)
    fi

    log "Installing packages ${packages_to_install[*]}"
    brew install ${packages_to_install[*]}

    export OPENSSL_ROOT_DIR=/usr/local/opt/openssl
    export OPENSSL_LIBRARIES=/usr/local/opt/openssl/lib
    export OPENSSL_SSL_LIBRARY=/usr/local/opt/openssl/lib/libssl.dylib
    if [[ "${packages_to_install[@]}" =~ "llvm@${LLVM_VERSION}" ]]; then
      export CC=/usr/local/opt/llvm@${LLVM_VERSION}/bin/clang
      export CXX=/usr/local/opt/llvm@${LLVM_VERSION}/bin/clang++
      export LDFLAGS=-L/usr/local/opt/llvm@${LLVM_VERSION}/lib
      export CPPFLAGS=-I/usr/local/opt/llvm@${LLVM_VERSION}/include
    fi
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
      make -j${NUM_PROC}
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
    make -j${NUM_PROC}
    sudo make install
    popd
    rm -fr /tmp/v2020.10.19.00.tar.gz /tmp/folly-2020.10.19.00
  fi

  log "Installing python packages for vineyard codegen."
  pip3 install -U pip --user
  pip3 install grpcio-tools libclang parsec setuptools wheel twine --user

  log "Output environments config file ${OUTPUT_ENV_FILE}"
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
  # TODO: check vineyard version with vineyadd --version
  if command -v /usr/local/bin/vineyardd &> /dev/null && \
     [[ "$(head -n 1 /usr/local/lib/cmake/vineyard/vineyard-config-version.cmake | \
        awk -F '"' '{print $2}')" == "${V6D_VERSION}" ]]; then
    log "vineyard ${V6D_VERSION} already installed, skip."
    return 0
  fi

  check_and_remove_dir "/tmp/libvineyard"
  git clone -b ${V6D_BRANCH} --single-branch --depth=1 \
      https://github.com/alibaba/libvineyard.git /tmp/libvineyard
  pushd /tmp/libvineyard
  git submodule update --init
  mkdir -p build && pushd build
  if [[ "${PLATFORM}" == *"Darwin"* ]]; then
    cmake .. -DCMAKE_BUILD_TYPE=RelWithDebInfo \
             -DBUILD_VINEYARD_PYTHON_BINDINGS=ON -DBUILD_SHARED_LIBS=ON \
             -DBUILD_VINEYARD_IO_OSS=ON -DBUILD_VINEYARD_TESTS=OFF
  else
    cmake .. -DBUILD_SHARED_LIBS=ON \
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
    make install WITH_LEARNING_ENGINE=OFF INSTALL_PREFIX=${INSTALL_PREFIX} NETWORKX=OFF BUILD_TYPE=${BUILD_TYPE}
  else
    make install WITH_LEARNING_ENGINE=ON INSTALL_PREFIX=${INSTALL_PREFIX} BUILD_TYPE=${BUILD_TYPE}
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

  # parse args for install_deps command
  while test $# -ne 0; do
    arg=$1; shift
    case ${arg} in
      --help) install_deps_usage; exit ;;
      --verbose) VERBOSE=true; readonly VERBOSE; ;;
      *)
        echo "unrecognized option '${arg}'"
        install_deps_usage; exit;;
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

  succ_msg="Install dependencies successfully. The script had output the related
  environments to ${OUTPUT_ENV_FILE}.\n
  Please run 'source ${OUTPUT_ENV_FILE}' before run GraphScope."
  succ ${succ_msg}
  if [[ ${VERBOSE} == true ]]; then
    set +x
  fi
}

##########################
# Main function for build_and_deploy command.
# Globals:
#   VERBOSE
#   BUILD_TYPE
#   INSTALL_PREFIX
# Arguments:
#   None
# Outputs:
#   output log to stdout, output error to stderr.
##########################
build_and_deploy() {

  # parse args for install_deps command
  while test $# -ne 0; do
    arg=$1; shift
    case ${arg} in
      --help)        build_and_deploy_usage; exit ;;
      --verbose)     VERBOSE=true; readonly VERBOSE; ;;
      --build_type)  BUILD_TYPE=$1; readonly BUILD_TYPE; shift ;;
      --prefix)
        if [ $# -eq 0 ]; then
          echo "there should be given a path for prefix option."
          build_and_deploy_usage; exit;
        fi
        INSTALL_PREFIX=$1; readonly INSTALL_PREFIX; shift ;;
      *)
        echo "unrecognized option '${arg}'"
        build_and_deploy_usage; exit;;
    esac
  done

  if [[ ${VERBOSE} == true ]]; then
    set -x
  fi

  get_os_version

  check_os_compatibility

  # if .graphscope_env already exists, source it
  if [ -f "${OUTPUT_ENV_FILE}" ]; then
    log "Found env file ${OUTPUT_ENV_FILE} exists, source the env file."
    source ${OUTPUT_ENV_FILE}
  fi

  check_dependencies

  if [ ${#packages_to_install[@]} -ne 0 ]; then
    err "The dependences of GraphScope are not satisfied."
    err "These packages: [${packages_to_install[*]}] are not installed or their version are not compatible."
    exit 1
  fi

  install_libgrape-lite

  install_vineyard

  install_graphscope

  succ "GraphScope has been built successfully and installed on ${INSTALL_PREFIX}."
  succ "Please manually run:"
  succ "export GRAPHSCOPE_HOME=${INSTALL_PREFIX}"
  succ "before using GraphScope via Python client, enjoy!"
  if [[ ${VERBOSE} == true ]]; then
    set +x
  fi
}

set -e
set -o pipefail

# parse argv
# TODO(acezen): now the option need to specify before command, that's not user-friendly.
while test $# -ne 0; do
  arg=$1; shift
  case ${arg} in
    -h|--help)        usage; exit ;;
    install_deps)     install_deps "$@"; exit;;
    build_and_deploy) build_and_deploy "$@"; exit;;
    *)
      echo "unrecognized option or command '${arg}'"
      usage; exit 1;;
  esac
done
if test  $# -eq 0; then
  usage
fi

set +e
set +o pipefail
