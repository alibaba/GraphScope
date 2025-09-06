#!/bin/bash
set -e

# blue
info() {
  printf "\e[34m%b\e[0m\n" "$*"
}

# red
err() {
  printf "\e[31m%b\e[0m\n" "$*"
}

# yellow
warning() {
  printf "\e[1;33m%b\e[0m\n" "$*"
}

# red
debug() {
  printf "\e[31m%b\e[0m\n" "[DEBUG] $*"
}

get_os_version() {
  if [ -f /etc/centos-release ]; then
    # Older Red Hat, CentOS, Alibaba Cloud Linux etc.
    PLATFORM=CentOS
    OS_VERSION=$(sed 's/.* \([0-9]\).*/\1/' < /etc/centos-release)
    if grep -q "Alibaba Cloud Linux" /etc/centos-release; then
      PLATFORM="Aliyun_based_on_CentOS"
      OS_VERSION=$(rpm -E %{rhel})
    fi
  elif [ -f /etc/os-release ]; then
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
  else
    # Fall back to uname, e.g. "Linux <version>", also works for BSD, Darwin, etc.
    PLATFORM=$(uname -s)
    OS_VERSION=$(uname -r)
  fi
  if [[ "${PLATFORM}" != *"Ubuntu"* && "${PLATFORM}" != *"CentOS"* && "${PLATFORM}" != *"Darwin"* && "${PLATFORM}" != *"Aliyun"* ]];then
    err "Only support on Ubuntu/CentOS/macOS/AliyunOS platform."
    exit 1
  fi
  if [[ "${PLATFORM}" == *"Ubuntu"* && "${OS_VERSION:0:2}" -lt "20" ]]; then
    err "Ubuntu ${OS_VERSION} found, requires 20 or greater."
    exit 1
  fi
  if [[ "${PLATFORM}" == *"CentOS"* && "${OS_VERSION}" -lt "7" ]]; then
    err "CentOS ${OS_VERSION} found, requires 8 or greater."
    exit 1
  fi
  if [[ "${PLATFORM}" == *"Darwin"* ]]; then
    export HOMEBREW_NO_INSTALL_CLEANUP=1
    export HOMEBREW_NO_INSTALLED_DEPENDENTS_CHECK=1
  fi
  echo "$PLATFORM-$OS_VERSION"
}

# default values
readonly OS=$(get_os_version)
readonly OS_PLATFORM=${OS%-*}
readonly OS_VERSION=${OS#*-}
readonly ARCH=$(uname -m)
readonly OUTPUT_ENV_FILE="${HOME}/.graphscope_env"
if [[ "${OS_PLATFORM}" == *"Darwin"* ]]; then
  readonly HOMEBREW_PREFIX=$(brew --prefix)
fi
readonly ARROW_VERSION="15.0.2"
readonly MPI_PREFIX="/opt/openmpi"  # fixed, related to coordinator/setup.py
readonly V6D_PREFIX="/opt/vineyard" # fixed, related to coordinator/setup.py
readonly tempdir="/tmp/gs-local-deps"
v6d_version="main"
no_v6d=false
for_analytical=false
for_analytical_java=false
for_interactive=false
for_learning=false
cn_flag=false
debug_flag=false
install_prefix="/opt/graphscope"

# parse args
while (( "$#" )); do
  case "$1" in
    dev)
      for_analytical=true
      for_analytical_java=true
      for_interactive=true
      for_learning=true
      shift
      ;;
    dev-analytical)
      for_analytical=true
      shift
      ;;
    dev-analytical-java)
      for_analytical=true
      for_analytical_java=true
      shift
      ;;
    dev-interactive)
      for_interactive=true
      shift
      ;;
    dev-learning)
      for_learning=true
      shift
      ;;
    --install-prefix)
      install_prefix="$2"
      shift 2
      ;;
    --v6d-version)
      v6d_version="$2"
      shift 2
      ;;
    --no-v6d)
      no_v6d=true
      shift
      ;;
    --cn)
      cn_flag=true
      shift
      ;;
    --debug)
      debug_flag=true
      shift
      ;;
    *)
      shift
      ;;
  esac
done

if [[ ${for_analytical} == false && ${for_interactive} == false && ${for_learning} == false ]]; then
    usage="Usage: ${0} dev/dev-analytical/dev-analytical-java/dev-interactive/dev-learning"
    usage="${usage} [--cn] [--v6d-version <version>] [--install-prefix <path>] [--no-v6d] [--debug]"
    err "${usage}"
    exit 0
fi

if [[ ${debug_flag} == true ]]; then
  debug "OS: ${OS}, OS_PLATFORM: ${OS_PLATFORM}, OS_VERSION: ${OS_VERSION}"
  debug "install dependencies for analytical=${for_analytical}, analytical-java=${for_analytical_java}, v6d_version=${v6d_version}, no_v6d=${no_v6d}"
  debug "install dependencies for interactive=${for_interactive}, learning=${for_learning}"
  debug "install prefix: ${install_prefix}"
fi

# sudo
SUDO=sudo
if [[ $(id -u) -eq 0 ]]; then
  SUDO=""
fi

# speed up
if [ "${cn_flag}" == true ]; then
  export HOMEBREW_BREW_GIT_REMOTE="https://mirrors.tuna.tsinghua.edu.cn/git/homebrew/brew.git"
  export HOMEBREW_CORE_GIT_REMOTE="https://mirrors.tuna.tsinghua.edu.cn/git/homebrew/homebrew-core.git"
  export HOMEBREW_BOTTLE_DOMAIN="https://mirrors.tuna.tsinghua.edu.cn/homebrew-bottles"
fi

# install functions
init_workspace_and_env() {
  info "creating directory: ${install_prefix} ${tempdir} ${V6D_PREFIX}"
  ${SUDO} mkdir -p ${install_prefix} ${tempdir} ${V6D_PREFIX}
  ${SUDO} chown -R $(id -u):$(id -g) ${install_prefix} ${tempdir} ${V6D_PREFIX}
  export PATH=${install_prefix}/bin:${PATH}
  export LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:${install_prefix}/lib:${install_prefix}/lib64
}

# utils functions
function set_to_cn_url() {
  local url=$1
  if [[ ${cn_flag} == true ]]; then
    url="https://graphscope.oss-cn-beijing.aliyuncs.com/dependencies"
  fi
  echo ${url}
}

function fetch_source() {
  local url=$1
  local file=$2
  curl -fsSL -o "${file}" "${url}/${file}"
}

function download_and_untar() {
  local url=$1
  local file=$2
  local directory=$3
  if [ ! -d "${directory}" ]; then
    [ ! -f "${file}" ] && fetch_source "${url}" "${file}"
    tar zxf "${file}"
  fi
}

function git_clone() {
  local url=$1
  local file=$2
  local directory=$3
  local branch=$4
  if [ ! -d "${directory}" ]; then
    if [ ! -f "${file}" ]; then
      git clone --depth=1 --branch "${branch}" "${url}" "${directory}"
      pushd "${directory}" || exit
      git submodule update --init || true
      popd || exit
    else
      tar zxf "${file}"
    fi
  fi
}

# cmake for centos
install_cmake() {
  if [[ -f "${install_prefix}/bin/cmake" ]]; then
    return 0
  fi
  pushd "${tempdir}" || exit
  file="cmake-3.24.3-linux-${ARCH}.sh"
  url="https://github.com/Kitware/CMake/releases/download/v3.24.3"
  url=$(set_to_cn_url ${url})
  [ ! -f "${file}" ] && fetch_source "${url}" "${file}"
  bash "${file}" --prefix="${install_prefix}" --skip-license
  popd || exit
  rm -rf "${tempdir:?}/${file:?}"
}

# gflags for centos
install_gflags() {
  if [[ -f "${install_prefix}/include/gflags/gflags.h" ]]; then
    return 0
  fi
  pushd "${tempdir}" || exit
  directory="gflags-2.2.2"
  file="v2.2.2.tar.gz"
  url="https://github.com/gflags/gflags/archive"
  url=$(set_to_cn_url ${url})
  download_and_untar "${url}" "${file}" "${directory}"
  pushd ${directory} || exit
  cmake . -DCMAKE_INSTALL_PREFIX="${install_prefix}" \
          -DCMAKE_PREFIX_PATH="${install_prefix}" \
          -DBUILD_SHARED_LIBS=ON
  make -j$(nproc)
  make install
  popd || exit
  popd || exit
  rm -rf "${tempdir:?}/${directory:?}" "${tempdir:?}/${file:?}"
}

# glog for centos
install_glog() {
  if [[ -f "${install_prefix}/include/glog/logging.h" ]]; then
    return 0
  fi
  pushd "${tempdir}" || exit
  directory="glog-0.6.0"
  file="v0.6.0.tar.gz"
  url="https://github.com/google/glog/archive"
  url=$(set_to_cn_url ${url})
  download_and_untar "${url}" "${file}" "${directory}"
  pushd ${directory} || exit
  cmake . -DCMAKE_INSTALL_PREFIX="${install_prefix}" \
          -DCMAKE_PREFIX_PATH="${install_prefix}" \
          -DBUILD_SHARED_LIBS=ON
  make -j$(nproc)
  make install
  popd || exit
  popd || exit
  rm -rf "${tempdir:?}/${directory:?}" "${tempdir:?}/${file:?}"
}

# boost with leaf for centos and ubuntu
install_boost() {
  if [[ -f "${install_prefix}/include/boost/version.hpp" ]]; then
    return 0
  fi
  pushd "${tempdir}" || exit
  directory="boost_1_75_0"
  file="${directory}.tar.gz"
  url="https://archives.boost.io/release/1.75.0/source"
  url=$(set_to_cn_url ${url})
  download_and_untar "${url}" "${file}" "${directory}"
  pushd ${directory} || exit
  # seastar needs filesystem program_options thread unit_test_framework
  # interactive needs context regex date_time
  ./bootstrap.sh --prefix="${install_prefix}" \
    --with-libraries=system,filesystem,context,program_options,regex,thread,random,chrono,atomic,date_time,test
  ./b2 install link=shared runtime-link=shared variant=release threading=multi
  popd || exit
  popd || exit
  rm -rf "${tempdir:?}/${directory:?}" "${tempdir:?}/${file:?}"
}

# openssl for centos
install_openssl() {
  if [[ -f "${install_prefix}/include/openssl/ssl.h" ]]; then
    return 0
  fi
  pushd "${tempdir}" || exit
  directory="openssl-OpenSSL_1_1_1k"
  file="OpenSSL_1_1_1k.tar.gz"
  url="https://github.com/openssl/openssl/archive"
  url=$(set_to_cn_url ${url})
  download_and_untar "${url}" "${file}" "${directory}"
  pushd ${directory} || exit
  ./config --prefix="${install_prefix}" -fPIC -shared
  make -j$(nproc)
  make install
  popd || exit
  popd || exit
  rm -rf "${tempdir:?}/${directory:?}" "${tempdir:?}/${file:?}"
}

# static openssl for centos 8
install_openssl_static() {
  if [[ -f "${install_prefix}/include/openssl/ssl.h" ]]; then
    return 0
  fi
  pushd "${tempdir}" || exit
  directory="openssl-OpenSSL_1_1_1k"
  file="OpenSSL_1_1_1k.tar.gz"
  url="https://github.com/openssl/openssl/archive"
  url=$(set_to_cn_url ${url})
  download_and_untar "${url}" "${file}" "${directory}"
  pushd ${directory} || exit
  ./config --prefix="${install_prefix}" no-shared -fPIC
  make -j$(nproc)
  make install
  popd || exit
  popd || exit
  rm -rf "${tempdir:?}/${directory:?}" "${tempdir:?}/${file:?}"
}

# arrow for ubuntu and centos
install_arrow() {
  if [[ "${OS_PLATFORM}" == *"Ubuntu"* ]]; then
    if ! dpkg -s libarrow-dev &>/dev/null; then
      ${SUDO} apt-get install -y lsb-release
      # shellcheck disable=SC2046,SC2019,SC2018
      wget -c https://apache.jfrog.io/artifactory/arrow/"$(lsb_release --id --short | tr 'A-Z' 'a-z')"/apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb -P /tmp/
      ${SUDO} apt-get install -y -V /tmp/apache-arrow-apt-source-latest-"$(lsb_release --codename --short)".deb
      ${SUDO} apt-get update -y
      ${SUDO} apt-get install -y libarrow-dev=${ARROW_VERSION}-1 libarrow-dataset-dev=${ARROW_VERSION}-1 libarrow-acero-dev=${ARROW_VERSION}-1 libparquet-dev=${ARROW_VERSION}-1
      rm /tmp/apache-arrow-apt-source-latest-*.deb
    fi
  elif [[ "${OS_PLATFORM}" == *"CentOS"* || "${OS_PLATFORM}" == *"Aliyun"* ]]; then
    install_arrow_centos
  fi
}

# arrow for centos
install_arrow_centos() {
  if [[ -f "${install_prefix}/include/arrow/api.h" ]]; then
    return 0
  fi
  pushd "${tempdir}" || exit
  directory="arrow-apache-arrow-${ARROW_VERSION}"
  file="apache-arrow-${ARROW_VERSION}.tar.gz"
  url="https://github.com/apache/arrow/archive"
  url=$(set_to_cn_url ${url})
  download_and_untar "${url}" "${file}" "${directory}"
  pushd ${directory} || exit
  cmake ./cpp \
    -DCMAKE_PREFIX_PATH="${install_prefix}" \
    -DCMAKE_INSTALL_PREFIX="${install_prefix}" \
    -DARROW_COMPUTE=ON \
    -DARROW_WITH_UTF8PROC=OFF \
    -DARROW_CSV=ON \
    -DARROW_CUDA=OFF \
    -DARROW_DATASET=OFF \
    -DARROW_FILESYSTEM=ON \
    -DARROW_FLIGHT=OFF \
    -DARROW_GANDIVA=OFF \
    -DARROW_HDFS=OFF \
    -DARROW_JSON=OFF \
    -DARROW_ORC=OFF \
    -DARROW_PARQUET=OFF \
    -DARROW_PLASMA=OFF \
    -DARROW_PYTHON=OFF \
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
    -DARROW_BUILD_STATIC=OFF
  make -j$(nproc)
  make install
  popd || exit
  popd || exit
  rm -rf "${tempdir:?}/${directory:?}" "${tempdir:?}/${file:?}"
}

# openmpi for centos
install_openmpi() {
  if [[ -f "${install_prefix}/include/mpi.h" ]]; then
    return 0
  fi
  info "creating directory: ${MPI_PREFIX}"
  ${SUDO} mkdir -p ${MPI_PREFIX}
  ${SUDO} chown -R $(id -u):$(id -g) ${MPI_PREFIX}
  pushd "${tempdir}" || exit
  directory="openmpi-4.0.5"
  file="openmpi-4.0.5.tar.gz"
  url="https://download.open-mpi.org/release/open-mpi/v4.0"
  url=$(set_to_cn_url ${url})
  download_and_untar "${url}" "${file}" "${directory}"
  pushd ${directory} || exit
  ./configure --enable-mpi-cxx --disable-dlopen --prefix=${MPI_PREFIX}
  make -j$(nproc)
  make install
  popd || exit
  popd || exit
  cp -rs ${MPI_PREFIX}/* "${install_prefix}"
  rm -rf "${tempdir:?}/${directory:?}" "${tempdir:?}/${file:?}"
}

# protobuf for centos
install_protobuf() {
  if [[ -f "${install_prefix}/include/google/protobuf/port.h" ]]; then
    return 0
  fi
  pushd "${tempdir}" || exit
  directory="protobuf-21.9"
  file="protobuf-all-21.9.tar.gz"
  url="https://github.com/protocolbuffers/protobuf/releases/download/v21.9"
  url=$(set_to_cn_url ${url})
  download_and_untar "${url}" "${file}" "${directory}"
  pushd ${directory} || exit
  ./configure --prefix="${install_prefix}" --enable-shared --disable-static
  make -j$(nproc)
  make install
  popd || exit
  popd || exit
  rm -rf "${tempdir:?}/${directory:?}" "${tempdir:?}/${file:?}"
}

# zlib for centos
install_zlib() {
  if [[ -f "${install_prefix}/include/zlib.h" ]]; then
    return 0
  fi
  pushd "${tempdir}" || exit
  directory="zlib-1.2.11"
  file="v1.2.11.tar.gz"
  url="https://github.com/madler/zlib/archive"
  url=$(set_to_cn_url ${url})
  download_and_untar "${url}" "${file}" "${directory}"
  pushd ${directory} || exit
  cmake . -DCMAKE_INSTALL_PREFIX="${install_prefix}" \
          -DCMAKE_PREFIX_PATH="${install_prefix}" \
          -DBUILD_SHARED_LIBS=ON
  make -j$(nproc)
  make install
  popd || exit
  popd || exit
  rm -rf "${tempdir:?}/${directory:?}" "${tempdir:?}/${file:?}"
}

install_mimalloc() {
  pushd "${tempdir}" || exit
  git clone https://github.com/microsoft/mimalloc -b v1.8.6
  cd mimalloc
  mkdir -p build && cd build
  cmake .. -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_INSTALL_PREFIX="${install_prefix}"
  make -j$(nproc)
  make install
  popd || exit
  rm -rf "${tempdir:?}/mimalloc"
}

# opentelemetry
install_opentelemetry() {
  pushd "${tempdir}" || exit
  git clone https://github.com/open-telemetry/opentelemetry-cpp -b v1.15.0
  cd opentelemetry-cpp
  cmake . -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_INSTALL_PREFIX="${install_prefix}" \
    -DCMAKE_CXX_STANDARD=17 \
    -DCMAKE_POSITION_INDEPENDENT_CODE=ON \
    -DBUILD_SHARED_LIBS=ON \
    -DWITH_OTLP_HTTP=ON \
    -DWITH_OTLP_GRPC=OFF \
    -DWITH_ABSEIL=OFF \
    -DWITH_PROMETHEUS=OFF \
    -DBUILD_TESTING=OFF \
    -DWITH_EXAMPLES=OFF
  make -j -j$(nproc)
  make install
  popd || exit
  rm -rf "${tempdir:?}/opentelemetry-cpp"
}

# grpc for centos
install_grpc() {
  if [[ -f "${install_prefix}/include/grpcpp/grpcpp.h" ]]; then
    return 0
  fi
  directory="grpc"
  branch="v1.49.1"
  file="${directory}-${branch}.tar.gz"
  url="https://github.com/grpc/grpc.git"
  url=$(set_to_cn_url ${url})
  pushd "${tempdir}" || exit
  if [[ ${url} == *.git ]]; then
    git_clone "${url}" "${file}" "${directory}" "${branch}"
  else
    download_and_untar "${url}" "${file}" "${directory}"
  fi
  pushd ${directory} || exit
  cmake . -DCMAKE_INSTALL_PREFIX="${install_prefix}" \
          -DCMAKE_PREFIX_PATH="${install_prefix}" \
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
          -DOPENSSL_ROOT_DIR="${install_prefix}" \
          -DCMAKE_CXX_FLAGS="-fpermissive" \
          -DPNG_ARM_NEON_OPT=0
  make -j$(nproc)
  make install
  popd || exit
  popd || exit
  rm -rf "${tempdir:?}/${directory:?}" "${tempdir:?}/${file:?}"
}

# patchelf
install_patchelf() {
  if [[ -f "${install_prefix}/bin/patchelf" ]]; then
    return 0
  fi
  pushd "${tempdir}" || exit
  directory="patchelf"  # patchelf doesn't have a folder
  file="patchelf-0.14.5-${ARCH}.tar.gz"
  url="https://github.com/NixOS/patchelf/releases/download/0.14.5"
  url=$(set_to_cn_url ${url})
  mkdir -p "${directory}"
  pushd "${directory}" || exit
  download_and_untar "${url}" "${file}" "${directory}"
  mkdir -p ${install_prefix}/bin
  mv bin/patchelf ${install_prefix}/bin/patchelf
  popd || exit
  popd || exit
  rm -rf "${tempdir:?}/${directory:?}" "${tempdir:?}/${file:?}"
}

# libgrape-lite
install_libgrape_lite() {
  if [[ "${OS_PLATFORM}" == *"Darwin"* ]]; then
    if [[ -f "${HOMEBREW_PREFIX}/include/grape/grape.h" ]]; then
      return 0
    fi
  else
    if [[ -f "${install_prefix}/include/grape/grape.h" ]]; then
      return 0
    fi
  fi
  local branch=$1
  pushd "${tempdir}" || exit
  git clone -b ${branch} https://github.com/alibaba/libgrape-lite.git
  cd libgrape-lite
  # Configure the minimum required version of cmake to 3.5: https://github.com/alibaba/GraphScope/actions/runs/14216252611/job/39833569906?pr=4591
  cmake . -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_INSTALL_PREFIX="${install_prefix}" \
    -DBUILD_LIBGRAPELITE_TESTS=OFF \
    -DCMAKE_POLICY_VERSION_MINIMUM=3.5
  make -j$(nproc)
  make install
  popd || exit
  rm -rf "${tempdir:?}/libgrape-lite"
}

# vineyard
install_vineyard() {
  if [[ -f "${V6D_PREFIX}/include/vineyard/client/client.h" ]]; then
    return 0
  fi
  pushd "${tempdir}" || exit
  python3 -m pip --no-cache-dir install pip -U --user
  python3 -m pip --no-cache-dir install libclang wheel auditwheel --user
  auditwheel_path=$(python3 -c "import auditwheel; print(auditwheel.__path__[0] + '/main_repair.py')")
  if [[ "${OS_PLATFORM}" == *"Darwin"* ]]; then
    BUILD_VINEYARD_GRAPH_WITH_GAR="OFF"
    sed -i '' 's/p.error/logger.warning/g' ${auditwheel_path}
  else
    BUILD_VINEYARD_GRAPH_WITH_GAR="ON"
    sed -i 's/p.error/logger.warning/g' ${auditwheel_path}
  fi
  if [[ "${v6d_version}" != "v"* ]]; then
    directory="v6d"
    file="${directory}-${v6d_version}.tar.gz"
    url="https://github.com/v6d-io/v6d.git"
    git_clone "${url}" "${file}" "${directory}" "${v6d_version}"
  else
    # remove the prefix 'v'
    directory="v6d-${v6d_version:1:100}"
    file="${directory}.tar.gz"
    url="https://github.com/v6d-io/v6d/releases/download/${v6d_version}"
    download_and_untar "${url}" "${file}" "${directory}"
  fi
  pushd ${directory} || exit

  cmake . -DCMAKE_PREFIX_PATH="${install_prefix}" \
        -DCMAKE_INSTALL_PREFIX="${V6D_PREFIX}" \
        -DBUILD_VINEYARD_TESTS=OFF \
        -DBUILD_SHARED_LIBS=ON \
        -DBUILD_VINEYARD_PYTHON_BINDINGS=ON  \
        -DBUILD_VINEYARD_GRAPH_WITH_GAR=${BUILD_VINEYARD_GRAPH_WITH_GAR}
  make -j$(nproc)
  make install
  if [[ "${OS_PLATFORM}" == *"Darwin"* ]]; then
    strip "${V6D_PREFIX}"/bin/vineyard*
  else
    strip "${V6D_PREFIX}"/bin/vineyard* "${V6D_PREFIX}"/lib/libvineyard*
  fi
  pip3 install --no-cache -i https://pypi.org/simple -U "vineyard" "vineyard-io"
  cp -rs "${V6D_PREFIX}"/* "${install_prefix}"/
  set +e
  popd || exit
  popd || exit
  rm -rf "${tempdir:?}/${directory:?}" "${tempdir:?}/${file:?}"
}

# java
install_java_and_maven() {
  if [[ "${OS_PLATFORM}" == *"Darwin"* ]]; then
    if ! command -v javac &>/dev/null; then
      brew install --ignore-dependencies openjdk@11
    fi
    if ! command -v mvn &>/dev/null; then
      brew install --ignore-dependencies maven
    fi
  elif [[ "${OS_PLATFORM}" == *"Ubuntu"* ]]; then
    if ! command -v javac &>/dev/null; then
      ${SUDO} apt-get install default-jdk -y
    fi
    if ! command -v mvn &>/dev/null; then
      ${SUDO} apt-get install maven -y
    fi
  else
    if ! command -v javac &>/dev/null; then
      ${SUDO} yum install java-11-openjdk-devel -y
    fi
    if ! command -v mvn &>/dev/null; then
      install_maven
    fi
  fi
}

# maven for centos
install_maven() {
  if [[ -f "${install_prefix}/bin/mvn" ]]; then
    return 0
  fi
  pushd "${tempdir}" || exit
  directory="apache-maven-3.8.6"
  file="apache-maven-3.8.6-bin.tar.gz"
  url="https://archive.apache.org/dist/maven/maven-3/3.8.6/binaries"
  url=$(set_to_cn_url ${url})
  download_and_untar "${url}" "${file}" "${directory}"
  cp -r ${directory} "${install_prefix}"/
  mkdir -p "${install_prefix}"/bin
  ln -s "${install_prefix}/${directory}/bin/mvn" "${install_prefix}/bin/mvn"
  popd || exit
  rm -rf "${tempdir:?}/${directory:?}" "${tempdir:?}/${file:?}"
}

install_hiactor() {
  if [[ -f "${install_prefix}/include/hiactor/core/actor_core.hh" ]]; then
    return 0
  fi
  pushd "${tempdir}" || exit
  git clone https://github.com/alibaba/hiactor.git -b v0.1.1 --single-branch
  cd hiactor && git submodule update --init --recursive
  sudo bash ./seastar/seastar/install-dependencies.sh
  mkdir build && cd build
  cmake -DHiactor_DEMOS=OFF -DHiactor_TESTING=OFF -DHiactor_DPDK=OFF -DCMAKE_INSTALL_PREFIX="${install_prefix}" \
        -DHiactor_CXX_DIALECT=gnu++17 -DSeastar_CXX_FLAGS="-DSEASTAR_DEFAULT_ALLOCATOR -mno-avx512" ..
  make -j$(nproc) && make install
  popd && rm -rf "${tempdir}/hiactor"
}


install_cppkafka() {
  if [[ -f "${install_prefix}/include/cppkafka/cppkafka.h" ]]; then
    return 0
  fi
  pushd "${tempdir}" || exit
  directory="cppkafka-0.4.0"
  file="0.4.0.tar.gz"
  url="https://graphscope.oss-cn-beijing.aliyuncs.com/dependencies"
  url=$(set_to_cn_url ${url})
  download_and_untar "${url}" "${file}" "${directory}"
  pushd ${directory} || exit
  # cppkafka may not find the lib64 directory
  export LIBRARY_PATH=${LIBRARY_PATH}:${install_prefix}/lib:${install_prefix}/lib64
  cmake . -DCMAKE_INSTALL_PREFIX="${install_prefix}" \
          -DCMAKE_PREFIX_PATH="${install_prefix}" \
          -DCPPKAFKA_DISABLE_TESTS=ON  \
          -DCPPKAFKA_DISABLE_EXAMPLES=ON
  make -j4
  make install
  popd || exit
  popd || exit
  rm -rf "${tempdir:?}/${directory:?}" "${tempdir:?}/${file:?}"
}


BASIC_PACKAGES_LINUX=("file" "curl" "wget" "git" "sudo")
BASIC_PACKAGES_UBUNTU=("${BASIC_PACKAGES_LINUX[@]}" "build-essential" "cmake" "libunwind-dev" "python3-pip")
BASIC_PACKAGES_CENTOS_8=("${BASIC_PACKAGES_LINUX[@]}" "epel-release" "libunwind-devel" "libcurl-devel" "perl" "which")
BASIC_PACKAGES_CENTOS_7=("${BASIC_PACKAGES_CENTOS_8[@]}" "centos-release-scl-rh")
ADDITIONAL_PACKAGES_CENTOS_8=("gcc-c++" "python38-devel")
ADDITIONAL_PACKAGES_CENTOS_7=("make" "devtoolset-8-gcc-c++" "rh-python38-python-pip" "rh-python38-python-devel")

install_basic_packages() {
  if [[ "${OS_PLATFORM}" == *"Ubuntu"* ]]; then
    ${SUDO} apt-get update -y
    DEBIAN_FRONTEND=noninteractive TZ=Etc/UTC ${SUDO} apt-get install -y ${BASIC_PACKAGES_UBUNTU[*]}
  elif [[ "${OS_PLATFORM}" == *"CentOS"* || "${OS_PLATFORM}" == *"Aliyun"* ]]; then
    ${SUDO} yum update -y
    if [[ "${OS_VERSION}" -eq "7" ]]; then
      # centos7
      ${SUDO} yum install -y ${BASIC_PACKAGES_CENTOS_7[*]}
      # change the source for centos-release-scl-rh
      ${SUDO} sed -i 's/mirrorlist/#mirrorlist/g' /etc/yum.repos.d/CentOS-*scl*
      ${SUDO} sed -i 's|#baseurl=http://mirror.centos.org|baseurl=http://vault.centos.org|g' /etc/yum.repos.d/CentOS-*scl*
      ${SUDO} sed -i 's|# baseurl=http://mirror.centos.org|baseurl=http://vault.centos.org|g' /etc/yum.repos.d/CentOS-*scl*
      ${SUDO} yum install -y ${ADDITIONAL_PACKAGES_CENTOS_7[*]}
	  else
      if [[ "${OS_PLATFORM}" == *"Aliyun"* ]]; then
        ${SUDO} yum install -y 'dnf-command(config-manager)'
        ${SUDO} dnf install -y epel-release --allowerasing
      else
        ${SUDO} sed -i 's/mirrorlist/#mirrorlist/g' /etc/yum.repos.d/CentOS-*
        ${SUDO} sed -i 's|#baseurl=http://mirror.centos.org|baseurl=http://vault.centos.org|g' /etc/yum.repos.d/CentOS-*
        ${SUDO} yum install -y 'dnf-command(config-manager)'
        ${SUDO} dnf install -y epel-release
        ${SUDO} dnf config-manager --set-enabled powertools
      fi
      ${SUDO} dnf config-manager --set-enabled epel
      ${SUDO} yum install -y ${BASIC_PACKAGES_CENTOS_8[*]}
      ${SUDO} yum install -y ${ADDITIONAL_PACKAGES_CENTOS_8[*]}
    fi
  fi
}

ANALYTICAL_MACOS=("apache-arrow" "boost" "gflags" "glog" "open-mpi" "openssl@1.1" "protobuf" "grpc" "rapidjson" "msgpack-cxx" "librdkafka" "patchelf")
ANALYTICAL_UBUNTU=("libopenmpi-dev" "libgflags-dev" "libgoogle-glog-dev" "libprotobuf-dev" "libgrpc++-dev" "libmsgpack-dev" "librdkafka-dev" "protobuf-compiler-grpc" "rapidjson-dev")
ANALYTICAL_CENTOS_7=("librdkafka-devel" "msgpack-devel" "rapidjson-devel")
ANALYTICAL_CENTOS_8=("${ANALYTICAL_CENTOS_7[@]}" "boost-devel" "gflags-devel" "glog-devel")

install_analytical_centos_common_dependencies() {
  install_patchelf
  install_arrow_centos
  install_openmpi
  install_protobuf
  install_zlib
  install_grpc
}

install_analytical_dependencies() {
  if [[ "${OS_PLATFORM}" == *"Darwin"* ]]; then
    brew install ${ANALYTICAL_MACOS[*]}
    export LDFLAGS="${LDFLAGS} -L${HOMEBREW_PREFIX}/opt/openssl@1.1/lib"
    export CPPFLAGS="${CPPFLAGS} -I${HOMEBREW_PREFIX}/opt/openssl@1.1/include"
    export PKG_CONFIG_PATH="${HOMEBREW_PREFIX}/opt/openssl@1.1/lib/pkgconfig"
  elif [[ "${OS_PLATFORM}" == *"Ubuntu"* ]]; then
    DEBIAN_FRONTEND=noninteractive TZ=Etc/UTC ${SUDO} apt-get install -y ${ANALYTICAL_UBUNTU[*]}
    # patchelf
    install_patchelf
    # arrow
    install_arrow
    # install boost >= 1.75 for leaf
    install_boost
  else
    if [[ "${OS_VERSION}" -eq "7" ]]; then
      ${SUDO} yum install -y ${ANALYTICAL_CENTOS_7[*]}
      source /opt/rh/devtoolset-8/enable
      source /opt/rh/rh-python38/enable
      export LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:${install_prefix}/lib:${install_prefix}/lib64
      install_cmake
      install_gflags
      install_glog
      install_boost
      # must to install openssl before grpc
      install_openssl
    else
      ${SUDO} yum install -y ${ANALYTICAL_CENTOS_8[*]}
      export LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:/lib/:/lib64${install_prefix}/lib:${install_prefix}/lib64
      install_cmake
      install_openssl_static
    fi
    install_analytical_centos_common_dependencies
  fi
  # install java for gae-java
  install_java_and_maven
  # install vineyard
  if [[ "${no_v6d}" != true ]]; then
    install_vineyard
    # if [[ "${OS_PLATFORM}" == *"Darwin"* ]]; then
    #   brew install vineyard
    # else
    #   install_vineyard
    # fi
  fi
}

install_analytical_java_dependencies() {
  # llvm
  if [[ "${OS_PLATFORM}" == *"Darwin"* ]]; then
    brew install llvm || true # prevent the `brew link` failure
    export CC=${HOMEBREW_PREFIX}/opt/llvm/bin/clang
    export CXX=${HOMEBREW_PREFIX}/opt/llvm/bin/clang++
    export CPPFLAGS="${CPPFLAGS} -I${HOMEBREW_PREFIX}/opt/llvm/include"
    export CARGO_TARGET_X86_64_APPLE_DARWIN_LINKER=${CC}
  elif [[ "${OS_PLATFORM}" == *"Ubuntu"* ]]; then
    ${SUDO} apt-get install -y llvm-11-dev lld-11 clang-11
  else
    if [[ "${OS_VERSION}" -eq "7" ]]; then
      ${SUDO} yum install -y llvm-toolset-7.0-clang-devel
      source /opt/rh/llvm-toolset-7.0/enable
      export LIBCLANG_PATH=/opt/rh/llvm-toolset-7.0/root/usr/lib64/
    else
      ${SUDO} yum install -y llvm-devel clang-devel lld
    fi
  fi
}

INTERACTIVE_MACOS=("apache-arrow" "rapidjson" "boost" "glog" "gflags" "yaml-cpp" "protobuf")
INTERACTIVE_UBUNTU=("rapidjson-dev" "libgoogle-glog-dev" "libgflags-dev" "libyaml-cpp-dev" "libprotobuf-dev" "libgflags-dev")
INTERACTIVE_CENTOS=("rapidjson-devel" "glog-devel")

install_interactive_dependencies() {
  # dependencies package
  if [[ "${OS_PLATFORM}" == *"Darwin"* ]]; then
    brew install ${INTERACTIVE_MACOS[*]}
  elif [[ "${OS_PLATFORM}" == *"Ubuntu"* ]]; then
    DEBIAN_FRONTEND=noninteractive TZ=Etc/UTC ${SUDO} apt-get install -y ${INTERACTIVE_UBUNTU[*]}
    install_arrow
    install_boost
    # hiactor is only supported on ubuntu
    install_hiactor
    install_mimalloc
    ${SUDO} sh -c 'echo "fs.aio-max-nr = 1048576" >> /etc/sysctl.conf'
    ${SUDO} sysctl -p /etc/sysctl.conf
  else
    ${SUDO} yum install -y ${INTERACTIVE_CENTOS[*]}
    install_arrow
    install_boost
    install_mimalloc
  fi
  # libgrape-lite
  install_libgrape_lite "v0.3.2"
  # java
  install_java_and_maven
  # rust
  if ! command -v rustup &>/dev/null; then
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
    source $HOME/.cargo/env
    rustup install 1.87.0
    rustup default 1.87.0
    rustc --version
  fi
  # opentelemetry
  if [[ "${OS_PLATFORM}" != *"Darwin"* ]]; then
    # opentelemetry expect libprotoc >= 3.13.0, see https://github.com/open-telemetry/opentelemetry-cpp/discussions/2223
    proto_version=$(protoc --version | awk '{print $2}')
    major_version=$(echo ${proto_version} | cut -d'.' -f1)
    minor_version=$(echo ${proto_version} | cut -d'.' -f2)
    if [[ ${major_version} -lt 3 ]] || [[ ${major_version} -eq 3 && ${minor_version} -lt 13 ]]; then
      warning "OpenTelemetry requires protoc >= 3.13, current version is ${proto_version}, please upgrade it."
    else
      install_opentelemetry
    fi
  fi
}

install_learning_dependencies() {
  install_cppkafka
}

write_env_config() {
  echo "" > ${OUTPUT_ENV_FILE}
  # common
  {
    echo "export GRAPHSCOPE_HOME=${install_prefix}"
    echo "export CMAKE_PREFIX_PATH=/opt/vineyard:/opt/graphscope/"
    echo "export PATH=${install_prefix}/bin:\$HOME/.local/bin:\$HOME/.cargo/bin:\$PATH"
    echo "export LD_LIBRARY_PATH=${LD_LIBRARY_PATH}"
    echo "export LIBRARY_PATH=${install_prefix}/lib:${install_prefix}/lib64"
  } >> "${OUTPUT_ENV_FILE}"
  {
    if [[ "${OS_PLATFORM}" == *"Darwin"* ]]; then
      echo "export OPENSSL_ROOT_DIR=${HOMEBREW_PREFIX}/opt/openssl"
      echo "export OPENSSL_LIBRARIES=${HOMEBREW_PREFIX}/opt/openssl/lib"
      echo "export OPENSSL_SSL_LIBRARY=${HOMEBREW_PREFIX}/opt/openssl/lib/libssl.dylib"
    elif [[ "${OS_PLATFORM}" == *"CentOS"* || "${OS_PLATFORM}" == *"Aliyun"* ]]; then
      if [[ "${OS_VERSION}" -eq "7" ]]; then
        echo "source /opt/rh/devtoolset-8/enable"
        echo "source /opt/rh/rh-python38/enable"
      fi
      echo "export OPENSSL_ROOT_DIR=${install_prefix}"
    fi
  } >> "${OUTPUT_ENV_FILE}"
  # JAVA_HOME
  {
   	if [[ "${for_analytical}" == true || "${for_interactive}" == true ]]; then
      if [[ "${OS_PLATFORM}" == *"Darwin"* ]]; then
        if [ -z "${JAVA_HOME}" ]; then
          echo "export JAVA_HOME=\$(/usr/libexec/java_home -v11)"
        fi
      elif [[ "${OS_PLATFORM}" == *"Ubuntu"* ]]; then
        if [ -z "${JAVA_HOME}" ]; then
          echo "export JAVA_HOME=/usr/lib/jvm/default-java"
        fi
      else
        if [ -z "${JAVA_HOME}" ]; then
          echo "export OPENSSL_ROOT_DIR=${install_prefix}"
        fi
      fi
    fi
  } >> "${OUTPUT_ENV_FILE}"
  {
    if [[ "${for_analytical_java}" == true ]]; then
      if [[ "${OS_PLATFORM}" == *"Darwin"* ]]; then
        echo "export CC=${HOMEBREW_PREFIX}/opt/llvm/bin/clang"
        echo "export CXX=${HOMEBREW_PREFIX}/opt/llvm/bin/clang++"
        echo "export CARGO_TARGET_X86_64_APPLE_DARWIN_LINKER=${CC}"
        echo "export LDFLAGS=\"-L${HOMEBREW_PREFIX}/opt/llvm/lib\""
        echo "export CPPFLAGS=\"-I${HOMEBREW_PREFIX}/opt/llvm/include\""
      elif [[ "${OS_PLATFORM}" == *"CentOS"* || "${OS_PLATFORM}" == *"Aliyun"* ]]; then
        echo "source /opt/rh/llvm-toolset-7.0/enable || true"
        echo "export LIBCLANG_PATH=/opt/rh/llvm-toolset-7.0/root/usr/lib64/"
      fi
    fi
  } >> "${OUTPUT_ENV_FILE}"
}

install_deps() {
  init_workspace_and_env
  install_basic_packages
  [[ "${for_analytical}" == true ]] && install_analytical_dependencies
  [[ "${for_analytical_java}" == true ]] && install_analytical_java_dependencies
  [[ "${for_interactive}" == true ]] && install_interactive_dependencies
  [[ "${for_learning}" == true ]] && install_learning_dependencies
  write_env_config
  info "The script has installed all dependencies, don't forget to exec command:\n
  $ source ${OUTPUT_ENV_FILE}
  \nbefore building GraphScope."
}

install_deps
