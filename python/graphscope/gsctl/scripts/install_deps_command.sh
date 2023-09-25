script_dir="$(cd "$(dirname "$0")" && pwd)"
source ${script_dir}/lib/get_os_version.sh
source ${script_dir}/lib/log.sh
source ${script_dir}/lib/colors.sh
source ${script_dir}/lib/install_thirdparty_dependencies.sh
source ${script_dir}/lib/install_vineyard.sh
source ${script_dir}/lib/util.sh
source ${script_dir}/initialize.sh

# parse args
while getopts ":t:i:d:v:j:-:" opt; do
  case ${opt} in
    t)
      type=${OPTARG}
      ;;
    i)
      install_prefix=${OPTARG}
      ;;
    d)
      deps_prefix=${OPTARG}
      ;;
    v)
      v6d_version=${OPTARG}
      ;;
    j)
      jobs=${OPTARG}
      ;;
    -)
      case ${OPTARG} in
        for-analytical)
          for_analytical=true
          ;;
        no-v6d)
          no_v6d=true
          ;;
        cn)
          cn=true
          ;;
        *)
          echo "Invalid option: --${OPTARG}"
          exit 1
          ;;
      esac
      ;;
    *)
      echo "Invalid option: -${OPTARG}"
      exit 1
      ;;
  esac
done
shift $((OPTIND -1))

SUDO=sudo
if [[ $(id -u) -eq 0 ]]; then
  warning "Please note that I am running as root."
  SUDO=""
fi

readonly OS=$(get_os_version)
readonly OS_PLATFORM=${OS%-*}
readonly OS_VERSION=${OS#*-}

readonly OUTPUT_ENV_FILE="${HOME}/.graphscope_env"

log "Installing ${type} dependencies for GraphScope on ${OS}..."

if [ "${cn}" == true ]; then
  log "Set to speed up downloading for CN locations."
  # export some mirror locations for CN, e.g., brew/docker...
  export HOMEBREW_BREW_GIT_REMOTE="https://mirrors.tuna.tsinghua.edu.cn/git/homebrew/brew.git"
  export HOMEBREW_CORE_GIT_REMOTE="https://mirrors.tuna.tsinghua.edu.cn/git/homebrew/homebrew-core.git"
  export HOMEBREW_BOTTLE_DOMAIN="https://mirrors.tuna.tsinghua.edu.cn/homebrew-bottles"
  export GRAPHSCOPE_DOWNLOAD_FROM_CN="true"
fi

check_os_compatibility() {
  if [[ "${OS_PLATFORM}" != *"Ubuntu"* && "${OS_PLATFORM}" != *"CentOS"* && "${OS_PLATFORM}" != *"Darwin"* && "${OS_PLATFORM}" != *"Aliyun"* ]];  then
    err "The script is only support platforms of Ubuntu/CentOS/macOS/AliyunOS"
    exit 1
  fi

  if [[ "${OS_PLATFORM}" == *"Ubuntu"* && "${OS_VERSION:0:2}" -lt "20" ]]; then
    err "The version of Ubuntu is ${OS_VERSION}. This script requires Ubuntu 20 or greater."
    exit 1
  fi

  if [[ "${OS_PLATFORM}" == *"CentOS"* && "${OS_VERSION}" -lt "7" ]]; then
    err "The version of CentOS is ${OS_VERSION}. This script requires CentOS 8 or greater."
    exit 1
  fi

  log "Running on ${OS_PLATFORM} ${OS_VERSION}"
}

BASIC_PACKAGES_LINUX=("file" "curl" "wget" "git" "sudo")

BASIC_PACKAGES_UBUNTU=("${BASIC_PACKAGES_LINUX[@]}" "build-essential" "cmake" "libunwind-dev" "python3-pip")

BASIC_PACKAGES_CENTOS_8=("${BASIC_PACKAGES_LINUX[@]}" "epel-release" "libunwind-devel" "libcurl-devel" "perl" "which")
BASIC_PACKAGES_CENTOS_7=("${BASIC_PACKAGES_CENTOS_8[@]}" "centos-release-scl-rh")
ADDITIONAL_PACKAGES_CENTOS_8=("gcc-c++" "python38-devel")
ADDITIONAL_PACKAGES_CENTOS_7=("make" "devtoolset-8-gcc-c++" "rh-python38-python-pip" "rh-python38-python-devel")

ANALYTICAL_UBUNTU=(
  "libboost-all-dev"
  "libopenmpi-dev"
  "libgflags-dev"
  "libgoogle-glog-dev"
  "libprotobuf-dev"
  "libgrpc++-dev"
  "libmsgpack-dev"
  "librdkafka-dev"
  "protobuf-compiler-grpc"
  "rapidjson-dev"
)

ANALYTICAL_CENTOS_7=("librdkafka-devel" "msgpack-devel" "rapidjson-devel")
ANALYTICAL_CENTOS_8=("${ANALYTICAL_CENTOS_7[@]}" "boost-devel" "gflags-devel" "glog-devel")

ANALYTICAL_MACOS=(
  "apache-arrow"
  "boost"
  "gflags"
  "glog"
  "open-mpi"
  "openssl@1.1"
  "protobuf"
  "grpc"
  "rapidjson"
  "msgpack-cxx"
  "librdkafka"
  "patchelf"
)

_install_apache_arrow_ubuntu() {
  if ! command -v dpkg -s libarrow-dev &>/dev/null; then
    log "Installing apache-arrow."
    ${SUDO} apt-get install -y lsb-release
    # shellcheck disable=SC2046,SC2019,SC2018
    wget -c https://apache.jfrog.io/artifactory/arrow/"$(lsb_release --id --short | tr 'A-Z' 'a-z')"/apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb \
      -P /tmp/
    ${SUDO} apt-get install -y -V /tmp/apache-arrow-apt-source-latest-"$(lsb_release --codename --short)".deb
    ${SUDO} apt-get update -y
    ${SUDO} apt-get install -y libarrow-dev
    rm /tmp/apache-arrow-apt-source-latest-*.deb
  else
    log "apache-arrow (libarrow-dev) already installed, skip."
  fi
}

_install_java_maven_ubuntu() {
  if ! command -v javac &>/dev/null; then
    log "Installing default-jdk"
    ${SUDO} apt-get install default-jdk -y
  fi
  if ! command -v mvn &>/dev/null; then
    log "Installing maven"
    ${SUDO} apt-get install maven -y
  fi
}

_install_java_maven_centos() {
  if ! command -v javac &>/dev/null; then
    log "Installing java-11-openjdk-devel"
    ${SUDO} yum install java-11-openjdk-devel -y
  fi
  if ! command -v mvn &>/dev/null; then
    log "Installing maven"
    install_maven "${deps_prefix}" "${install_prefix}"
  fi
}

_install_java_maven_macos() {
  if ! command -v javac &>/dev/null; then
    log "Installing openjdk@11"
    # we need arm64-base jvm, install from brew.
    brew install --ignore-dependencies openjdk@11
  fi
  if ! command -v mvn &>/dev/null; then
    log "Installing maven"
    brew install --ignore-dependencies maven
  fi
}

_install_dependencies_analytical_ubuntu() {
  DEBIAN_FRONTEND=noninteractive TZ=Etc/UTC ${SUDO} apt-get install -y ${ANALYTICAL_UBUNTU[*]}
  _install_apache_arrow_ubuntu
}

_install_dependencies_analytical_centos_common() {
  # the openssl must put before grpc, otherwise the grpc
  # cannot find the openssl.
  install_apache_arrow "${deps_prefix}" "${install_prefix}"
  install_open_mpi "${deps_prefix}" "${install_prefix}"
  install_protobuf "${deps_prefix}" "${install_prefix}"
  install_zlib "${deps_prefix}" "${install_prefix}"
  install_grpc "${deps_prefix}" "${install_prefix}"
}

_install_dependencies_analytical_centos8() {
  ${SUDO} yum install -y ${ANALYTICAL_CENTOS_8[*]}
  export LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:/lib/:/lib64${install_prefix}/lib:${install_prefix}/lib64
  install_cmake "${deps_prefix}" "${install_prefix}"
  install_openssl_static "${deps_prefix}" "${install_prefix}"
  _install_dependencies_analytical_centos_common
}
_install_dependencies_analytical_centos7() {
  ${SUDO} yum install -y ${ANALYTICAL_CENTOS_7[*]}
  source /opt/rh/devtoolset-8/enable
  source /opt/rh/rh-python38/enable
  export LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:${install_prefix}/lib:${install_prefix}/lib64

  install_cmake "${deps_prefix}" "${install_prefix}"
  install_gflags "${deps_prefix}" "${install_prefix}"
  install_glog "${deps_prefix}" "${install_prefix}"
  install_boost "${deps_prefix}" "${install_prefix}"
  install_openssl "${deps_prefix}" "${install_prefix}"
  _install_dependencies_analytical_centos_common
}
_install_dependencies_analytical_macos() {
  brew install ${ANALYTICAL_MACOS[*]}
  homebrew_prefix=$(brew --prefix)
  export LDFLAGS="${LDFLAGS} -L${homebrew_prefix}/opt/openssl@1.1/lib"
  export CPPFLAGS="${CPPFLAGS} -I${homebrew_prefix}/opt/openssl@1.1/include"
  export PKG_CONFIG_PATH="${homebrew_prefix}/opt/openssl@1.1/lib/pkgconfig"
}

install_basic_packages_universal() {
  if [[ "${OS_PLATFORM}" == *"Ubuntu"* ]]; then
    ${SUDO} apt-get update -y
    DEBIAN_FRONTEND=noninteractive TZ=Etc/UTC ${SUDO} apt-get install -y ${BASIC_PACKAGES_UBUNTU[*]}
  elif [[ "${OS_PLATFORM}" == *"CentOS"* || "${OS_PLATFORM}" == *"Aliyun"* ]]; then
    if [[ "${OS_VERSION}" -eq "7" ]]; then
      ${SUDO} yum install -y ${BASIC_PACKAGES_CENTOS_7[*]}
      ${SUDO} yum install -y ${ADDITIONAL_PACKAGES_CENTOS_7[*]}
    else
      if [[ "${OS_PLATFORM}" == *"Aliyun"* ]]; then 
        ${SUDO} yum install -y 'dnf-command(config-manager)'
        ${SUDO} dnf install -y epel-release --allowerasing
      else
        sed -i 's/mirrorlist/#mirrorlist/g' /etc/yum.repos.d/CentOS-*
        sed -i 's|#baseurl=http://mirror.centos.org|baseurl=http://vault.centos.org|g' /etc/yum.repos.d/CentOS-*
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

install_grape_vineyard_universal() {
  if [[ "${OS_PLATFORM}" == *"Darwin"* ]]; then
    brew install vineyard
  else
    log "Installing python packages for vineyard codegen."
    pip3 --no-cache-dir install pip -U --user
    pip3 --no-cache-dir install libclang wheel auditwheel --user
    install_vineyard "${deps_prefix}" "${install_prefix}" "${v6d_version}" "${jobs}"
  fi
}

install_rust_universal() {
  if ! command -v rustup &>/dev/null; then
    log "Installing rust with fixed version: 1.71/0."
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
    source $HOME/.cargo/env
    rustup install 1.71.0
    rustup default 1.71.0
    rustc --version
  fi
}

install_java_maven_universal() {
  if [[ "${OS_PLATFORM}" == *"Darwin"* ]]; then
    _install_java_maven_macos
  elif [[ "${OS_PLATFORM}" == *"Ubuntu"* ]]; then
    _install_java_maven_ubuntu
  else
    _install_java_maven_centos
  fi
}

install_llvm_universal() {
  if [[ "${OS_PLATFORM}" == *"Darwin"* ]]; then
    brew install llvm || true # prevent the `brew link` failure
    homebrew_prefix=$(brew --prefix)
    export CC=${homebrew_prefix}/opt/llvm/bin/clang
    export CXX=${homebrew_prefix}/opt/llvm/bin/clang++
    export CPPFLAGS="${CPPFLAGS} -I${homebrew_prefix}/opt/llvm/include"
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

install_dependencies_analytical_universal() {
  if [[ "${OS_PLATFORM}" == *"Darwin"* ]]; then
    _install_dependencies_analytical_macos
  elif [[ "${OS_PLATFORM}" == *"Ubuntu"* ]]; then
    install_patchelf "${deps_prefix}" "${install_prefix}"
    _install_dependencies_analytical_ubuntu
  else
    install_patchelf "${deps_prefix}" "${install_prefix}"
    if [[ "${OS_VERSION}" -eq "7" ]]; then
      _install_dependencies_analytical_centos7
      install_java_maven_universal
    else
      _install_dependencies_analytical_centos8
    fi
  fi
}

write_env_config() {
  log "Output environments config file ${OUTPUT_ENV_FILE}"
  if [ -f "${OUTPUT_ENV_FILE}" ]; then
    warning "Found ${OUTPUT_ENV_FILE} exists, remove the environment config file and generate a new one."
    rm -f "${OUTPUT_ENV_FILE}"
  fi

  {
    echo "export GRAPHSCOPE_HOME=${install_prefix}"
    echo "export CMAKE_PREFIX_PATH=/opt/vineyard"
    echo "export PATH=${install_prefix}/bin:\$HOME/.cargo/bin:\$PATH"
    echo "export LD_LIBRARY_PATH=${LD_LIBRARY_PATH}"
    echo "export LIBRARY_PATH=${install_prefix}/lib:${install_prefix}/lib64"
  } >>"${OUTPUT_ENV_FILE}"

  if [[ "${OS_PLATFORM}" == *"Darwin"* ]]; then
    homebrew_prefix=$(brew --prefix)
    {
      echo "export CC=${homebrew_prefix}/opt/llvm/bin/clang"
      echo "export CXX=${homebrew_prefix}/opt/llvm/bin/clang++"
      echo "export CARGO_TARGET_X86_64_APPLE_DARWIN_LINKER=${CC}"
      if [ -z "${JAVA_HOME}" ]; then
        echo "export JAVA_HOME=\$(/usr/libexec/java_home -v11)"
      fi
      echo "export OPENSSL_ROOT_DIR=${homebrew_prefix}/opt/openssl"
      echo "export OPENSSL_LIBRARIES=${homebrew_prefix}/opt/openssl/lib"
      echo "export OPENSSL_SSL_LIBRARY=${homebrew_prefix}/opt/openssl/lib/libssl.dylib"
      echo "export LDFLAGS=\"-L${homebrew_prefix}/opt/llvm/lib\""
      echo "export CPPFLAGS=\"-I${homebrew_prefix}/opt/llvm/include\""
    } >>"${OUTPUT_ENV_FILE}"

  elif [[ "${OS_PLATFORM}" == *"Ubuntu"* ]]; then
    {
      if [ -z "${JAVA_HOME}" ]; then
        echo "export JAVA_HOME=/usr/lib/jvm/default-java"
      fi
    } >>"${OUTPUT_ENV_FILE}"
  else
    {
      if [[ "${OS_VERSION}" -eq "7" ]]; then
        echo "source /opt/rh/devtoolset-8/enable"
        echo "source /opt/rh/rh-python38/enable"
        echo "source /opt/rh/llvm-toolset-7.0/enable || true"
        echo "export LIBCLANG_PATH=/opt/rh/llvm-toolset-7.0/root/usr/lib64/"
      fi
      if [ -z "${JAVA_HOME}" ]; then
        echo "export JAVA_HOME=/usr/lib/jvm/jre-openjdk"
      fi
      echo "export OPENSSL_ROOT_DIR=${install_prefix}"
    } >>"${OUTPUT_ENV_FILE}"
  fi
}

init_workspace_and_env() {
  mkdir -p "${install_prefix}"
  mkdir -p "${deps_prefix}"
  export PATH=${install_prefix}/bin:${PATH}
  export LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:${install_prefix}/lib:${install_prefix}/lib64
}

install_deps_for_dev() {
  # install_deps for development on local
  check_os_compatibility

  init_workspace_and_env

  if [[ "${OS_PLATFORM}" == *"Darwin"* ]]; then
    export HOMEBREW_NO_INSTALL_CLEANUP=1
    export HOMEBREW_NO_INSTALLED_DEPENDENTS_CHECK=1
  fi

  install_basic_packages_universal
  if [ "${for_analytical}" == true ]; then
    install_dependencies_analytical_universal
    if [ "${no_v6d}" != true ]; then
      install_grape_vineyard_universal
    fi
  else # for all
    install_dependencies_analytical_universal
    if [ "${no_v6d}" != true ]; then
      install_grape_vineyard_universal
    fi
    install_java_maven_universal
    install_llvm_universal
    install_rust_universal
    install_cppkafka "${deps_prefix}" "${install_prefix}"
  fi

  write_env_config

  succ "The script has installed all dependencies for building GraphScope, use commands:\n
  $ source ${OUTPUT_ENV_FILE}
  $ make install
  \nto build and develop GraphScope."
}

install_deps_for_client() {
  # install python..
  # TODO: refine
  pip3 --no-cache-dir install -U pip --user
  pip3 --no-cache-dir install auditwheel daemons etcd-distro gremlinpython \
    hdfs3 fsspec oss2 s3fs ipython kubernetes libclang networkx==2.4 numpy pandas parsec pycryptodome \
    pyorc pytest scipy scikit_learn wheel --user
  pip3 --no-cache-dir install Cython --pre -U --user
}

# run subcommand with the type
install_deps_for_"${type}"
