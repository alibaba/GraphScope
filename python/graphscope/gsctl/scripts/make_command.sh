script_dir="$(cd "$(dirname "$0")" && pwd)"
source ${script_dir}/lib/get_os_version.sh
source ${script_dir}/lib/log.sh
source ${script_dir}/lib/colors.sh
source ${script_dir}/lib/install_thirdparty_dependencies.sh
source ${script_dir}/lib/install_vineyard.sh
source ${script_dir}/lib/util.sh
source ${script_dir}/initialize.sh


echo "# this file is located in 'src/make_command.sh'"
echo "# code for 'gs make' goes here"
echo "# you can edit it freely and regenerate (it will not be overwritten)"

while getopts ":c:i:s:" opt; do
  case $opt in
    c)
      component="$OPTARG"
      ;;
    i)
      install_prefix="$OPTARG"
      ;;
    s)
      storage_type="$OPTARG"
      ;;
    \?)
      echo "Invalid options: -$OPTARG" >&2
      exit 1
      ;;
    :)
      echo "The option -$OPTARG requires parameters" >&2
      exit 1
      ;;
     esac
done


log "Making component ${component}"


GS_SOURCE_DIR="$(dirname -- "$(readlink -f "${BASH_SOURCE}")")"

echo "dir $GS_SOURCE_DIR"


export INSTALL_PREFIX=${install_prefix}

make_all() {
    make all
}

make_install() {
    make install
}

make_analytical() {
    make analytical
}

make_interactive() {
    if [[ ${storage_type} = "experimental" ]]; then
        cd "${GS_SOURCE_DIR}"/interactive_engine/compiler && make build QUIET_OPT=""
    elif [[ ${storage_type} = "vineyard" ]]; then
        cd "${GS_SOURCE_DIR}"/interactive_engine && mvn install -DskipTests -Drust.compile.mode=release -P graphscope,graphscope-assembly
        cd "${GS_SOURCE_DIR}"/interactive_engine/assembly/target && tar xvzf graphscope.tar.gz
    else
        make interactive
    fi
}

make_learning() {
    make learning
}

make_analytical-install() {
    make analytical-install INSTALL_PREFIX=${install_prefix}
}

make_interactive-install() {
    make interactive-install INSTALL_PREFIX=${install_prefix}
}

make_learning-install() {
    make learning-install INSTALL_PREFIX=${install_prefix}
}

make_client() {
    make client
}

make_coordinator() {
    make coordinator
}

make_analytical-java() {
    make analytical-java
}

make_analytical-java-install() {
    make analytical-java-install INSTALL_PREFIX=${install_prefix}
}

make_clean() {
    make clean
}

make_${component}
