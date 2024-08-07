script_dir="$(cd "$(dirname "$0")" && pwd)"
source ${script_dir}/lib/get_os_version.sh
source ${script_dir}/lib/log.sh
source ${script_dir}/lib/colors.sh
source ${script_dir}/lib/install_thirdparty_dependencies.sh
source ${script_dir}/lib/install_vineyard.sh
source ${script_dir}/lib/util.sh
source ${script_dir}/initialize.sh


echo "# this file is located in 'src/make_image_command.sh'"
echo "# code for 'gs make-image' goes here"
echo "# you can edit it freely and regenerate (it will not be overwritten)"

#     allowed: [all, graphscope-dev, coordinator, analytical, analytical-java, interactive, interactive-frontend, interactive-executor, learning, vineyard-dev, vineyard-runtime]

while getopts ":c:r:t:" opt; do
  case $opt in
    c)
      component="$OPTARG"
      ;;
    r)
      registry="$OPTARG"
      ;;
    t)
      tag="$OPTARG"
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


log "Making image ${component}"

export INSTALL_PREFIX=${install_prefix}

make_all() {
    cd k8s
    make all
}

make_graphscope-dev() {
    cd k8s
    make graphscope-dev REGISTRY=${registry} VERSION=${tag}
}

make_analytical() {
    cd k8s
    make analytical REGISTRY=${registry} VERSION=${tag}
}

make_interactive() {
    cd k8s
    make interactive REGISTRY=${registry} VERSION=${tag}
}

make_interactive-frontend() {
    cd k8s
    make interactive-frontend REGISTRY=${registry} VERSION=${tag}
}

make_interactive-executor() {
    cd k8s
    make interactive-executor REGISTRY=${registry} VERSION=${tag}
}

make_learning() {
    cd k8s
    make learning REGISTRY=${registry} VERSION=${tag}
}


make_coordinator() {
    cd k8s
    make coordinator REGISTRY=${registry} VERSION=${tag}
}

make_vineyard-dev() {
    cd k8s
    make vineyard-dev REGISTRY=${registry} VERSION=${tag}
}

make_vineyard-runtime() {
    cd k8s
    make vineyard-runtime REGISTRY=${registry} VERSION=${tag}
}

make_${component}
