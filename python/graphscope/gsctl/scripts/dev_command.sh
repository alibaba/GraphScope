script_dir="$(cd "$(dirname "$0")" && pwd)"
source ${script_dir}/lib/get_os_version.sh
source ${script_dir}/lib/log.sh
source ${script_dir}/lib/colors.sh
source ${script_dir}/lib/install_thirdparty_dependencies.sh
source ${script_dir}/lib/install_vineyard.sh
source ${script_dir}/lib/util.sh


while getopts ":l:" opt; do
  case $opt in
    l)
      local="$OPTARG"
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


mount_option=""

if [[ -n $local ]]; then
	echo "Opened a new container with $local mounted to /home/graphscope/graphscope."
	mount_option="--mount type=bind,source=${local},target=/home/graphscope/graphscope"
else
	echo "No local directory assigned, open a new container without mounting local directory."
fi

# docker pull graphscope/graphscope-dev
REGISTRY="registry.cn-hongkong.aliyuncs.com"
docker run \
	-it \
	${mount_option} \
	${REGISTRY}/graphscope/graphscope-dev:latest
