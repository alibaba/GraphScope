script_dir="$(cd "$(dirname "$0")" && pwd)"
source ${script_dir}/lib/get_os_version.sh
source ${script_dir}/lib/log.sh
source ${script_dir}/lib/colors.sh
source ${script_dir}/lib/install_thirdparty_dependencies.sh
source ${script_dir}/lib/install_vineyard.sh
source ${script_dir}/lib/util.sh


echo "# this file is located in 'src/spark_classpath_command.sh'"
echo "# code for 'gs spark-classpath' goes here"
echo "# you can edit it freely and regenerate (it will not be overwritten)"

# TODO(zhanglei): generate a file named ~/.graphscope_4spark.env

# check if the file exists
# if yes, raise a warning
# otherwise, generate a new

echo "$(green_bold "Generated environment variables for spark jobs in ~/.graphscope_4spark.env")"
echo
echo "export the env to your bash terminal by running: source ~/.graphscope_4spark.env"
