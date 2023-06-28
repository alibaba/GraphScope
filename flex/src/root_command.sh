# echo "# this file is located in 'src/root_command.sh'"
# echo "# you can edit it freely and regenerate (it will not be overwritten)"
# inspect_args

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

output_dir=`realpath ${args[--output_dir]}`
mkdir -p ${output_dir}

if [ ${args[--app]} == "db" ]; then
  package_name="graphscope_flex_${args[--app]}"
  comps=''
  eval "comps=(${args[components]})"
  for i in "${comps[@]}"; do
    package_name="${package_name}_${i}"
  done
  build_dir=`mktemp -d`
  pushd $build_dir > /dev/null
  cmd="cmake -DCPACK_PACKAGE_NAME=${package_name} ${SCRIPT_DIR} && make -j && make package && mv ${package_name}*.deb ${output_dir}/"
  echo $cmd
  eval $cmd
  popd > /dev/null
else
  echo "unexpected app type"
fi

# TODO: parse args and make.
# echo "artifact: graphscope_flex_${args[--app]}_"+${$comps// /_}+".deb is built."  
