echo "# this file is located in 'src/root_command.sh'"
echo "# you can edit it freely and regenerate (it will not be overwritten)"
inspect_args

if [ ${args[--app]} == "db" ]; then
  echo "reach db"
  package_name="db"
  comps=''
  eval "comps=(${args[components]})"
  for i in "${comps[@]}"; do
    package_name="${package_name}_${i}"
  done
  echo $package_name
  output_dir=`pwd`
  echo $output_dir
  build_dir=`mktemp -d`
  pushd $build_dir
  cmd="cmake -DCPACK_PACKAGE_NAME=${package_name} ${output_dir} && make -j && make package && mv ${package_name}*.deb ${output_dir}/"
  echo $cmd
  eval $cmd
  popd
else
  echo "not reach db"
fi

# TODO: parse args and make.
# echo "artifact: graphscope_flex_${args[--app]}_"+${$comps// /_}+".deb is built."  
