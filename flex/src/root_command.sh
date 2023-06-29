# echo "# this file is located in 'src/root_command.sh'"
# echo "# you can edit it freely and regenerate (it will not be overwritten)"
# inspect_args

function build_grape_cpu {
  target_name=$1

  tmp_dir=`mktemp -d`
  pushd ${tmp_dir} > /dev/null
  cmake_options=""
  # if [ "$(expr substr $(uname -s) 1 5)" == "Linux" ]; then
  if [[ $(uname -s) == Linux* ]];then
    # Do something under GNU/Linux platform
    if grep -q avx512 /proc/cpuinfo; then
      cmake_options="${cmake_options} -DUSE_SIMD=ON"
    fi
    if grep -q HugePages_Total /proc/meminfo; then
      hpn=`grep HugePages_Total /proc/meminfo | cut -d ':' -f 2`
      if [[ $hpn -gt 0 ]]; then
        cmake_options="${cmake_options} -DUSE_HUGEPAGES=ON"
      fi
    fi
  fi
  cmd="git clone https://github.com/alibaba/libgrape-lite.git && cd libgrape-lite && git submodule update --init --recursive && mkdir build && cd build && cmake ${cmake_options} .. && make analytical_apps -j && mv run_app ${target_name}"
  echo $cmd
  eval $cmd
  popd > /dev/null
}

function build_grape_gpu {
  target_name=$1
  if nvidia-smi &> /dev/null; then
    tmp_dir=`mktemp -d`
    pushd ${tmp_dir} > /dev/null
    cmd="git clone https://github.com/alibaba/libgrape-lite.git && cd libgrape-lite && git submodule update --init --recursive && mkdir build && cd build && cmake .. && make gpu_analytical_apps -j && mv run_cuda_app ${target_name}"
    echo $cmd
    eval $cmd
    popd > /dev/null
  else
    echo "Building libgrape-gpu failed: GPU is not found"
  fi
}

function build_grape_ldbc_driver {
  package_name=$1

  tmp_dir=`mktemp -d`
  pushd ${tmp_dir} > /dev/null
  cmd="git clone https://github.com/alibaba/libgrape-lite.git && cd libgrape-lite/ldbc_driver/ && mvn package && mv graphalytics-*.bin.tar.gz ${package_name}"
  echo $cmd
  eval $cmd
  popd > /dev/null
}

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

output_dir=`realpath ${args[--output_dir]}`
mkdir -p ${output_dir}

if [ ${args[--app]} == "db" ]; then
  package_name="graphscope_flex_${args[--app]}"
  comps=""
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
elif [ ${args[--app]} == "olap" ]; then
  target_name="graphscope_flex_${args[--app]}"
  comps=""
  eval "comps=(${args[components]})"
  for i in "${comps[@]}"; do
    target_name="${target_name}_${i}"
  done
  if [[ " ${comps[*]} " =~ " grape-cpu " ]]; then
    build_grape_cpu ${output_dir}/${target_name}
  elif [[ " ${comps[*]} " =~ " grape-gpu " ]]; then
    build_grape_gpu ${output_dir}/${target_name}
  fi
elif [ ${args[--app]} == "ldbcdriver" ]; then
  target_name="graphscope_flex_olap"
  comps=""
  eval "comps=(${args[components]})"
  for i in "${comps[@]}"; do
    target_name="${target_name}_${i}"
  done
  if [[ " ${comps[*]} " =~ " grape-cpu " ]]; then
    build_grape_cpu ${output_dir}/${target_name}
  elif [[ " ${comps[*]} " =~ " grape-gpu " ]]; then
    build_grape_gpu ${output_dir}/${target_name}
  fi

  package_name="graphscope_flex_ldbcdriver"
  for i in "${comps[@]}"; do
    package_name="${package_name}_${i}"
  done
  package_name="${package_name}-SNAPSHOT-bin.tar.gz"

  build_grape_ldbc_driver ${output_dir}/${package_name}
fi

# TODO: parse args and make.
# echo "artifact: graphscope_flex_${args[--app]}_"+${$comps// /_}+".deb is built."  
