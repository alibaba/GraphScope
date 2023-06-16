#!/bin/bash

set -e

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
FLEX_HOME=${SCRIPT_DIR}/../
echo "FLEX_HOME root =         ${FLEX_HOME}"
FLEX_INCLUDE=${FLEX_HOME}/include/flex/
echo "FLEX_INCLUDE directory = ${CODE_GEN_ROOT}"

# first try to resolve as if we are installed. the try to resolve locally.

if [ ! -d ${FLEX_INCLUDE} ]; then
    echo "try FLEX_HOME"${FLEX_INCLUDE}" not exists."
    echo "try to resolve locally"
else 
    echo "try FLEX_HOME"${FLEX_INCLUDE}" exists."
    CODEGEN_RUNNER=${FLEX_HOME}/bin/gen_query_from_plan
    CMAKE_TEMPLATE_PATH=${FLEX_HOME}/include/flex/codegen/template/CMakeLists.txt.template
fi

FLEX_INCLUDE=${FLEX_HOME}/../
echo "try to find flex with FLEX_INCLUDE directory = ${FLEX_INCLUDE}"
if [ ! -d ${FLEX_INCLUDE} ]; then
    echo "FLEX_INCLUDE directory = ${FLEX_INCLUDE} not exists."
    echo "Fail to find flex."
    exit 1
else
    CODEGEN_RUNNER=${FLEX_HOME}/build/codegen/gen_query_from_plan
    CMAKE_TEMPLATE_PATH=${FLEX_HOME}/codegen/template/CMakeLists.txt.template
fi

echo "Codegen runner = ${CODEGEN_RUNNER}"
echo "Cmake template path = ${CMAKE_TEMPLATE_PATH}"
#check these files exist
if [ ! -f ${CODEGEN_RUNNER} ]; then
    echo "Codegen runner = ${CODEGEN_RUNNER} not exists."
    echo "Fail to find codegen_runner."
    exit 1
fi

if [ ! -f ${CMAKE_TEMPLATE_PATH} ]; then
    echo "Cmake template path = ${CMAKE_TEMPLATE_PATH} not exists."
    echo "Fail to find CMakeLists.txt.template."
    exit 1
fi


compile_so(){
    #check input params size == 2
    if [ $# -ne 2 ]; then
        echo "Usage: $0 input_file output_dir, but receive: "$#
        exit 1
    fi
    input_path=$1
    output_dir=$2
    echo ${input_path}", "${output_dir}

    last_file_name=$(basename ${input_path})

    echo "last file name: "${last_file_name}
    # requiest last_file_name suffix is .pb
    if [[ $last_file_name == *.pb ]]; then
        query_name="${last_file_name%.pb}"
        echo "File has .pb suffix."
    elif [[ $last_file_name == *.cc ]]; then
        echo "File havs .cc suffix."
        query_name="${last_file_name%.cc}"
    else 
        echo "Expect a .pb or .cc file"
        exit 1
    fi
    cur_dir=${output_dir}"/"
    mkdir -p ${cur_dir}
    output_cc_name=${cur_dir}"/"${query_name}".cc"
    if [[ "$(uname)" == "Linux" ]]; then
        output_so_path=${cur_dir}"/lib"${query_name}".so"
    elif [[ "$(uname)" == "Darwin" ]]; then
        output_so_path=${cur_dir}"/lib"${query_name}".dylib"
    else 
        echo "Not support OS."
        exit 1;
    fi

    #only do codegen when receives a .pb file.
    if [[ $last_file_name == *.pb ]]; then
        cmd="${CODEGEN_RUNNER} ${input_path} ${output_cc_name}"
        echo "Codegen command = ${cmd}"
        eval ${cmd}
        echo "----------------------------"
    elif [[ $last_file_name == *.cc ]]; then
	    cp $input_path ${output_cc_name}
    fi
    echo "Start running cmake and make"

    # copy cmakelist.txt to output path.
    cp ${CMAKE_TEMPLATE_PATH} ${cur_dir}/CMakeLists.txt
    # run cmake and make in output path.
    pushd ${cur_dir}
    cmd="cmake . -DQUERY_NAME=${query_name} -DFLEX_INCLUDE_PREFIX=${FLEX_INCLUDE}"
    # if CMAKE_CXX_COMPILER is set, use it.
    if [ ! -z ${CMAKE_CXX_COMPILER} ]; then
        cmd=${cmd}" -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}"
    fi
    # if CMAKE_C_COMPILER is set, use it.
    if [ ! -z ${CMAKE_C_COMPILER} ]; then
        cmd=${cmd}" -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}"
    fi
    echo "Cmake command = ${cmd}"
    echo "---------------------------"
    eval ${cmd}

    ################### now build ##########################
    # get number of cores
    #n_cores=`nproc`
    n_cores=1
    cmd="make -j"${n_cores}
    echo "Make command = ${cmd}"
    eval ${cmd}
    echo "---------------------------"
    #check if build success
    if [ $? -ne 0 ]; then
        echo "Build failed."
        exit 1
    fi
    # check output_so_name exists
    if [ ! -f ${output_so_path} ]; then
        echo "Build failed, "${output_so_path}" not exists."
        exit 1
    fi
    echo "Finish building, output to "${output_so_path}
    popd 
}


# input path
# output dir
run() {
    for i in "$@"; do
    case $i in
        -i=*|--input=*)
        INPUT="${i#*=}"
        shift # past argument=value
        ;;
        -o=*|--output_path=*)
        OUTPUT_PATH="${i#*=}"
        shift # past argument=value
        ;;
        -*|--*)
        echo "Unknown option $i"
        exit 1
        ;;
        *)
        ;;
    esac
    done

    echo "Input        ="${INPUT}
    echo "Output path  ="${OUTPUT_PATH}
    # compile the input file to a .so file, put in $OUTPUT_PATH.
    compile_so ${INPUT} ${OUTPUT_PATH}
}

if [ $# -lt 2 ]; then
    echo "Usage: $0 input_file output_dir"
    exit 1
fi

run "$@"
