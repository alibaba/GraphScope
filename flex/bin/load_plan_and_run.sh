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

cypher_to_plan(){
    if [ $# -ne 2 ]; then
        echo "Usage: $0 input_file output_file, but receive: "$#
        exit 1
    fi
    # check GIE_HOME set
    if [ -z ${GIE_HOME} ]; then
        echo "GIE_HOME not set."
        exit 1
    fi
    input_path=$1
    output_path=$2
    # find java executable
    JAVA_EXECUTABLE=$(which java)
    if [ -z ${JAVA_EXECUTABLE} ]; then
        # try find from JAVA_HOME
        if [ -z ${JAVA_HOME} ]; then
            echo "JAVA_HOME not set."
            exit 1
        else
            JAVA_EXECUTABLE=${JAVA_HOME}/bin/java
        fi
        exit 1
    fi
    echo "Java executable = ${JAVA_EXECUTABLE}"
    echo "---------------------------"
    echo "Find compiler exists"
    # read from file ${input_path}
    cypher_query=$(cat ${input_path})
    echo "Find cypher query:"
    echo "---------------------------"
    echo ${cypher_query}
    echo "---------------------------"

    #get abs path of input_path
    real_input_path=$(realpath ${input_path})
    real_output_path=$(realpath ${output_path})

    pushd ${GIE_HOME}"/compiler/"
    compiler_jar=${GIE_HOME}"/compiler/target/compiler-0.0.1-SNAPSHOT.jar"
    #check exists
    if [ ! -f ${compiler_jar} ]; then
        echo "Compiler jar = ${compiler_jar} not exists."
        echo "Fail to find compiler jar."
        exit 1
    fi
    cmd="make physical_plan query='${real_input_path}' physical='${real_output_path}'"
    echo "running physical plan genration with "${cmd}
    eval ${cmd}
    popd

    echo "---------------------------"
    #check output 
    if [ ! -f ${output_path} ]; then
        echo "Output file = ${output_path} not exists."
        echo "Fail to generate physical plan."
        exit 1
    fi
}


compile_so(){
    #check input params size eq 2 or 3
    if [ $# -ne 2 ] && [ $# -ne 3 ]; then
        echo "Usage: $0 input_file work_dir [output_dir]"
        exit 1
    fi
    input_path=$1
    work_dir=$2
    if [ $# -eq 3 ]; then
        output_dir=$3
    fi
    echo "Input path = ${input_path}"
    echo "Work dir = ${work_dir}"
    echo "Output dir = ${output_dir}"

    last_file_name=$(basename ${input_path})

    echo "last file name: "${last_file_name}
    # requiest last_file_name suffix is .pb
    if [[ $last_file_name == *.pb ]]; then
        query_name="${last_file_name%.pb}"
        echo "File has .pb suffix."
    elif [[ $last_file_name == *.cc ]]; then
        echo "File havs .cc suffix."
        query_name="${last_file_name%.cc}"
    elif [[ $last_file_name == *.cypher ]];
    then
        echo "File has .cypher suffix."
        query_name="${last_file_name%.cypher}"
    else 
        echo "Expect a .pb or .cc file"
        exit 1
    fi
    cur_dir=${work_dir}"/"
    mkdir -p ${cur_dir}
    output_cc_path=${cur_dir}"/"${query_name}".cc"
    if [[ "$(uname)" == "Linux" ]]; then
        output_so_path=${cur_dir}"/lib"${query_name}".so"
        dst_so_path=${output_dir}"/lib"${query_name}".so"
    elif [[ "$(uname)" == "Darwin" ]]; then
        output_so_path=${cur_dir}"/lib"${query_name}".dylib"
        dst_so_path=${output_dir}"/lib"${query_name}".dylib"
    else 
        echo "Not support OS."
        exit 1;
    fi

    #only do codegen when receives a .pb file.
    if [[ $last_file_name == *.pb ]]; then
        cmd="${CODEGEN_RUNNER} ${input_path} ${output_cc_path}"
        echo "Codegen command = ${cmd}"
        eval ${cmd}
        echo "----------------------------"
    elif [[ $last_file_name == *.cypher ]]; then
        echo "Generating code from cypher query"
        # first do .cypher to .pb
        output_pb_path=${cur_dir}"/"${query_name}".pb"
        cypher_to_plan ${input_path} ${output_pb_path}
        echo "----------------------------"
        echo "Codegen from cypher query done."
        echo "----------------------------"
        cmd="${CODEGEN_RUNNER} ${output_pb_path} ${output_cc_path}"
        echo "Codegen command = ${cmd}"
        eval ${cmd}
        # then. do .pb to .cc
    elif [[ $last_file_name == *.cc ]]; then
	    cp $input_path ${output_cc_path}
    fi
    echo "Start running cmake and make"
    #check output_cc_path exists
    if [ ! -f ${output_cc_path} ]; then
        echo "Codegen failed, "${output_cc_path}" not exists."
        exit 1
    fi

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

    ################### now copy ##########################
    # copy output to output_dir
    if [ ! -z ${output_dir} ]; then
        mkdir -p ${output_dir}
    else
        echo "Output dir not set, skip copying."
        exit 0
    fi
    # check output_dir doesn't contains output_so_name
    if [ -f ${dst_so_path} ]; then
        echo "Output dir "${output_dir}" already contains "${query_name}".so"
        echo "Please remove it first."
        exit 1
    fi
    cp ${output_so_path} ${output_dir}
    #check dst_so_path exists
    if [ ! -f ${dst_so_path} ]; then
        echo "Copy failed, "${dst_so_path}" not exists."
        exit 1
    fi
    echo "Finish copying, output to ${dst_so_path}"
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
        -w=*|--work_dir=*)
        WORK_DIR="${i#*=}"
        shift # past argument=value
        ;;
        -o=*|--output_dir=*)
        OUTPUT_DIR="${i#*=}"
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
    echo "Work dir     ="${WORK_DIR}
    echo "Output path  ="${OUTPUT_DIR}

    # check input exist
    if [ ! -f ${INPUT} ]; then
        echo "Input file "${INPUT}" not exists."
        exit 1
    fi

    # compile the input file to a .so file, put in $OUTPUT_PATH.
    compile_so ${INPUT} ${WORK_DIR} ${OUTPUT_DIR}
    #if output dir is specified, we will copy to output dir.
}

if [ $# -lt 2 ]; then
    echo "Usage: $0 input_file work_dir output_dir"
    echo "Example: $0 -i=../query/1.pb -w=. -o=."
    echo "your num args: "$#
    exit 1
fi

run "$@"
