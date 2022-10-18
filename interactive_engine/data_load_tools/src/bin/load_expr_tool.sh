#!/usr/bin/env bash
#
# A script to build experimental graph data by odps framework.

mode=$1
cur_dir=$(cd $(dirname $0); pwd)
artifacts_dir=/tmp/artifacts
build_lib_image=registry.cn-hongkong.aliyuncs.com/graphscope/lib-graph-store

prepare_artifacts() {
    odps=odpscmd

    mkdir -p ${artifacts_dir}
    echo "start to compile data_load_tool jar ..............."
    cd ${cur_dir}/../../.. && mvn clean package -DskipTests -Pjava-release
    # run in docker to generate .so which can match with the odps runtime env
    echo "start to compile graph_store library ..............."
    docker run -v $(cd ${cur_dir}/../../../.. && pwd):/home/GraphScope -it ${build_lib_image}:latest sh -c "source ~/.bashrc && cd /home/GraphScope/interactive_engine/executor/store/exp_store && cargo build --release"
    cp ${cur_dir}/../../target/data_load_tools-0.0.1-SNAPSHOT.jar ${artifacts_dir}/
    cp ${cur_dir}/../../../executor/store/target/release/libgraph_store.so ${artifacts_dir}/
    cp ${cur_dir}/../conf/config.ini ${artifacts_dir}/

    echo "start to upload artifacts to odps resources ..............."
    ${odps} -e "add file ${artifacts_dir}/libgraph_store.so -f;"
    ${odps} -e "add jar ${artifacts_dir}/data_load_tools-0.0.1-SNAPSHOT.jar -f;"
}

prepare_odps_table() {
    odps=odpscmd
    local_path=$1
    graph_name=$(sed '/^unique.name=/!d;s/.*=//' ${artifacts_dir}/config.ini)
    skip_header=$(sed '/^skip.header=/!d;s/.*=//' ${artifacts_dir}/config.ini)

    echo "start to load data into odps tables ..............."
    for file in ${local_path}/*
    do
        name="${file##*/}"
        type=${name%.*}
        table_name=${graph_name}_$type
        ${odps} -e "create table IF NOT EXISTS ${table_name}(str string);";
        ${odps} -e "tunnel upload -field-delimiter=U\0009 -header=${skip_header} -overwrite=true $file ${table_name}"
    done

    echo "start to create output tables for data encoding ..............."
    num=$(sed '/^encode.output.table.num=/!d;s/.*=//' ${artifacts_dir}/config.ini)
    for i in $(seq 0 $((num-1)))
    do
        vertex_table=${graph_name}_encode_vertex_$i
        edge_table=${graph_name}_encode_edge_$i
        ${odps} -e "create table IF NOT EXISTS ${vertex_table}(id1 bigint,id2 bigint,id3 bigint,id4 bigint,id5 bigint,bytes string,len bigint,code bigint);";
        ${odps} -e "create table IF NOT EXISTS ${edge_table}(id1 bigint,id2 bigint,id3 bigint,id4 bigint,id5 bigint,bytes string,len bigint,code bigint);";
    done
}

build_data() {
    odps=odpscmd

    echo "start to build graph data ..............."
    ${odps} -e "jar -resources data_load_tools-0.0.1-SNAPSHOT.jar,libgraph_store.so -classpath ${artifacts_dir}/data_load_tools-0.0.1-SNAPSHOT.jar com.alibaba.graphscope.dataload.IrDataBuild ${artifacts_dir}/config.ini;"
}

clean_data() {
    odps=odpscmd
    local_path=$1
    graph_name=$(sed '/^unique.name=/!d;s/.*=//' ${artifacts_dir}/config.ini)

    echo "start to clean odps tables ..............."
    for file in ${local_path}/*
    do
        name="${file##*/}"
        type=${name%.*}
        table_name=${graph_name}_$type
        ${odps} -e "drop table IF EXISTS ${table_name};"
    done

    echo "start to clean encoding output tables ..............."
    num=$(sed '/^encode.output.table.num=/!d;s/.*=//' ${artifacts_dir}/config.ini)
    for i in $(seq 0 $((num-1)))
    do
        vertex_table=${graph_name}_encode_vertex_$i
        edge_table=${graph_name}_encode_edge_$i
        ${odps} -e "drop table IF EXISTS ${vertex_table};";
        ${odps} -e "drop table IF EXISTS ${edge_table};";
    done

    rm -rf ${artifacts_dir}
}

download_data() {
    ossutil=$1
    download_path=$2

    graph_name=$(sed '/^unique.name=/!d;s/.*=//' ${artifacts_dir}/config.ini)
    partition_num=$(sed '/^write.graph.reducer.num=/!d;s/.*=//' ${artifacts_dir}/config.ini)
    oss_bucket_name=$(sed '/^oss.bucket.name=/!d;s/.*=//' ${artifacts_dir}/config.ini)

    # download graph_schema
    download_schema_data ${ossutil} ${download_path} ${oss_bucket_name} ${graph_name}

    # download graph_data_bin for each partition
    for i in $(seq 0 $((partition_num-1)))
    do
        download_partition_graph_data ${ossutil} ${download_path} ${oss_bucket_name} ${graph_name} ${partition_num} $i
    done
}

download_schema_data() {
    ossutil=$1
    download_path=$2
    oss_bucket_name=$3
    graph_name=$4

    oss_path_prefix=oss://${oss_bucket_name}/${graph_name}

    mkdir -p $download_path/graph_schema
    ${ossutil} cp ${oss_path_prefix}/graph_schema/schema.json $download_path/graph_schema/schema.json
}

download_partition_graph_data() {
    ossutil=$1
    download_path=$2
    oss_bucket_name=$3
    graph_name=$4
    partition_num=$5
    partition_id=$6

    partition_name=${graph_name}/${partition_num}
    oss_path_prefix=oss://${oss_bucket_name}/${partition_name}

    i=${partition_id}
    mkdir -p $download_path/graph_data_bin/partition_$i

    ${ossutil} cp ${oss_path_prefix}/graph_data_bin/partition_$i/edge_property $download_path/graph_data_bin/partition_$i/edge_property
    ${ossutil} cp ${oss_path_prefix}/graph_data_bin/partition_$i/graph_struct $download_path/graph_data_bin/partition_$i/graph_struct
    ${ossutil} cp ${oss_path_prefix}/graph_data_bin/partition_$i/index_data $download_path/graph_data_bin/partition_$i/index_data
    ${ossutil} cp ${oss_path_prefix}/graph_data_bin/partition_$i/node_property $download_path/graph_data_bin/partition_$i/node_property
}

if [ "$mode" = "prepare_artifacts" ]
then
    if $(! command -v odpscmd &> /dev/null); then
      echo "usage: PATH=\$PATH:<your odpscmd path> ./load_expr_tool.sh prepare_artifacts" && exit 1
    fi
    prepare_artifacts
elif [ "$mode" = "prepare_tables" ]
then
    shift 1
    if $(! command -v odpscmd &> /dev/null) || [[ "$#" -ne 1 ]]; then
      echo "usage: PATH=\$PATH:<your odpscmd path> ./load_expr_tool.sh prepare_tables <your csv data path>" && exit 1
    fi
    prepare_odps_table $1
elif [ "$mode" = "build_data" ]
then
    if $(! command -v odpscmd &> /dev/null); then
      echo "usage: PATH=\$PATH:<your odpscmd path> ./load_expr_tool.sh build_data" && exit 1
    fi
    build_data
elif [ "$mode" = "clean_data" ]
then
    shift 1
    if $(! command -v odpscmd &> /dev/null) || [[ "$#" -ne 1 ]]; then
      echo "usage: PATH=\$PATH:<your odpscmd path> ./load_expr_tool.sh clean_data <your local data path>" && exit 1
    fi
    clean_data $1
elif [ "$mode" = "get_data" ]
then
    shift 1
    ossutil=$1
    if [[ "$#" -ne 2 ]] || $(! command -v ${ossutil} &> /dev/null); then
      echo "usage: PATH=\$PATH:<your ossutil path> ./load_expr_tool.sh get_data <your ossutil binary name> <your local download path>" && exit 1
    fi
    download_data $1 $2
else
    echo "invalid mode"
fi
