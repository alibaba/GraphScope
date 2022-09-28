# 基于实验存储的数据编译

## Prerequisite
### csv数据
- 本地数据目录：
./ldbc_bi_sf1_raw
├── comment_0_0.csv
├── comment_hascreator_person_0_0.csv
├── comment_hastag_tag_0_0.csv
├── comment_islocatedin_place_0_0.csv
├── comment_replyof_comment_0_0.csv
├── comment_replyof_post_0_0.csv
├── forum_0_0.csv
├── forum_containerof_post_0_0.csv
├── forum_hasmember_person_0_0.csv
├── forum_hasmoderator_person_0_0.csv
├── forum_hastag_tag_0_0.csv
├── organisation_0_0.csv
├── organisation_islocatedin_place_0_0.csv
├── person_0_0.csv
├── person_hasinterest_tag_0_0.csv
├── person_islocatedin_place_0_0.csv
├── person_knows_person_0_0.csv
├── person_likes_comment_0_0.csv
├── person_likes_post_0_0.csv
├── person_studyat_organisation_0_0.csv
├── person_workat_organisation_0_0.csv
├── place_0_0.csv
├── place_ispartof_place_0_0.csv
├── post_0_0.csv
├── post_hascreator_person_0_0.csv
├── post_hastag_tag_0_0.csv
├── post_islocatedin_place_0_0.csv
├── tag_0_0.csv
├── tag_hastype_tagclass_0_0.csv
├── tagclass_0_0.csv
└── tagclass_issubclassof_tagclass_0_0.csv

- 每个csv的文件格式为标准的ldbc格式

### 工具
- [odpscmd](http://help.aliyun-inc.com/internaldoc/detail/413426.html?spm=a2c1f.8259796.2.36.lKgqX3)
- [ossutil](https://help.aliyun.com/document_detail/50451.html)

## Getting Started
### 准备artifacts
该过程会基于当前代码分支准备执行数据编译所需的artifacts: data_load.jar, graph_store.so, config.ini; 默认本地存放路径为/tmp/artifacts
```bash
cd GraphScope/interactive_engine/data_load_tools/src/bin
./load_expr_tool.sh prepare_artifacts <your odpscmd path>
```
也可以直接从oss下载
```bash
mkdir -p /tmp/artifacts
# download artifacts from oss path
${ossutil} cp oss://siyuan-transfer/expr_data_loading/artifacts.tar.gz artifacts.tar.gz
tar -xvzf ./artifacts.tar.gz -C /tmp
# upload artifacts to odps environment
${odpscmd} -e "add file /tmp/artifacts/libgraph_store.so -f;"
${odpscmd} -e "add jar /tmp/artifacts/data_load_tools-0.0.1-SNAPSHOT.jar -f;"
```
### 配置
```bash
skip.header=true
# 编译模式：ENCODE or WRITE_GRAPH
data.build.mode=ENCODE
# encode output tables数量
encode.output.table.num=1
# 最后生成的graph数据的partition数量
write.graph.reducer.num=1
# 用于标识最后生成的graph数据，format为graph_sf_partitions，i.e bi_sf1_1代表bi数据在sf1大小、分区数量为1下生成的graph数据
unique.name=bi_sf1
# 数据的schema文件 (默认配置为ldbc schema)
column.mapping.meta={...}
```
### 创建odps tables并load数据
该过程会创建odps编译所需的input tables，并将本地csv数据上传并导入到input tables中，作为ENCODE过程的输入数据；此外也会创建output tables，用于存放ENCODE输出的vertex/edge的结构数据；
```bash
./load_expr_tool.sh prepare_tables <your odpscmd path> <your csv data path>
```

### 构建数据
该过程首先从config.ini中解析出当前的执行阶段(ENCODE or WRITE_GRAPH)，并执行不同阶段的编译过程；ENCODE接受input tables作为输入，并将编译好的vertex/edge数据分别存放在不同的output tables中，作为后续WRITE_GRAPH的输入数据；WRITE_GRAPH将最后finalize的graph数据结构输出到oss path上，可供后续下载；
```bash
./load_expr_tool.sh build_data <your odpscmd path>
```

### 下载数据
该过程从oss path上下载graph数据到本地；
```bash
./load_expr_tool.sh get_data <your ossutil path> <your local download path>
```