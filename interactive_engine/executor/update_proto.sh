#!/bin/sh
# Copyright 2020 Alibaba Group Holding Limited.
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#     http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

echo "Deprecated!! "
echo "Just run 'cargo build --all' which will generate proto files."
exit 1

source_root="`dirname "$0"`"
source_root=`cd ${source_root}/../; pwd`

proto_file_dir=`cd ${source_root}/common/proto; pwd`
output_path=`cd ${source_root}/rust/common/src/proto; pwd`

function generate() {
    input_file=$1
    echo "[Generating] file: ${input_file}"

    protoc --rust_out=${output_path} \
           --grpc_out=${output_path} \
           --plugin=protoc-gen-grpc=`which grpc_rust_plugin` \
           --proto_path=${proto_file_dir} ${input_file}

    if [[ $? -ne 0 ]]; then
        echo "Generate proto failed"
        exit 1
    fi
}

for f in `find ${proto_file_dir} -name "*.proto"`; do
    generate ${f}
done

echo "Done!"
