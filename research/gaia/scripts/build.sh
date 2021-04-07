#!/bin/sh
# Copyright 2020 Alibaba Group Holding Limited.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Create the binary folder (if not exist)
mkdir -p bin

# Build graph storage
cd ../graph_store
echo "Build tools for graph storage..."
cargo build --release --bin download_raw
cargo build --release --bin build_store
cp target/release/download_raw ../scripts/bin
cp target/release/build_store ../scripts/bin

echo "Build tools for starting RPC server..."
# Build RPC server for Gremlin
cd ../gremlin/gremlin_core
cargo build --release --bin start_rpc_server
cp target/release/start_rpc_server ../../scripts/bin

echo "Build Gremlin compiler ..."
# Build Gremlin compiler
cd ../compiler
mvn clean package -DskipTests
cp gremlin-server-plugin/target/gremlin-server-plugin-1.0-SNAPSHOT-jar-with-dependencies.jar ../../scripts/bin
