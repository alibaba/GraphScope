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

scriptName="build.sh"
options="all"

# Print usage
usage() {
  echo "${scriptName} [OPTION]...
 To build the utilities for GAIA.
 ${bold}Options:${reset}
  -o, --options    Options for building GAIA.
                   Available options: graph_loader, start_rpc, gremlin_server, all
  -h, --help       Display this help and exit.
"
}

# Read the options and set stuff
while [[ $1 = -?* ]]; do
  case $1 in
    -h|--help) usage >&2; exit ;;
    -o|--options) shift; options=${1} ;;
    *) usage >&2; exit ;;
  esac
  shift
done

echo "Build GAIA utilities for: ${options}"

if [ "${options}" != "all"  ] && [ "${options}" != "graph_loader"  ]  \
  && [ "${options}" != "start_rpc"  ] && [ "${options}" != "gremlin_server" ]
then
    echo "Invalid options: ${options}"
    usage >&2;
    exit;
fi

if [ "${options}" = "all"  ] || [ "${options}" = "graph_loader"  ]
then
  # Build graph storage
  cd ../../../graph_store
  echo "Build tools for graph storage..."
  cargo build --release --bin downloader
  cargo build --release --bin par_loader
  cargo build --release --bin simple_loader
  cp target/release/downloader ../query_service/gremlin/scripts/bin
  cp target/release/par_loader ../query_service/gremlin/scripts/bin
  cp target/release/simple_loader ../query_service/gremlin/scripts/bin
  cd ../query_service/gremlin/scripts
fi

if [ "${options}" = "all"  ] || [ "${options}" = "start_rpc"  ]
then
  echo "Build tools for starting RPC server..."
  # Build RPC server for Gremlin
  cd ../gremlin_core
  cargo build --release --bin start_rpc_server
  cp target/release/start_rpc_server ../scripts/bin
  cd ../scripts
fi

if [ "${options}" = "all"  ] || [ "${options}" = "gremlin_server"  ]
then
  echo "Build Gremlin compiler ..."
  # Build Gremlin compiler
  cd ../compiler
  mvn clean package -DskipTests
  cp gremlin-server-plugin/target/gremlin-server-plugin-1.0-SNAPSHOT-jar-with-dependencies.jar ../scripts/bin
  cd ../scripts
fi
