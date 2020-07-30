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

set -x

cargo test --release --no-run
sudo perf record -g --call-graph dwarf -- target/release/examples/bouned_no_lock_single_worker -m 1500 -b 10000
sudo perf script > out.perf
~/FlameGraph-master/stackcollapse-perf.pl out.perf > out.fold
~/FlameGraph-master/flamegraph.pl out.fold  > x.svg

hostname -i

python -m SimpleHTTPServer

