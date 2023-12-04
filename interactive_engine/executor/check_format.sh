#!/bin/bash
# Copyright 2022 Alibaba Group Holding Limited.
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

CURRENT=$(pwd)
if ! rustup toolchain list | grep -q nightly; then
  echo 'Installing nightly toolchain...'
  rustup toolchain install nightly
  rustup component add rustfmt --toolchain nightly
fi

directories=(
  "assembly/groot"
  "assembly/v6d"
  "common/dyn_type"
  "engine/pegasus"
  "ir"
  "store"
)

CURRENT=$(pwd)

for dir in "${directories[@]}"; do
  cd "${CURRENT}/${dir}"
  cargo +nightly fmt -- --check
  #cargo +nightly fmt
done
