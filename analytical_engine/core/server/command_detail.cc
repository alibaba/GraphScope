/** Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "core/server/command_detail.h"

#include <string>

namespace gs {

grape::InArchive& operator<<(grape::InArchive& archive,
                             const CommandDetail& cd) {
  std::map<int, std::string> buffer;
  for (auto& pair : cd.params) {
    buffer[pair.first] = pair.second.SerializeAsString();
  }

  archive << cd.type;
  archive << buffer;
  archive << cd.large_attr.SerializeAsString();
  archive << cd.query_args.SerializeAsString();

  return archive;
}

grape::OutArchive& operator>>(grape::OutArchive& archive, CommandDetail& cd) {
  std::map<int, std::string> buffer;
  std::string s_large_attr, s_args;

  archive >> cd.type;
  archive >> buffer;
  archive >> s_large_attr;
  archive >> s_args;

  for (auto& pair : buffer) {
    rpc::AttrValue attr_value;
    attr_value.ParseFromString(pair.second);
    cd.params[pair.first] = attr_value;
  }
  cd.large_attr.ParseFromString(s_large_attr);
  cd.query_args.ParseFromString(s_args);

  return archive;
}

}  // namespace gs
