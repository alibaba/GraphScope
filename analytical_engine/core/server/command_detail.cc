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
  // type
  archive << cd.type;
  // params
  std::map<int, std::string> buffer;
  for (auto& pair : cd.params) {
    buffer[pair.first] = pair.second.SerializeAsString();
  }
  archive << buffer;
  // large attr
  bool has_chunk_list = cd.large_attr.has_chunk_list();
  archive << has_chunk_list;
  if (has_chunk_list) {
    size_t chunk_list_size = cd.large_attr.chunk_list().items().size();
    archive << chunk_list_size;
    for (size_t i = 0; i < chunk_list_size; ++i) {
      const auto& chunk = cd.large_attr.chunk_list().items(i);
      // buffer
      archive << chunk.buffer();
      // attr
      std::map<int, std::string> attr;
      for (auto& pair : chunk.attr()) {
        attr[pair.first] = pair.second.SerializeAsString();
      }
      archive << attr;
    }
  }
  // query_args
  archive << cd.query_args.SerializeAsString();

  return archive;
}

grape::OutArchive& operator>>(grape::OutArchive& archive, CommandDetail& cd) {
  // type
  archive >> cd.type;
  // params
  std::map<int, std::string> buffer;
  archive >> buffer;
  for (auto& pair : buffer) {
    rpc::AttrValue attr_value;
    attr_value.ParseFromString(pair.second);
    cd.params[pair.first] = attr_value;
  }
  // large attr
  bool has_chunk_list;
  archive >> has_chunk_list;
  if (has_chunk_list) {
    size_t chunk_list_size;
    archive >> chunk_list_size;
    if (chunk_list_size > 0) {
      auto* chunk_list = cd.large_attr.mutable_chunk_list();
      for (size_t i = 0; i < chunk_list_size; ++i) {
        auto* chunk = chunk_list->add_items();
        // buffer
        std::string buf;
        archive >> buf;
        chunk->set_buffer(std::move(buf));
        // attr
        auto* mutable_attr = chunk->mutable_attr();
        std::map<int, std::string> attr;
        archive >> attr;
        for (auto& pair : attr) {
          rpc::AttrValue attr_value;
          attr_value.ParseFromString(pair.second);
          (*mutable_attr)[pair.first].CopyFrom(attr_value);
        }
      }
    }
  }
  // query_args
  std::string s_args;
  archive >> s_args;
  cd.query_args.ParseFromString(s_args);

  return archive;
}

}  // namespace gs
