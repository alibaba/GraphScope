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

#include <stdio.h>

#include <fstream>
#include <memory>
#include <string>
#include <vector>

#include "glog/logging.h"

#include "vineyard/client/client.h"
#include "vineyard/graph/vertex_map/arrow_vertex_map.h"
#include "vineyard/io/io/local_io_adaptor.h"

std::string generate_path(const std::string& prefix, int part_num) {
  if (part_num == 1) {
    return prefix;
  } else {
    std::string ret;
    bool first = true;
    for (int i = 0; i < part_num; ++i) {
      if (first) {
        first = false;
        ret += (prefix + "_" + std::to_string(i));
      } else {
        ret += (";" + prefix + "_" + std::to_string(i));
      }
    }
    return ret;
  }
}

void loadVertices(
    const std::string& vfile, int vertex_label_num,
    std::vector<std::vector<std::shared_ptr<arrow::Table>>>& v_tables) {
  std::vector<std::string> v_list;
  boost::split(v_list, vfile, boost::is_any_of(";"));
  v_tables.resize(vertex_label_num);

  for (auto fname : v_list) {
    for (int i = 0; i < vertex_label_num; ++i) {
      std::unique_ptr<vineyard::LocalIOAdaptor> io_adaptor(
          new vineyard::LocalIOAdaptor(fname + "_" + std::to_string(i) +
                                       "#header_row=true"));
      io_adaptor->SetPartialRead(0, 1);
      io_adaptor->Open();
      std::shared_ptr<arrow::Table> tmp_table;
      io_adaptor->ReadTable(&tmp_table);
      v_tables[i].push_back(tmp_table);
    }
  }
}

int main(int argc, char** argv) {
  if (argc < 5) {
    printf(
        "usage: ./test_string_vertex_map <ipc_socket> <vfile_prefix> "
        "<v_label_num> <vfile_part>\n");
    return 1;
  }
  std::string ipc_socket = std::string(argv[1]);
  vineyard::fid_t fnum = atoi(argv[4]);
  std::string vpath = generate_path(argv[2], atoi(argv[4]));
  int vertex_label_num = atoi(argv[3]);

  vineyard::Client client;
  VINEYARD_CHECK_OK(client.Connect(ipc_socket));

  LOG(INFO) << "Connected to IPCServer: " << ipc_socket;

  vineyard::ObjectID vm_id;
  {
    std::vector<std::vector<std::shared_ptr<arrow::Table>>> v_tables;
    loadVertices(vpath, vertex_label_num, v_tables);

    std::vector<std::vector<std::shared_ptr<arrow::LargeStringArray>>>
        oid_lists(vertex_label_num);
    for (auto& vec : oid_lists) {
      vec.resize(fnum);
    }

    CHECK_EQ(v_tables.size(), vertex_label_num);
    for (int i = 0; i < vertex_label_num; ++i) {
      CHECK_EQ(v_tables[i].size(), fnum);
      for (vineyard::fid_t j = 0; j < fnum; ++j) {
        auto table = v_tables[i][j];
        std::shared_ptr<arrow::Table> combined_table;
        if (table->column(0)->num_chunks() != 1) {
          table->CombineChunks(arrow::default_memory_pool(), &combined_table);
        } else {
          combined_table = table;
        }
        oid_lists[i][j] = std::dynamic_pointer_cast<arrow::LargeStringArray>(
            combined_table->column(0)->chunk(0));
      }
    }

    vineyard::BasicArrowVertexMapBuilder<vineyard::arrow_string_view, uint64_t>
        vm_builder(client, fnum, vertex_label_num, oid_lists);

    auto vm = vm_builder.Seal(client);
    vm_id = vm->id();
  }

  auto vm_ptr = std::dynamic_pointer_cast<
      vineyard::ArrowVertexMap<vineyard::arrow_string_view, uint64_t>>(
      client.GetObject(vm_id));

  vineyard::IdParser<uint64_t> id_parser;
  id_parser.Init(fnum, vertex_label_num);

  for (vineyard::fid_t i = 0; i < fnum; ++i) {
    for (int j = 0; j < vertex_label_num; ++j) {
      std::string path = "./vm_" + std::to_string(i) + "_" + std::to_string(j);
      std::ofstream fout;
      fout.open(path);

      uint64_t vnum = vm_ptr->GetInnerVertexSize(i, j);
      for (uint64_t k = 0; k < vnum; ++k) {
        uint64_t gid = id_parser.GenerateId(i, j, k);
        vineyard::arrow_string_view oid;
        CHECK(vm_ptr->GetOid(gid, oid));

        fout << oid << std::endl;
      }

      fout.close();
    }
  }

  return 0;
}
