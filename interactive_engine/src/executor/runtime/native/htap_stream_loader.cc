/**
 * Copyright 2020 Alibaba Group Holding Limited.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include <stdio.h>

#include <fstream>
#include <string>

#include "boost/lexical_cast.hpp"
#include "glog/logging.h"

#include "vineyard/client/client.h"
#include "vineyard/graph/fragment/arrow_fragment.h"
#include "vineyard/graph/fragment/arrow_fragment_group.h"
#include "vineyard/graph/loader/arrow_fragment_loader.h"

#include "htap_types.h"
#include "property_graph_stream.h"

#include "htap_ds_impl.h"

using batch_group_t = std::unordered_map<vineyard::htap_types::LABEL_ID_TYPE,
                     std::vector<std::shared_ptr<arrow::RecordBatch>>>;

static batch_group_t
    gather_chunks_in_stream(vineyard::Client &client,
                            std::shared_ptr<vineyard::GlobalPGStream> gs,
                            std::string &graph_schema,
                            bool vertex) {
  std::vector<std::shared_ptr<vineyard::PropertyGraphInStream>> available_streams;
  for (auto const& out_stream : gs->AvailableStreams(client)) {
    available_streams.emplace_back(
        std::make_shared<vineyard::PropertyGraphInStream>(client, *out_stream));
  }
  VINEYARD_ASSERT(!available_streams.empty());
  graph_schema = available_streams[0]->graph_schema()->ToJSONString();

  batch_group_t batches;

  std::string tag = vertex ? "VERTEX" : "EDGE";

  {
    std::mutex mtx;
    std::vector<std::thread> pull_stream_threads(available_streams.size());
    for (size_t i = 0; i < available_streams.size(); ++i) {
      pull_stream_threads[i] = std::thread([&, i]() {
        auto stream = available_streams[i];
        while (true) {
          std::shared_ptr<arrow::RecordBatch> batch = nullptr;
          vineyard::Status status;
          if (vertex) {
            status = stream->GetNextVertices(client, batch);
          } else {
            status = stream->GetNextEdges(client, batch);
          }
          if (status.IsStreamDrained() || status.IsStreamFailed()) {
            LOG(INFO) << "the ith " << tag << " stream stopped: " << i << ", "
                      << status.ToString();
            break;
          }
          VINEYARD_ASSERT(batch != nullptr);
          std::lock_guard<std::mutex> mtx_guard(mtx);
          auto metadata = batch->schema()->metadata();
          LOG(INFO) << "batch schema = " << batch->schema()->ToString(true);
          std::unordered_map<std::string, std::string> meta_map;
          metadata->ToUnorderedMap(&meta_map);
          auto label_id = boost::lexical_cast<vineyard::htap_types::LABEL_ID_TYPE>(
              meta_map.at("label_id"));
          LOG(INFO) << "receive " << tag << " batch for label " << label_id
                    << ", size = " << batch->num_rows();
          batches[label_id].emplace_back(batch);
        }
      });
    }
    for (auto& thrd : pull_stream_threads) {
      thrd.join();
    }
  }

  return batches;
}
  
static std::vector<std::shared_ptr<arrow::Table>>
    gather_vertex_chunks_in_stream(vineyard::Client &client,
                            std::shared_ptr<vineyard::GlobalPGStream> gs,
                            std::string &graph_schema) {

  auto batch_groups = gather_chunks_in_stream(client, gs, graph_schema, true);
  std::vector<std::shared_ptr<arrow::Table>> vtables;

  for (auto const &vertices: batch_groups) {
      std::shared_ptr<arrow::Table> table;
      VINEYARD_CHECK_OK(vineyard::RecordBatchesToTable(vertices.second, &table));
      vtables.emplace_back(table);
  }
  return vtables;
}

static std::vector<std::vector<std::shared_ptr<arrow::Table>>>
    gather_edge_chunks_in_stream(vineyard::Client &client,
                            std::shared_ptr<vineyard::GlobalPGStream> gs,
                            std::string &graph_schema) {

  auto batch_groups = gather_chunks_in_stream(client, gs, graph_schema, false);
  std::vector<std::vector<std::shared_ptr<arrow::Table>>> etables;

  for (auto const &edges: batch_groups) {
    std::map<std::pair<LabelId, LabelId>,
             std::vector<std::shared_ptr<arrow::RecordBatch>>> sub_batches;
    for (auto const &batch: edges.second) {
      auto meta = batch->schema()->metadata();
      std::unordered_map<std::string, std::string> meta_map;
      meta->ToUnorderedMap(&meta_map);
      auto src_label_id = std::strtol(meta_map.at("src_label_id").c_str(), nullptr, 10);
      auto dst_label_id = std::strtol(meta_map.at("dst_label_id").c_str(), nullptr, 10);
      sub_batches[std::make_pair(src_label_id, dst_label_id)].emplace_back(batch);
    }
    std::vector<std::shared_ptr<arrow::Table>> tables;
    for (auto const &kv: sub_batches) {
      std::shared_ptr<arrow::Table> table;
      VINEYARD_CHECK_OK(vineyard::RecordBatchesToTable(kv.second, &table));
      tables.emplace_back(table);
    }
    etables.emplace_back(tables);
  }
  return etables;
}

int main(int argc, char** argv) {
  if (argc < 2) {
    printf("usage: ./htap_stream_wait_loader_test <name_to_wait>\n");
    return 1;
  }

  std::string name_to_wait(argv[1]);

  grape::InitMPIComm();
  grape::CommSpec comm_spec;
  comm_spec.Init(MPI_COMM_WORLD);

  vineyard::Client& client = vineyard::Client::Default();

  VINEYARD_CHECK_OK(client.DropName(name_to_wait));
  vineyard::ObjectID global_streamobject_id;
  LOG(INFO) << "wait for stream: " << name_to_wait;
  VINEYARD_CHECK_OK(client.GetName(name_to_wait, global_streamobject_id, true));

  LOG(INFO) << "receive global stream object id: "
            << vineyard::ObjectIDToString(global_streamobject_id)
            << ", " << global_streamobject_id;

  MPI_Barrier(comm_spec.comm());

  auto gs = std::dynamic_pointer_cast<vineyard::GlobalPGStream>(
      client.GetObject(global_streamobject_id));
  VINEYARD_ASSERT(gs != nullptr);

  std::string graph_schema;
  auto vtables = gather_vertex_chunks_in_stream(client, gs, graph_schema);
  auto etables = gather_edge_chunks_in_stream(client, gs, graph_schema);

  MPI_Barrier(comm_spec.comm());

  // load fragment
  vineyard::ObjectID fragment_group_id;
  {
    auto loader = std::make_unique<
        vineyard::ArrowFragmentLoader<vineyard::property_graph_types::OID_TYPE,
                                      vineyard::property_graph_types::VID_TYPE>>(
        client, comm_spec, vtables, etables, true);

    fragment_group_id = boost::leaf::try_handle_all(
        [&loader]() {
          return loader->LoadFragmentAsFragmentGroup();
        },
        [](const boost::leaf::error_info& unmatched) {
          LOG(FATAL) << "Unmatched error " << unmatched;
          return 0;
        });
  }

  MPI_Barrier(comm_spec.comm());
  LOG(INFO) << "[fragment group id]: " << fragment_group_id;

  std::shared_ptr<vineyard::ArrowFragmentGroup> fg =
      std::dynamic_pointer_cast<vineyard::ArrowFragmentGroup>(
          client.GetObject(fragment_group_id));

  for (const auto& pair : fg->Fragments()) {
    LOG(INFO) << "[frag-" << pair.first << "]: " << pair.second;
  }
  VINEYARD_CHECK_OK(client.DropName(name_to_wait));

  htap_impl::GraphHandleImpl *handle = new htap_impl::GraphHandleImpl();
  htap_impl::get_graph_handle(fragment_group_id, 1, handle);
  auto schema = handle->schema;
  if (comm_spec.worker_id() == 0) {
    LOG(INFO) << "schema = " << schema->ToJSONString();
  }
  schema->DumpToFile("/tmp/" + std::to_string(fragment_group_id) + ".json");

  MPI_Barrier(comm_spec.comm());

  return 0;
}
