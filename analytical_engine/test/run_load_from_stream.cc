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

#include <cstdio>
#include <fstream>
#include <memory>
#include <string>
#include <vector>

#include "vineyard/basic/stream/dataframe_stream.h"
#include "vineyard/basic/stream/parallel_stream.h"
#include "vineyard/client/client.h"
#include "vineyard/graph/fragment/arrow_fragment.h"

#include "core/io/property_parser.h"
#include "core/loader/arrow_fragment_loader.h"

using GraphType =
    vineyard::ArrowFragment<vineyard::property_graph_types::OID_TYPE,
                            vineyard::property_graph_types::VID_TYPE>;
using GraphLoaderType =
    gs::ArrowFragmentLoader<vineyard::property_graph_types::OID_TYPE,
                            vineyard::property_graph_types::VID_TYPE>;

static std::shared_ptr<gs::detail::Graph> make_graph_info(
    vineyard::Client& client, std::vector<std::string> const& estream,
    std::vector<std::string> const& vstreams, bool directed) {
  auto graphinfo = std::make_shared<gs::detail::Graph>();
  // add estreams
  for (auto const& es : estream) {
    auto edge = std::make_shared<gs::detail::Edge>();
    std::vector<std::string> ess;
    boost::split(ess, es, boost::is_any_of(";"));
    for (auto const& e : ess) {
      auto sublabel = gs::detail::Edge::SubLabel();
      auto pstream = client.GetObject<vineyard::ParallelStream>(
          vineyard::ObjectIDFromString(e));
      VINEYARD_ASSERT(pstream != nullptr,
                      "The pstream " + e + " doesn't exist");
      auto stream =
          std::dynamic_pointer_cast<vineyard::DataframeStream>(pstream->Get(0));
      auto params = stream->GetParams();
      sublabel.src_label = params.at("src_label");
      sublabel.dst_label = params.at("dst_label");
      sublabel.src_vid = "0";
      sublabel.dst_vid = "1";
      sublabel.protocol = "vineyard";
      sublabel.values = e;
      edge->sub_labels.emplace_back(sublabel);
    }
    graphinfo->edges.emplace_back(edge);
  }
  // add vstreams
  for (auto const& v : vstreams) {
    auto vertex = std::make_shared<gs::detail::Vertex>();
    auto pstream = client.GetObject<vineyard::ParallelStream>(
        vineyard::ObjectIDFromString(v));
    VINEYARD_ASSERT(pstream != nullptr, "The stream " + v + " doesn't exist");
    auto stream =
        std::dynamic_pointer_cast<vineyard::DataframeStream>(pstream->Get(0));
    auto params = stream->GetParams();
    vertex->protocol = "vineyard";
    vertex->values = v;
    vertex->label = params.at("label");
    vertex->vid = "0";
    graphinfo->vertices.emplace_back(vertex);
  }
  graphinfo->directed = directed;
  return graphinfo;
}

int main(int argc, char** argv) {
  if (argc < 6) {
    printf(
        "usage: ./run_load_from_stream <ipc_socket> "
        "<e_label_num> <estreams...> "
        "<v_label_num> <vstreams...> "
        "[directed]\n");
    return 1;
  }
  int index = 1;
  std::string ipc_socket = std::string(argv[index++]);

  int edge_label_num = atoi(argv[index++]);
  std::vector<std::string> estreams;
  for (int i = 0; i < edge_label_num; ++i) {
    estreams.push_back(argv[index++]);
  }

  int vertex_label_num = atoi(argv[index++]);
  std::vector<std::string> vstreams;
  for (int i = 0; i < vertex_label_num; ++i) {
    vstreams.push_back(argv[index++]);
  }

  int directed = 1;
  if (argc > index) {
    directed = atoi(argv[index++]);
  }

  grape::InitMPIComm();
  grape::CommSpec comm_spec;
  comm_spec.Init(MPI_COMM_WORLD);

  vineyard::Client client;
  VINEYARD_CHECK_OK(client.Connect(ipc_socket));

  LOG(INFO) << "Connected to IPCServer: " << ipc_socket;

  auto graphinfo = make_graph_info(client, estreams, vstreams, directed);

  vineyard::ObjectID fragment_id;
  {
    auto loader =
        std::make_unique<GraphLoaderType>(client, comm_spec, graphinfo);
    fragment_id = boost::leaf::try_handle_all(
        [&loader]() { return loader->LoadFragment(); },
        [](const vineyard::GSError& e) {
          LOG(FATAL) << e.error_msg;
          return 0;
        },
        [](const boost::leaf::error_info& unmatched) {
          LOG(FATAL) << "Unmatched error " << unmatched;
          return 0;
        });
  }

  LOG(INFO) << "[worker-" << comm_spec.worker_id()
            << "] loaded graph to vineyard: " << fragment_id << " ...";

  std::shared_ptr<GraphType> frag =
      std::dynamic_pointer_cast<GraphType>(client.GetObject(fragment_id));
  auto schema = frag->schema();

  LOG(INFO) << "[worker-" << comm_spec.worker_id()
            << "] loaded graph from vineyard: " << schema.ToJSONString();

  MPI_Barrier(comm_spec.comm());

  grape::FinalizeMPIComm();
  return 0;
}
