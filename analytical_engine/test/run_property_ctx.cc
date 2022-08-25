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
#include <string>

#include "glog/logging.h"

#include "grape/grape.h"
#include "grape/util.h"
#include "vineyard/client/client.h"
#include "vineyard/graph/fragment/arrow_fragment.h"

#include "apps/property/property_sssp.h"
#include "core/context/i_context.h"
#include "core/context/labeled_vertex_property_context.h"
#include "core/loader/arrow_fragment_loader.h"
#include "core/utils/transform_utils.h"

namespace bl = boost::leaf;

using GraphType =
    vineyard::ArrowFragment<vineyard::property_graph_types::OID_TYPE,
                            vineyard::property_graph_types::VID_TYPE>;

void RunPropertySSSP(std::shared_ptr<GraphType> fragment,
                     const grape::CommSpec& comm_spec,
                     const std::string& out_prefix) {
  using AppType = gs::PropertySSSP<GraphType>;
  auto app = std::make_shared<AppType>();
  auto worker = AppType::CreateWorker(app, fragment);
  auto spec = grape::DefaultParallelEngineSpec();

  worker->Init(comm_spec, spec);
  MPI_Barrier(comm_spec.comm());
  worker->Query(4);

  auto ctx = worker->GetContext();

  worker->Finalize();

  std::shared_ptr<gs::LabeledVertexPropertyContextWrapper<GraphType>> t_ctx =
      std::dynamic_pointer_cast<
          gs::LabeledVertexPropertyContextWrapper<GraphType>>(ctx);
  MPI_Barrier(comm_spec.comm());
  {
    auto selector = gs::LabeledSelector::parse("r:label0.property0").value();
    auto arc = t_ctx->ToNdArray(comm_spec, selector, {"", ""}).value();
    if (comm_spec.fid() == 0) {
      gs::output_nd_array(std::move(*arc), out_prefix + "nd_array_result_0");
    } else {
      CHECK(arc->Empty());
    }
  }
  MPI_Barrier(comm_spec.comm());
  {
    auto selector = gs::LabeledSelector::parse("r:label1.property0").value();
    auto arc = t_ctx->ToNdArray(comm_spec, selector, {"", ""}).value();
    if (comm_spec.fid() == 0) {
      gs::output_nd_array(std::move(*arc), out_prefix + "nd_array_result_1");
    } else {
      CHECK(arc->Empty());
    }
  }
  MPI_Barrier(comm_spec.comm());
  {
    auto selector = gs::LabeledSelector::parse("r:label2.property0").value();
    auto arc = t_ctx->ToNdArray(comm_spec, selector, {"", ""}).value();
    if (comm_spec.fid() == 0) {
      gs::output_nd_array(std::move(*arc), out_prefix + "nd_array_result_2");
    } else {
      CHECK(arc->Empty());
    }
  }
  MPI_Barrier(comm_spec.comm());
  {
    auto selector = gs::LabeledSelector::parse("r:label3.property0").value();
    auto arc = t_ctx->ToNdArray(comm_spec, selector, {"", ""}).value();
    if (comm_spec.fid() == 0) {
      gs::output_nd_array(std::move(*arc), out_prefix + "nd_array_result_3");
    } else {
      CHECK(arc->Empty());
    }
  }
  MPI_Barrier(comm_spec.comm());
  {
    auto selector = gs::LabeledSelector::parse("r:label0.id").value();
    auto arc = t_ctx->ToNdArray(comm_spec, selector, {"", ""}).value();
    if (comm_spec.fid() == 0) {
      gs::output_nd_array(std::move(*arc), out_prefix + "nd_array_id_0");
    } else {
      CHECK(arc->Empty());
    }
  }
  MPI_Barrier(comm_spec.comm());
  {
    auto selector = gs::LabeledSelector::parse("r:label1.id").value();
    auto arc = t_ctx->ToNdArray(comm_spec, selector, {"", ""}).value();
    if (comm_spec.fid() == 0) {
      gs::output_nd_array(std::move(*arc), out_prefix + "nd_array_id_1");
    } else {
      CHECK(arc->Empty());
    }
  }
  MPI_Barrier(comm_spec.comm());
  {
    auto selector = gs::LabeledSelector::parse("r:label2.id").value();
    auto arc = t_ctx->ToNdArray(comm_spec, selector, {"", ""}).value();
    if (comm_spec.fid() == 0) {
      gs::output_nd_array(std::move(*arc), out_prefix + "nd_array_id_2");
    } else {
      CHECK(arc->Empty());
    }
  }
  MPI_Barrier(comm_spec.comm());
  {
    auto selector = gs::LabeledSelector::parse("r:label3.id").value();
    auto arc = t_ctx->ToNdArray(comm_spec, selector, {"", ""}).value();
    if (comm_spec.fid() == 0) {
      gs::output_nd_array(std::move(*arc), out_prefix + "nd_array_id_3");
    } else {
      CHECK(arc->Empty());
    }
  }
  MPI_Barrier(comm_spec.comm());
  {
    auto selector = gs::LabeledSelector::parse("v:label0.property0").value();
    auto arc = t_ctx->ToNdArray(comm_spec, selector, {"", ""}).value();
    if (comm_spec.fid() == 0) {
      gs::output_nd_array(std::move(*arc), out_prefix + "nd_array_property_0");
    } else {
      CHECK(arc->Empty());
    }
  }
  MPI_Barrier(comm_spec.comm());
  {
    auto selector = gs::LabeledSelector::parse("v:label1.property0").value();
    auto arc = t_ctx->ToNdArray(comm_spec, selector, {"", ""}).value();
    if (comm_spec.fid() == 0) {
      gs::output_nd_array(std::move(*arc), out_prefix + "nd_array_property_1");
    } else {
      CHECK(arc->Empty());
    }
  }
  MPI_Barrier(comm_spec.comm());
  {
    auto selector = gs::LabeledSelector::parse("v:label2.property0").value();
    auto arc = t_ctx->ToNdArray(comm_spec, selector, {"", ""}).value();
    if (comm_spec.fid() == 0) {
      gs::output_nd_array(std::move(*arc), out_prefix + "nd_array_property_2");
    } else {
      CHECK(arc->Empty());
    }
  }
  MPI_Barrier(comm_spec.comm());
  {
    auto selector = gs::LabeledSelector::parse("v:label3.property0").value();
    auto arc = t_ctx->ToNdArray(comm_spec, selector, {"", ""}).value();
    if (comm_spec.fid() == 0) {
      gs::output_nd_array(std::move(*arc), out_prefix + "nd_array_property_3");
    } else {
      CHECK(arc->Empty());
    }
  }

  MPI_Barrier(comm_spec.comm());
  {
    std::vector<std::pair<std::string, std::string>> selector_list;
    selector_list.emplace_back("id", "v:label0.id");
    selector_list.emplace_back("result", "r:label0.property0");
    selector_list.emplace_back("property", "v:label0.property0");
    selector_list.emplace_back("result2", "r:label0.property0");

    std::string selectors = gs::generate_selectors(selector_list);
    auto parsed_selectors =
        gs::LabeledSelector::ParseSelectors(selectors).value();
    auto arc =
        t_ctx->ToDataframe(comm_spec, parsed_selectors, {"", ""}).value();

    if (comm_spec.fid() == 0) {
      gs::output_dataframe(std::move(*arc), out_prefix + "dataframe_0");
    } else {
      CHECK(arc->Empty());
    }
  }

  {
    std::vector<std::pair<std::string, std::string>> selector_list;
    selector_list.emplace_back("id", "v:label1.id");
    selector_list.emplace_back("result", "r:label1.property0");
    selector_list.emplace_back("property", "v:label1.property0");
    selector_list.emplace_back("result2", "r:label1.property0");

    std::string selectors = gs::generate_selectors(selector_list);
    auto parsed_selectors =
        gs::LabeledSelector::ParseSelectors(selectors).value();
    auto arc =
        t_ctx->ToDataframe(comm_spec, parsed_selectors, {"", ""}).value();

    if (comm_spec.fid() == 0) {
      gs::output_dataframe(std::move(*arc), out_prefix + "dataframe_1");
    } else {
      CHECK(arc->Empty());
    }
  }

  {
    std::vector<std::pair<std::string, std::string>> selector_list;
    selector_list.emplace_back("id", "v:label2.id");
    selector_list.emplace_back("result", "r:label2.property0");
    selector_list.emplace_back("property", "v:label2.property0");
    selector_list.emplace_back("result2", "r:label2.property0");

    std::string selectors = gs::generate_selectors(selector_list);
    auto parsed_selectors =
        gs::LabeledSelector::ParseSelectors(selectors).value();
    auto arc =
        t_ctx->ToDataframe(comm_spec, parsed_selectors, {"", ""}).value();

    if (comm_spec.fid() == 0) {
      gs::output_dataframe(std::move(*arc), out_prefix + "dataframe_2");
    } else {
      CHECK(arc->Empty());
    }
  }

  {
    std::vector<std::pair<std::string, std::string>> selector_list;
    selector_list.emplace_back("id", "v:label3.id");
    selector_list.emplace_back("result", "r:label3.property0");
    selector_list.emplace_back("property", "v:label3.property0");
    selector_list.emplace_back("result2", "r:label3.property0");

    std::string selectors = gs::generate_selectors(selector_list);
    auto parsed_selectors =
        gs::LabeledSelector::ParseSelectors(selectors).value();
    auto arc =
        t_ctx->ToDataframe(comm_spec, parsed_selectors, {"", ""}).value();

    if (comm_spec.fid() == 0) {
      gs::output_dataframe(std::move(*arc), out_prefix + "dataframe_3");
    } else {
      CHECK(arc->Empty());
    }
  }
}

void Run(vineyard::Client& client, const grape::CommSpec& comm_spec,
         vineyard::ObjectID id) {
  std::shared_ptr<GraphType> fragment =
      std::dynamic_pointer_cast<GraphType>(client.GetObject(id));

  RunPropertySSSP(fragment, comm_spec, "./output_property_ctx_sssp/");
}

int main(int argc, char** argv) {
  if (argc < 6) {
    printf(
        "usage: ./run_property_ctx <ipc_socket> <e_label_num> <efiles...> "
        "<v_label_num> <vfiles...> [directed]\n");
    return 1;
  }
  int index = 1;
  std::string ipc_socket = std::string(argv[index++]);

  int edge_label_num = atoi(argv[index++]);
  std::vector<std::string> efiles;
  for (int i = 0; i < edge_label_num; ++i) {
    efiles.push_back(argv[index++]);
  }

  int vertex_label_num = atoi(argv[index++]);
  std::vector<std::string> vfiles;
  for (int i = 0; i < vertex_label_num; ++i) {
    vfiles.push_back(argv[index++]);
  }

  int directed = 1;
  if (argc > index) {
    directed = atoi(argv[index]);
  }

  grape::InitMPIComm();
  {
    grape::CommSpec comm_spec;
    comm_spec.Init(MPI_COMM_WORLD);

    vineyard::Client client;
    VINEYARD_CHECK_OK(client.Connect(ipc_socket));

    LOG(INFO) << "Connected to IPCServer: " << ipc_socket;

    vineyard::ObjectID fragment_id;
    {
      auto loader = std::make_unique<
          gs::ArrowFragmentLoader<vineyard::property_graph_types::OID_TYPE,
                                  vineyard::property_graph_types::VID_TYPE>>(
          client, comm_spec, efiles, vfiles, directed != 0);
      fragment_id =
          bl::try_handle_all([&loader]() { return loader->LoadFragment(); },
                             [](const vineyard::GSError& e) {
                               LOG(FATAL) << e.error_msg;
                               return 0;
                             },
                             [](const bl::error_info& unmatched) {
                               LOG(FATAL) << "Unmatched error " << unmatched;
                               return 0;
                             });
    }

    LOG(INFO) << "[worker-" << comm_spec.worker_id()
              << "] loaded graph to vineyard ...";

    MPI_Barrier(comm_spec.comm());

    Run(client, comm_spec, fragment_id);

    MPI_Barrier(comm_spec.comm());
  }

  grape::FinalizeMPIComm();
  return 0;
}
