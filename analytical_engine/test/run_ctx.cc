/** Copyright 2020 Alibaba Group Holding Limited.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

#include <cstdio>
#include <fstream>
#include <string>

#include "glog/logging.h"

#include "grape/grape.h"
#include "grape/util.h"
#include "sssp/sssp.h"
#include "vineyard/client/client.h"
#include "vineyard/graph/fragment/arrow_fragment.h"
#include "vineyard/graph/utils/grape_utils.h"

#include "core/error.h"
#include "core/fragment/arrow_projected_fragment.h"
#include "core/loader/arrow_fragment_loader.h"
#include "core/object/fragment_wrapper.h"
#include "core/utils/transform_utils.h"

using FragmentType =
    gs::ArrowProjectedFragment<int64_t, uint64_t, double, int64_t>;

void RunCTXSSSP(std::shared_ptr<FragmentType> fragment,
                const grape::CommSpec& comm_spec, const std::string& out_prefix,
                vineyard::Client& client, vineyard::ObjectID& ndarray_object,
                vineyard::ObjectID& dataframe_object) {
  using AppType = grape::SSSP<FragmentType>;
  using DataT = double;
  auto app = std::make_shared<AppType>();
  auto worker = AppType::CreateWorker(app, fragment);
  auto spec = grape::DefaultParallelEngineSpec();
  worker->Init(comm_spec, spec);
  MPI_Barrier(comm_spec.comm());
  worker->Query(4);

  auto ctx = worker->GetContext();

  worker->Finalize();

  std::ofstream ostream;
  std::string output_path =
      grape::GetResultFilename(out_prefix, fragment->fid());
  LOG(INFO) << "to write to: " << output_path;
  ostream.open(output_path);
  ctx->Output(ostream);
  ostream.close();

  gs::rpc::graph::GraphDefPb graph_def;
  graph_def.set_graph_type(gs::rpc::graph::ARROW_PROJECTED);

  auto frag_wrapper = std::make_shared<gs::FragmentWrapper<FragmentType>>(
      "graph_123", graph_def, fragment);
  gs::VertexDataContextWrapper<FragmentType, DataT> ctx_wrapper(
      "ctx_wrapper_" + vineyard::random_string(8), frag_wrapper, ctx);

  auto selector = gs::Selector::parse("r").value();
  auto range = std::make_pair("", "");
  std::unique_ptr<grape::InArchive> arc =
      std::move(ctx_wrapper.ToNdArray(comm_spec, selector, range).value());

  if (comm_spec.worker_id() == 0) {
    grape::OutArchive oarc;
    oarc = std::move(*arc);

    int64_t ndim, length1, length2;
    int data_type;
    oarc >> ndim;
    CHECK_EQ(ndim, 1);
    oarc >> length1;
    oarc >> data_type;
    CHECK_EQ(data_type, 2);
    oarc >> length2;
    CHECK_EQ(length1, length2);

    std::ofstream assembled_ostream;
    std::string assembled_output_path = out_prefix + "/assembled_ndarray.dat";
    assembled_ostream.open(assembled_output_path);

    for (int64_t i = 0; i < length1; ++i) {
      double v;
      oarc >> v;
      assembled_ostream << v << std::endl;
    }

    CHECK(oarc.Empty());

    assembled_ostream.close();
  }

  arc->Clear();
  std::string s_selectors;
  {
    std::vector<std::pair<std::string, std::string>> selector_list;
    selector_list.emplace_back("id", "v.id");
    selector_list.emplace_back("result", "r");
    s_selectors = gs::generate_selectors(selector_list);
  }
  auto selectors = gs::Selector::ParseSelectors(s_selectors).value();
  arc = std::move(ctx_wrapper.ToDataframe(comm_spec, selectors, range).value());

  if (comm_spec.worker_id() == 0) {
    grape::OutArchive oarc;
    oarc = std::move(*arc);

    int64_t ndim, length;
    int col_type1, col_type2;
    oarc >> ndim;
    CHECK_EQ(ndim, 2);
    oarc >> length;

    std::string col_name1, col_name2;
    oarc >> col_name1;
    oarc >> col_type1;
    CHECK_EQ(col_type1, 3);

    std::ofstream assembled_col1_ostream;
    std::string assembled_col1_output_path =
        out_prefix + "/assembled_dataframe_col_1_" + col_name1 + ".dat";
    assembled_col1_ostream.open(assembled_col1_output_path);
    for (int64_t i = 0; i < length; ++i) {
      int64_t id;
      oarc >> id;
      assembled_col1_ostream << id << std::endl;
    }
    assembled_col1_ostream.close();

    oarc >> col_name2;
    oarc >> col_type2;
    CHECK_EQ(col_type2, 2);

    std::ofstream assembled_col2_ostream;
    std::string assembled_col2_output_path =
        out_prefix + "/assembled_dataframe_col_2_" + col_name2 + ".dat";
    assembled_col2_ostream.open(assembled_col2_output_path);
    for (int64_t i = 0; i < length; ++i) {
      double data;
      oarc >> data;
      assembled_col2_ostream << data << std::endl;
    }
    assembled_col2_ostream.close();

    CHECK(oarc.Empty());
  }
  {
    auto tmp = ctx_wrapper.ToVineyardTensor(comm_spec, client, selector, range);
    CHECK(tmp);
    ndarray_object = tmp.value();
  }
  {
    auto tmp =
        ctx_wrapper.ToVineyardDataframe(comm_spec, client, selectors, range);
    CHECK(tmp);
    dataframe_object = tmp.value();
  }
}

void output_vineyard_tensor(vineyard::Client& client,
                            vineyard::ObjectID tensor_object,
                            const grape::CommSpec& comm_spec,
                            const std::string& prefix) {
  auto stored_tensor = std::dynamic_pointer_cast<vineyard::GlobalTensor>(
      client.GetObject(tensor_object));
  auto const& shape = stored_tensor->shape();
  auto const& partition_shape = stored_tensor->partition_shape();
  auto const& local_chunks = stored_tensor->LocalPartitions(client);
  CHECK_EQ(shape.size(), 1);
  CHECK_EQ(partition_shape.size(), 1);
  CHECK_EQ(local_chunks.size(), static_cast<size_t>(comm_spec.local_num()));
  if (comm_spec.worker_id() == 0) {
    LOG(INFO) << "tensor shape: " << shape[0] << ", " << partition_shape[0];
  }

  if (comm_spec.local_id() == 0) {
    for (auto obj : local_chunks) {
      auto single_tensor = std::dynamic_pointer_cast<vineyard::ITensor>(obj);
      if (single_tensor->value_type() != vineyard::AnyType::Double) {
        LOG(FATAL) << "type not correct...";
      }
      CHECK_EQ(single_tensor->shape().size(), 1);
      CHECK_EQ(single_tensor->partition_index().size(), 1);
      int64_t length = single_tensor->shape()[0];
      LOG(INFO) << "[worker-" << comm_spec.worker_id() << "]: tensor chunk-"
                << single_tensor->partition_index()[0] << ": " << length;
      auto casted_tensor =
          std::dynamic_pointer_cast<vineyard::Tensor<double>>(single_tensor);
      std::string output_path =
          prefix + "/single_tensor_" +
          std::to_string(single_tensor->partition_index()[0]) + ".dat";
      std::ofstream fout;
      fout.open(output_path);

      auto data = casted_tensor->data();
      for (int64_t i = 0; i < length; ++i) {
        fout << data[i] << std::endl;
        fout.flush();
      }

      fout.close();
    }
  }
}

void output_vineyard_dataframe(vineyard::Client& client,
                               vineyard::ObjectID dataframe_object,
                               const grape::CommSpec& comm_spec,
                               const std::string& prefix) {
  auto stored_dataframe = std::dynamic_pointer_cast<vineyard::GlobalDataFrame>(
      client.GetObject(dataframe_object));
  auto const& partition_shape = stored_dataframe->partition_shape();
  auto const& local_chunks = stored_dataframe->LocalPartitions(client);
  CHECK_EQ(local_chunks.size(), static_cast<size_t>(comm_spec.local_num()));

  if (comm_spec.worker_id() == 0) {
    LOG(INFO) << "dataframe shape: " << partition_shape.first << ", "
              << partition_shape.second;
  }

  if (comm_spec.local_id() == 0) {
    for (auto const& obj : local_chunks) {
      auto single_dataframe =
          std::dynamic_pointer_cast<vineyard::DataFrame>(obj);
      auto chunk_index = single_dataframe->partition_index();
      auto shape = single_dataframe->shape();
      LOG(INFO) << "[worker-" << comm_spec.worker_id() << "]: dataframe chunk-("
                << chunk_index.first << ", " << chunk_index.second
                << ") shape is (" << shape.first << ", " << shape.second << ")";

      auto i_id_tensor = single_dataframe->Column("id");
      if (i_id_tensor->value_type() != vineyard::AnyType::Int64) {
        LOG(FATAL) << "type not correct...";
      }
      CHECK_EQ(i_id_tensor->shape().size(), 1);
      int64_t id_tensor_length = i_id_tensor->shape()[0];
      auto id_tensor =
          std::dynamic_pointer_cast<vineyard::Tensor<int64_t>>(i_id_tensor);

      auto i_data_tensor = single_dataframe->Column("data");
      if (i_data_tensor->value_type() != vineyard::AnyType::Double) {
        LOG(FATAL) << "type not correct...";
      }
      CHECK_EQ(i_data_tensor->shape().size(), 1);
      int64_t data_tensor_length = i_data_tensor->shape()[0];
      auto data_tensor =
          std::dynamic_pointer_cast<vineyard::Tensor<double>>(i_data_tensor);

      CHECK_EQ(id_tensor_length, data_tensor_length);

      std::string output_path = prefix + "/single_dataframe_" +
                                std::to_string(chunk_index.first) + "_" +
                                std::to_string(chunk_index.second) + ".dat";
      std::ofstream fout;
      fout.open(output_path);
      for (int64_t i = 0; i < id_tensor_length; ++i) {
        fout << id_tensor->data()[i] << "\t" << data_tensor->data()[i]
             << std::endl;
      }

      fout.close();
    }
  }
}

void Run(vineyard::Client& client, const grape::CommSpec& comm_spec,
         vineyard::ObjectID id) {
  using GraphType =
      vineyard::ArrowFragment<vineyard::property_graph_types::OID_TYPE,
                              vineyard::property_graph_types::VID_TYPE>;

  std::shared_ptr<GraphType> fragment =
      std::dynamic_pointer_cast<GraphType>(client.GetObject(id));

  std::shared_ptr<FragmentType> projected_fragment =
      FragmentType::Project(fragment, 0, 0, 0, 0);

  vineyard::ObjectID tensor_object, dataframe_object;

  RunCTXSSSP(projected_fragment, comm_spec, "./output_ctx_sssp/", client,
             tensor_object, dataframe_object);

  output_vineyard_tensor(client, tensor_object, comm_spec,
                         "./output_ctx_sssp/");
  output_vineyard_dataframe(client, dataframe_object, comm_spec,
                            "./output_ctx_sssp/");
}

int main(int argc, char** argv) {
  if (argc < 6) {
    printf(
        "usage: ./run_ctx <ipc_socket> <e_label_num> <efiles...> "
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
              << "] loaded graph to vineyard ...";

    MPI_Barrier(comm_spec.comm());

    Run(client, comm_spec, fragment_id);

    MPI_Barrier(comm_spec.comm());
  }

  grape::FinalizeMPIComm();
  return 0;
}
