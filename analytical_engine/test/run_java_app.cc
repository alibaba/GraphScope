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

#include <unistd.h>
#include <algorithm>
#include <cstddef>
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

#include "boost/property_tree/json_parser.hpp"
#include "boost/property_tree/ptree.hpp"
#include "gflags/gflags.h"
#include "gflags/gflags_declare.h"
#include "glog/logging.h"

#include "grape/grape.h"
#include "grape/types.h"
#include "grape/util.h"
#include "vineyard/basic/ds/types.h"
#include "vineyard/client/client.h"
#include "vineyard/graph/fragment/arrow_fragment.h"
#include "vineyard/graph/utils/grape_utils.h"

#include "apps/property/sssp_property.h"
#include "core/error.h"
#include "core/fragment/arrow_projected_fragment.h"
#include "core/java/javasdk.h"
#include "core/java/type_alias.h"
#include "core/loader/arrow_fragment_loader.h"
#include "core/object/fragment_wrapper.h"
#include "core/utils/transform_utils.h"
#include "java_pie/java_pie_projected_default_app.h"
#include "java_pie/java_pie_property_parallel_app.h"
#include "proto/graph_def.pb.h"

using FragmentType =
    vineyard::ArrowFragment<vineyard::property_graph_types::OID_TYPE,
                            vineyard::property_graph_types::VID_TYPE>;
using ProjectedFragmentType =
    gs::ArrowProjectedFragment<int64_t, uint64_t, double, int64_t>;
void output_nd_array(const grape::CommSpec& comm_spec,
                     std::unique_ptr<grape::InArchive> arc,
                     const std::string& output_path, int data_type_expected) {
  if (comm_spec.worker_id() == 0) {
    grape::OutArchive oarc;
    oarc = std::move(*arc);

    int64_t ndim, length1, length2;
    int data_type;
    oarc >> ndim;
    VLOG(1) << "ndim: " << ndim;
    CHECK_EQ(ndim, 1);
    oarc >> length1;
    oarc >> data_type;
    VLOG(1) << "length1: " << length1 << ",data type: " << data_type;
    CHECK_EQ(data_type, data_type_expected);
    oarc >> length2;
    VLOG(1) << "length2: " << length2;
    CHECK_EQ(length1, length2);

    std::ofstream assembled_ostream;
    assembled_ostream.open(output_path);
    VLOG(1) << "osream " << output_path;
    if (data_type_expected == 7) {
      for (int64_t i = 0; i < length1; ++i) {
        double v;
        oarc >> v;
        assembled_ostream << v << std::endl;
      }
    } else if (data_type_expected == 4) {
      for (int64_t i = 0; i < length1; ++i) {
        int64_t v;
        oarc >> v;
        assembled_ostream << v << std::endl;
      }
    } else {
      LOG(FATAL) << "Unregonizable data type " << data_type_expected;
    }

    VLOG(1) << "output complete: " << oarc.Empty() << output_path;
    CHECK(oarc.Empty());

    assembled_ostream.close();
  }
}

void output_data_frame(const grape::CommSpec& comm_spec,
                       std::unique_ptr<grape::InArchive> arc,
                       const std::string& output_prefix,
                       int expected_data_type) {
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
    CHECK_EQ(col_type1, 4);  // int64_t

    std::ofstream assembled_col1_ostream;
    std::string assembled_col1_output_path =
        output_prefix + "_assembled_dataframe_col_1_" + col_name1 + ".dat";
    assembled_col1_ostream.open(assembled_col1_output_path);

    for (int64_t i = 0; i < length; ++i) {
      int64_t id;
      oarc >> id;
      assembled_col1_ostream << id << std::endl;
    }
    assembled_col1_ostream.close();

    oarc >> col_name2;
    oarc >> col_type2;
    CHECK_EQ(col_type2, expected_data_type);

    std::ofstream assembled_col2_ostream;
    std::string assembled_col2_output_path =
        output_prefix + "_assembled_dataframe_col_2_" + col_name2 + ".dat";
    assembled_col2_ostream.open(assembled_col2_output_path);
    if (expected_data_type == 7) {
      for (int64_t i = 0; i < length; ++i) {
        double data;
        oarc >> data;
        assembled_col2_ostream << data << std::endl;
      }
    } else if (expected_data_type == 4) {
      for (int64_t i = 0; i < length; ++i) {
        int64_t data;
        oarc >> data;
        assembled_col2_ostream << data << std::endl;
      }
    } else {
      LOG(FATAL) << "Unregonizable data type " << expected_data_type;
    }
    assembled_col2_ostream.close();

    CHECK(oarc.Empty());
  }
}
template <typename DATA_T>
void output_vineyard_tensor(vineyard::Client& client,
                            vineyard::ObjectID tensor_object,
                            const grape::CommSpec& comm_spec,
                            const std::string& prefix,
                            vineyard::AnyType& expected_type) {
  auto stored_tensor = std::dynamic_pointer_cast<vineyard::GlobalTensor>(
      client.GetObject(tensor_object));
  auto const& shape = stored_tensor->shape();
  auto const& partition_shape = stored_tensor->partition_shape();
  auto const& local_chunks = stored_tensor->LocalPartitions(client);
  CHECK_EQ(shape.size(), 1);
  CHECK_EQ(partition_shape.size(), 1);
  CHECK_EQ(local_chunks.size(), 1);
  if (comm_spec.worker_id() == 0) {
    VLOG(1) << "tensor shape: " << shape[0] << ", " << partition_shape[0];
  }

  if (comm_spec.local_id() == 0) {
    for (auto obj : local_chunks) {
      auto single_tensor = std::dynamic_pointer_cast<vineyard::ITensor>(obj);
      VLOG(1) << "actual type "
              << vineyard::GetAnyTypeName(single_tensor->value_type());
      if (single_tensor->value_type() != expected_type) {
        LOG(FATAL) << "type not correct...";
      }
      CHECK_EQ(single_tensor->shape().size(), 1);
      int64_t length = single_tensor->shape()[0];
      auto casted_tensor =
          std::dynamic_pointer_cast<vineyard::Tensor<DATA_T>>(single_tensor);
      std::string output_path =
          prefix + "_v6d_single_tensor_" +
          std::to_string(comm_spec.worker_id()) +
          ".dat";  // std::to_string(single_tensor->partition_index()[0])
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

void QueryProperty(vineyard::Client& client,
                   std::shared_ptr<FragmentType> fragment,
                   const grape::CommSpec& comm_spec,
                   const std::string& app_name, const std::string& out_prefix,
                   const std::string& basic_params,
                   const std::string& selector_string,
                   const std::string& selectors_string) {
  using AppType = gs::JavaPIEPropertyParallelApp<FragmentType>;
  auto app = std::make_shared<AppType>();
  auto worker = AppType::CreateWorker(app, fragment);
  auto spec = grape::DefaultParallelEngineSpec();
  worker->Init(comm_spec, spec);
  std::string lib_path = "";
  worker->Query(basic_params, lib_path);
  std::ofstream ostream;
  std::string output_path =
      grape::GetResultFilename(out_prefix, fragment->fid());

  ostream.open(output_path);
  worker->Output(ostream);
  ostream.close();

  std::shared_ptr<gs::JavaPIEPropertyContext<FragmentType>> ctx =
      worker->GetContext();
  worker->Finalize();

  gs::rpc::graph::GraphDefPb graph_def;
  graph_def.set_graph_type(gs::rpc::graph::ARROW_PROPERTY);

  auto range = std::make_pair("", "");
  auto selectors =
      gs::LabeledSelector::ParseSelectors(selectors_string).value();
  auto selector = gs::LabeledSelector::parse(selector_string).value();

  auto frag_wrapper = std::make_shared<gs::FragmentWrapper<FragmentType>>(
      "graph_123", graph_def, fragment);
  auto ctx_wrapper = ctx->CreateInnerCtxWrapper(
      "ctx_wrapper_" + vineyard::random_string(8), frag_wrapper);
  if (ctx_wrapper->context_type() == "labeled_vertex_property") {
    auto vp_ctx_wrapper =
        std::dynamic_pointer_cast<gs::ILabeledVertexPropertyContextWrapper>(
            ctx_wrapper);
    /// 0. test ndarray
    {
      std::unique_ptr<grape::InArchive> arc = std::move(
          vp_ctx_wrapper->ToNdArray(comm_spec, selector, range).value());
      std::string java_out_prefix = out_prefix + "/java_assembled_ndarray.dat";
      output_nd_array(comm_spec, std::move(arc), java_out_prefix,
                      7);  // 7 for double
    }
    VLOG(1) << "[0] java finish test ndarray";

    // 1. Test data frame
    {
      // auto selectors = gs::gs::Selector::ParseSelectors(s_selectors).value();
      std::unique_ptr<grape::InArchive> arc = std::move(
          vp_ctx_wrapper->ToDataframe(comm_spec, selectors, range).value());
      std::string java_data_frame_out_prefix = out_prefix + "/java";
      output_data_frame(comm_spec, std::move(arc), java_data_frame_out_prefix,
                        7);
    }

    VLOG(1) << "[1] java finish test dataframe";
    // 2. test vineyard tensor
    {
      auto tmp =
          vp_ctx_wrapper->ToVineyardTensor(comm_spec, client, selector, range);
      CHECK(tmp);
      vineyard::ObjectID ndarray_object = tmp.value();
      std::string java_v6d_tensor_prefix = out_prefix + "/java";
      vineyard::AnyType expected_data_type = vineyard::AnyType::Double;
      output_vineyard_tensor<double>(client, ndarray_object, comm_spec,
                                     java_v6d_tensor_prefix,
                                     expected_data_type);
    }
    VLOG(1) << "[2] java finish test vineyard tensor";
  } else if (ctx_wrapper->context_type() == "labeled_vertex_data") {
    auto vd_ctx_wrapper =
        std::dynamic_pointer_cast<gs::ILabeledVertexDataContextWrapper>(
            ctx_wrapper);
    /// 0. test ndarray
    {
      std::unique_ptr<grape::InArchive> arc = std::move(
          vd_ctx_wrapper->ToNdArray(comm_spec, selector, range).value());
      std::string java_out_prefix = out_prefix + "/java_assembled_ndarray.dat";
      output_nd_array(comm_spec, std::move(arc), java_out_prefix,
                      7);  // 7 for double
    }
    VLOG(1) << "[0] java finish test ndarray";

    // 1. Test data frame
    {
      // auto selectors = gs::gs::Selector::ParseSelectors(s_selectors).value();
      std::unique_ptr<grape::InArchive> arc = std::move(
          vd_ctx_wrapper->ToDataframe(comm_spec, selectors, range).value());
      std::string java_data_frame_out_prefix = out_prefix + "/java";
      output_data_frame(comm_spec, std::move(arc), java_data_frame_out_prefix,
                        7);
    }

    VLOG(1) << "[1] java finish test dataframe";
    // 2. test vineyard tensor
    {
      auto tmp =
          vd_ctx_wrapper->ToVineyardTensor(comm_spec, client, selector, range);
      CHECK(tmp);
      vineyard::ObjectID ndarray_object = tmp.value();
      std::string java_v6d_tensor_prefix = out_prefix + "/java";
      vineyard::AnyType expected_data_type = vineyard::AnyType::Double;
      output_vineyard_tensor<double>(client, ndarray_object, comm_spec,
                                     java_v6d_tensor_prefix,
                                     expected_data_type);
    }
    VLOG(1) << "[2] java finish test vineyard tensor";
  } else {
    LOG(ERROR) << "Unrecognized ctx type: " << ctx_wrapper->context_type();
  }
}

void QueryProjected(vineyard::Client& client,
                    std::shared_ptr<ProjectedFragmentType> fragment,
                    const grape::CommSpec& comm_spec,
                    const std::string& app_name, const std::string& out_prefix,
                    const std::string& basic_params,
                    const std::string& selector_string,
                    const std::string& selectors_string) {
  using AppType = gs::JavaPIEProjectedDefaultApp<ProjectedFragmentType>;
  auto app = std::make_shared<AppType>();
  auto worker = AppType::CreateWorker(app, fragment);
  auto spec = grape::DefaultParallelEngineSpec();
  worker->Init(comm_spec, spec);
  std::string lib_path = "";
  worker->Query(basic_params, lib_path);
  std::ofstream ostream;
  std::string output_path =
      grape::GetResultFilename(out_prefix, fragment->fid());

  ostream.open(output_path);
  worker->Output(ostream);
  ostream.close();

  std::shared_ptr<gs::JavaPIEProjectedContext<ProjectedFragmentType>> ctx =
      worker->GetContext();
  worker->Finalize();

  gs::rpc::graph::GraphDefPb graph_def;
  graph_def.set_graph_type(gs::rpc::graph::ARROW_PROJECTED);

  auto selectors = gs::Selector::ParseSelectors(selectors_string).value();
  auto selector = gs::Selector::parse(selector_string).value();
  auto range = std::make_pair("", "");

  auto frag_wrapper =
      std::make_shared<gs::FragmentWrapper<ProjectedFragmentType>>(
          "graph_123", graph_def, fragment);

  auto ctx_wrapper = ctx->CreateInnerCtxWrapper(
      "ctx_wrapper_" + vineyard::random_string(8), frag_wrapper);
  if (ctx_wrapper->context_type() == "vertex_property") {
    auto vp_ctx_wrapper =
        std::dynamic_pointer_cast<gs::IVertexPropertyContextWrapper>(
            ctx_wrapper);
    /// 0. test ndarray
    {
      std::unique_ptr<grape::InArchive> arc = std::move(
          vp_ctx_wrapper->ToNdArray(comm_spec, selector, range).value());
      std::string java_out_prefix =
          out_prefix + "/java_projected_assembled_ndarray.dat";
      output_nd_array(comm_spec, std::move(arc), java_out_prefix,
                      4);  // 4 for int64_t
    }
    VLOG(1) << "[0] java projected finish test ndarray";

    // 1. Test data frame
    {
      // auto selectors = gs::gs::Selector::ParseSelectors(s_selectors).value();
      std::unique_ptr<grape::InArchive> arc = std::move(
          vp_ctx_wrapper->ToDataframe(comm_spec, selectors, range).value());
      std::string java_data_frame_out_prefix = out_prefix + "/java_projected";
      output_data_frame(comm_spec, std::move(arc), java_data_frame_out_prefix,
                        4);
    }

    VLOG(1) << "[1] java projected finish test dataframe";
    // 2. test vineyard tensor
    {
      auto tmp =
          vp_ctx_wrapper->ToVineyardTensor(comm_spec, client, selector, range);
      CHECK(tmp);
      vineyard::ObjectID ndarray_object = tmp.value();
      std::string java_v6d_tensor_prefix = out_prefix + "/java_projected";
      vineyard::AnyType expected_data_type = vineyard::AnyType::Int64;  // 4
      output_vineyard_tensor<int64_t>(client, ndarray_object, comm_spec,
                                      java_v6d_tensor_prefix,
                                      expected_data_type);
    }
    VLOG(1) << "[2] java projected finish test vineyard tensor";

  } else if (ctx_wrapper->context_type() == "vertex_data") {
    auto vd_ctx_wrapper =
        std::dynamic_pointer_cast<gs::IVertexDataContextWrapper>(ctx_wrapper);
    /// 0. test ndarray
    {
      std::unique_ptr<grape::InArchive> arc = std::move(
          vd_ctx_wrapper->ToNdArray(comm_spec, selector, range).value());
      std::string java_out_prefix =
          out_prefix + "/java_projected_assembled_ndarray.dat";
      output_nd_array(comm_spec, std::move(arc), java_out_prefix,
                      4);  // 4 for int64_t
    }
    VLOG(1) << "[0] java projected finish test ndarray";

    // 1. Test data frame
    {
      // auto selectors = gs::gs::Selector::ParseSelectors(s_selectors).value();
      std::unique_ptr<grape::InArchive> arc = std::move(
          vd_ctx_wrapper->ToDataframe(comm_spec, selectors, range).value());
      std::string java_data_frame_out_prefix = out_prefix + "/java_projected";
      output_data_frame(comm_spec, std::move(arc), java_data_frame_out_prefix,
                        4);
    }

    VLOG(1) << "[1] java projected finish test dataframe";
    // 2. test vineyard tensor
    {
      auto tmp =
          vd_ctx_wrapper->ToVineyardTensor(comm_spec, client, selector, range);
      CHECK(tmp);
      vineyard::ObjectID ndarray_object = tmp.value();
      std::string java_v6d_tensor_prefix = out_prefix + "/java_projected";
      vineyard::AnyType expected_data_type = vineyard::AnyType::Int64;  // 4
      output_vineyard_tensor<int64_t>(client, ndarray_object, comm_spec,
                                      java_v6d_tensor_prefix,
                                      expected_data_type);
    }
    VLOG(1) << "[2] java projected finish test vineyard tensor";

  } else {
    LOG(ERROR) << "Unrecognized ctx type: " << ctx_wrapper->context_type();
  }
  //  auto selector = gs::LabeledSelector::parse("r:label0.property0").value();
}

// Running test doesn't require codegen.
void Run(vineyard::Client& client, const grape::CommSpec& comm_spec,
         vineyard::ObjectID id, bool run_projected, bool run_property,
         const std::string& app_name) {
  std::shared_ptr<FragmentType> fragment =
      std::dynamic_pointer_cast<FragmentType>(client.GetObject(id));

  boost::property_tree::ptree pt;
  pt.put("src", "4");
  pt.put("app_class", app_name);
  if (getenv("USER_JAR_PATH")) {
    pt.put("jar_name", getenv("USER_JAR_PATH"));
  } else {
    LOG(ERROR) << "JAR_NAME not set";
    return;
  }

  if (!run_projected) {
    pt.put("frag_name", "gs::ArrowFragmentDefault<int64_t>");
    std::stringstream ss;
    boost::property_tree::json_parser::write_json(ss, pt);
    std::string basic_params = ss.str();
    VLOG(1) << "basic_params: " << basic_params;

    std::string selector_string;
    std::string selectors_string;
    if (run_property == 0) {
      // labeled_vetex_data
      selector_string = "r:label0";
      {
        std::vector<std::pair<std::string, std::string>> selector_list;
        selector_list.emplace_back("id", "v:label0.id");
        selector_list.emplace_back("result", "r:label0");
        selectors_string = gs::generate_selectors(selector_list);
      }
    } else {
      // labeled_vertex_property
      selector_string = "r:label0.dist_0";
      {
        std::vector<std::pair<std::string, std::string>> selector_list;
        selector_list.emplace_back("id", "v:label0.id");
        selector_list.emplace_back("result", "r:label0.dist_0");
        selectors_string = gs::generate_selectors(selector_list);
      }
    }
    VLOG(1) << "selector string: " << selector_string << ", selectors string "
            << selectors_string;
    // 1. run java query
    QueryProperty(client, fragment, comm_spec, app_name, "/tmp", basic_params,
                  selector_string, selectors_string);
  } else {  // 3. run projected
    pt.put("frag_name",
           "gs::ArrowProjectedFragment<int64_t,uint64_t,double,int64_t>");
    std::stringstream ss;
    boost::property_tree::json_parser::write_json(ss, pt);
    std::string basic_params = ss.str();
    VLOG(1) << "basic_params" << basic_params;
    VLOG(1) << "running projected";
    VLOG(1) << "vertex properties num: " << fragment->vertex_property_num(0);
    VLOG(1) << "edge properties num: " << fragment->edge_property_num(0);
    std::shared_ptr<ProjectedFragmentType> projected_fragment =
        ProjectedFragmentType::Project(fragment, "0", "0", "0", "2");
    // test get data
    using vertex_t = ProjectedFragmentType::vertex_t;
    vertex_t vertex;
    projected_fragment->GetInnerVertex(4, vertex);
    VLOG(1) << "source vertex" << vertex.GetValue();
    {
      std::string selector_string;
      std::string selectors_string;
      if (run_property == 0) {
        // vetex_data
        selector_string = "r";
        {
          std::vector<std::pair<std::string, std::string>> selector_list;
          selector_list.emplace_back("id", "v.id");
          selector_list.emplace_back("result", "r");
          selectors_string = gs::generate_selectors(selector_list);
        }
      } else {
        // vertex_property
        selector_string = "r.dist_0";
        {
          std::vector<std::pair<std::string, std::string>> selector_list;
          selector_list.emplace_back("id", "v.id");
          selector_list.emplace_back("result", "r.dist_0");
          selectors_string = gs::generate_selectors(selector_list);
        }
      }
      QueryProjected(client, projected_fragment, comm_spec, app_name, "/tmp",
                     basic_params, selector_string, selectors_string);
    }
  }
}

// Run projected or not: by passing value
// Run property or not:
int main(int argc, char** argv) {
  if (argc < 10) {
    printf(
        "usage: ./run_java_app <ipc_socket> <e_label_num> "
        "<efiles...> "
        "<v_label_num> <vfiles...> <run_projected> <run_property>"
        "[directed] [app_name]\n");
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

  int run_projected = atoi(argv[index++]);
  int run_property = atoi(argv[index++]);

  int directed = 1;
  std::string app_name = "";
  if (argc > index) {
    directed = atoi(argv[index++]);
  }
  if (argc > index) {
    app_name = argv[index++];
  }
  VLOG(1) << "app name " << app_name;

  grape::InitMPIComm();
  {
    grape::CommSpec comm_spec;
    comm_spec.Init(MPI_COMM_WORLD);

    vineyard::Client client;
    VINEYARD_CHECK_OK(client.Connect(ipc_socket));

    VLOG(1) << "Connected to IPCServer: " << ipc_socket;

    vineyard::ObjectID fragment_id;
    {
      auto loader = std::make_unique<
          gs::ArrowFragmentLoader<vineyard::property_graph_types::OID_TYPE,
                                  vineyard::property_graph_types::VID_TYPE>>(
          client, comm_spec, efiles, vfiles, directed != 0);
      fragment_id = boost::leaf::try_handle_all(
          [&loader]() { return loader->LoadFragment(); },
          [](const vineyard::GSError& e) {
            LOG(ERROR) << e.error_msg;
            return 0;
          },
          [](const boost::leaf::error_info& unmatched) {
            LOG(ERROR) << "Unmatched error " << unmatched;
            return 0;
          });
    }

    VLOG(1) << "[worker-" << comm_spec.worker_id()
            << "] loaded graph to vineyard ...";

    MPI_Barrier(comm_spec.comm());

    Run(client, comm_spec, fragment_id, run_projected, run_property, app_name);
    MPI_Barrier(comm_spec.comm());
  }

  grape::FinalizeMPIComm();
  return 0;
}
