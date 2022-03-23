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
#include <string>

#include <boost/asio.hpp>
#include "glog/logging.h"

#include "grape/grape.h"
#include "grape/util.h"
#include "vineyard/client/client.h"
#include "vineyard/graph/fragment/arrow_fragment.h"

#include "core/fragment/arrow_projected_fragment.h"
#include "core/fragment/arrow_projected_fragment_mapper.h"
#include "core/java/fragment_getter.h"
#include "core/loader/arrow_fragment_loader.h"

std::string getHostName() { return boost::asio::ip::host_name(); }

int main(int argc, char** argv) {
  if (argc < 6) {
    printf(
        "usage: ./projected_fragment_mapper_test <ipc_socket> <e_label_num> "
        "<efiles...> "
        "<v_label_num> <vfiles...>\n");
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

    {
      using FragmentType =
          vineyard::ArrowFragment<vineyard::property_graph_types::OID_TYPE,
                                  vineyard::property_graph_types::VID_TYPE>;
      using ProjectedFragmentType =
          gs::ArrowProjectedFragment<int64_t, uint64_t, double, int64_t>;

      LOG(INFO) << "[worker-" << comm_spec.worker_id()
                << "] loaded graph to vineyard ..." << fragment_id;
      MPI_Barrier(comm_spec.comm());
      auto fragment = std::dynamic_pointer_cast<FragmentType>(
          client.GetObject(fragment_id));
      LOG(INFO) << "vertex prop num:" << fragment->vertex_property_num(0);
      LOG(INFO) << "edge prop num:" << fragment->edge_property_num(0);
      std::shared_ptr<ProjectedFragmentType> projected_fragment =
          ProjectedFragmentType::Project(fragment, 0, 0, 0, 0);
      LOG(INFO) << "After projection: " << getHostName() << ":"
                << projected_fragment->id();

      LOG(INFO) << "ivnum: " << projected_fragment->GetInnerVerticesNum()
                << ",enum: " << projected_fragment->GetOutEdgeNum();
      {
        gs::ArrowProjectedFragmentMapper<int64_t, uint64_t, int64_t, double>
            mapper;
        arrow::Int64Builder vdata_builder;
        arrow::DoubleBuilder edata_builder;
        vdata_builder.Reserve(projected_fragment->GetInnerVerticesNum());
        auto ivnum = projected_fragment->GetInnerVerticesNum();
        for (size_t i = 0; i < ivnum; ++i) {
          vdata_builder.UnsafeAppend(static_cast<int64_t>(i));
        }
        auto edata_array = projected_fragment->get_edata_array_accessor();
        size_t edge_num = edata_array.GetLength();
        edata_builder.Reserve(edge_num);
        for (size_t i = 0; i < edge_num; ++i) {
          edata_builder.UnsafeAppend(static_cast<double>(i));
        }
        auto mapped_fragment =
            mapper.Map(projected_fragment->get_arrow_fragment(),
                       projected_fragment->vertex_label(),
                       projected_fragment->edge_label(), vdata_builder,
                       edata_builder, client);
        LOG(INFO) << "Got mapped fragment " << mapped_fragment->id();
        grape::Vertex<uint64_t> vertex;
        vertex.SetValue(10);
        LOG(INFO) << "new data: " << mapped_fragment->GetData(vertex);
      }
      {
        gs::ArrowProjectedFragmentMapper<int64_t, uint64_t, int64_t, int64_t>
            mapper;
        arrow::Int64Builder vdata_builder;
        auto ivnum = projected_fragment->GetInnerVerticesNum();
        vdata_builder.Reserve(ivnum);
        for (size_t i = 0; i < ivnum; ++i) {
          vdata_builder.UnsafeAppend(static_cast<int64_t>(i));
        }
        auto mapped_fragment = mapper.Map(
            projected_fragment->get_arrow_fragment(),
            projected_fragment->vertex_label(),
            projected_fragment->edge_prop_id(), vdata_builder, client);
        LOG(INFO) << "Got mapped fragment " << mapped_fragment->id();
        grape::Vertex<uint64_t> vertex;
        vertex.SetValue(10);
        LOG(INFO) << "new data: " << mapped_fragment->GetData(vertex);
      }
    }

    MPI_Barrier(comm_spec.comm());
  }

  grape::FinalizeMPIComm();
  return 0;
}
