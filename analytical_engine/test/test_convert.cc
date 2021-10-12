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

#include "grape/analytical_apps/wcc/wcc_auto.h"
#include "grape/grape.h"
#include "vineyard/client/client.h"
#include "vineyard/graph/fragment/arrow_fragment.h"

#include "core/fragment/arrow_projected_fragment.h"
#include "core/fragment/dynamic_fragment.h"
#include "core/loader/arrow_fragment_loader.h"
#include "core/loader/arrow_to_dynamic_converter.h"
#include "core/loader/dynamic_to_arrow_converter.h"

#define OID_TYPE int64_t
#define VID_TYPE uint64_t

template class vineyard::BasicArrowVertexMapBuilder<OID_TYPE, VID_TYPE>;
template class vineyard::ArrowVertexMap<OID_TYPE, VID_TYPE>;
template class vineyard::ArrowVertexMapBuilder<OID_TYPE, VID_TYPE>;
template class gs::ArrowProjectedVertexMap<OID_TYPE, VID_TYPE>;

template class gs::ArrowProjectedFragment<OID_TYPE, VID_TYPE, int64_t, int64_t>;

int main(int argc, char** argv) {
  if (argc < 6) {
    printf(
        "usage: ./test_convert <ipc_socket> <e_label_num> <efiles...> "
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

  int exit_code;

  grape::InitMPIComm();
  {
    grape::CommSpec comm_spec;
    comm_spec.Init(MPI_COMM_WORLD);

    vineyard::Client client;
    VINEYARD_CHECK_OK(client.Connect(ipc_socket));

    using oid_t = OID_TYPE;
    using vid_t = VID_TYPE;
    using FragmentType = vineyard::ArrowFragment<oid_t, vid_t>;

    gs::ArrowFragmentLoader<oid_t, vid_t> loader(client, comm_spec, efiles,
                                                 vfiles, directed != 0);

    exit_code = boost::leaf::try_handle_all(
        [&]() -> boost::leaf::result<int> {
          BOOST_LEAF_AUTO(obj_id, loader.LoadFragment());
          auto arrow_frag =
              std::dynamic_pointer_cast<FragmentType>(client.GetObject(obj_id));
          gs::ArrowToDynamicConverter<FragmentType> a2d_converter(comm_spec, 0);

          BOOST_LEAF_AUTO(dynamic_frag, a2d_converter.Convert(arrow_frag));
          LOG(INFO) << "ArrowFragment->DynamicFragment done.";
          gs::DynamicToArrowConverter<oid_t> d2a_converter(comm_spec, client);
          BOOST_LEAF_AUTO(arrow_frag1, d2a_converter.Convert(dynamic_frag));
          LOG(INFO) << "DynamicFragment->ArrowFragment done.";
          using ProjectedFragmentType =
              gs::ArrowProjectedFragment<OID_TYPE, VID_TYPE, int64_t, int64_t>;
          auto projected_frag =
              ProjectedFragmentType::Project(arrow_frag1, "0", "0", "0", "0");
          using AppType = grape::WCCAuto<ProjectedFragmentType>;
          auto app = std::make_shared<AppType>();
          auto worker = AppType::CreateWorker(app, projected_frag);
          auto spec = grape::DefaultParallelEngineSpec();

          worker->Init(comm_spec, spec);

          worker->Query();
          return 0;
        },
        [](const vineyard::GSError& e) {
          LOG(ERROR) << e.error_msg;
          return 1;
        },
        [](const boost::leaf::error_info& unmatched) {
          LOG(ERROR) << "Unmatched error " << unmatched;
          return 1;
        });
  }
  grape::FinalizeMPIComm();
  return exit_code;
}
