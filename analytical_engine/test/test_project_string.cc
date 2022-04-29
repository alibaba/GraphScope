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

#include "glog/logging.h"

#include "grape/analytical_apps/bfs/bfs.h"
#include "grape/grape.h"
#include "vineyard/client/client.h"
#include "vineyard/graph/fragment/arrow_fragment.h"

#include "core/fragment/arrow_projected_fragment.h"
#include "core/loader/arrow_fragment_loader.h"

#define OID_TYPE int64_t
#define VID_TYPE uint64_t

template class vineyard::BasicArrowVertexMapBuilder<OID_TYPE, VID_TYPE>;
template class vineyard::ArrowVertexMap<OID_TYPE, VID_TYPE>;
template class vineyard::ArrowVertexMapBuilder<OID_TYPE, VID_TYPE>;
template class gs::ArrowProjectedVertexMap<OID_TYPE, VID_TYPE>;
template class gs::ArrowProjectedFragment<OID_TYPE, VID_TYPE, std::string,
                                          std::string>;

void traverse_fragment(std::shared_ptr<gs::ArrowProjectedFragment<
                           OID_TYPE, VID_TYPE, std::string, std::string>>
                           fragment,
                       const std::string prefix) {
  std::string vfile_path =
      prefix + "_frag_" + std::to_string(fragment->fid()) + ".v";
  FILE* vfile = fopen(vfile_path.c_str(), "wb");

  auto inner_vertices = fragment->InnerVertices();
  for (auto v : inner_vertices) {
    auto id = fragment->GetId(v);
    std::string data = std::string(fragment->GetData(v));
    fprintf(vfile, "%ld|%s\n", id, data.c_str());
  }
  fflush(vfile);
  fclose(vfile);

  std::string oefile_path =
      prefix + "_frag_" + std::to_string(fragment->fid()) + ".oe";
  FILE* oefile = fopen(oefile_path.c_str(), "wb");
  for (auto v : inner_vertices) {
    auto oe = fragment->GetOutgoingAdjList(v);
    for (auto& e : oe) {
      auto u = e.neighbor();
      auto val = std::string(e.data());
      fprintf(oefile, "%ld|%ld|%s\n", fragment->GetId(v), fragment->GetId(u),
              val.c_str());
    }
  }
  fflush(oefile);
  fclose(oefile);

  std::string iefile_path =
      prefix + "_frag_" + std::to_string(fragment->fid()) + ".ie";
  FILE* iefile = fopen(iefile_path.c_str(), "wb");
  for (auto v : inner_vertices) {
    auto ie = fragment->GetIncomingAdjList(v);
    for (auto& e : ie) {
      auto u = e.neighbor();
      auto val = std::string(e.data());
      fprintf(iefile, "%ld|%ld|%s\n", fragment->GetId(u), fragment->GetId(v),
              val.c_str());
    }
  }
  fflush(iefile);
  fclose(iefile);
}

int main(int argc, char** argv) {
  if (argc < 6) {
    printf(
        "usage: ./test_project_string <e_label_num> <efile...> "
        "<v_label_num> <vfiles...> "
        "[directed]\n");
    return 1;
  }
  int index = 1;
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
  grape::CommSpec comm_spec;
  comm_spec.Init(MPI_COMM_WORLD);

  vineyard::Client& client = vineyard::Client::Default();

  using oid_t = OID_TYPE;
  using vid_t = VID_TYPE;

  auto loader = std::make_unique<vineyard::ArrowFragmentLoader<oid_t, vid_t>>(
      client, comm_spec, efiles, vfiles, directed != 0);

  int exit_code = boost::leaf::try_handle_all(
      [&]() -> boost::leaf::result<int> {
        BOOST_LEAF_AUTO(obj_id, loader->LoadFragment());
        LOG(INFO) << "got fragment: " << obj_id;

        using FragmentType = vineyard::ArrowFragment<oid_t, vid_t>;
        using ProjectedFragmentType =
            gs::ArrowProjectedFragment<oid_t, vid_t, std::string, std::string>;

        std::shared_ptr<FragmentType> fragment =
            std::dynamic_pointer_cast<FragmentType>(client.GetObject(obj_id));
        LOG(INFO) << "got property fragment-" << fragment->fid();
        std::shared_ptr<ProjectedFragmentType> projected_fragment =
            ProjectedFragmentType::Project(fragment, 2, 0, 0, 2);

        LOG(INFO) << "got fragment-" << projected_fragment->fid();

        traverse_fragment(projected_fragment, "./traverse");

        MPI_Barrier(comm_spec.comm());

        return 0;
      },
      [](const vineyard::GSError& error) {
        std::cerr << error.error_msg;
        return 1;
      },
      [](const boost::leaf::error_info& e) { return 1; });

  return exit_code;
}
