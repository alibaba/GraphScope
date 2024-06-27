#include "core/applications.h"

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

#include <gflags/gflags.h>
#include <gflags/gflags_declare.h>
#include <glog/logging.h>
#include <sys/stat.h>
#include <algorithm>
#include <cstdint>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

#include "grape/config.h"
#include "grape/fragment/immutable_edgecut_fragment.h"
#include "grape/fragment/loader.h"
#include "grape/grape.h"

#include "bfs/bfs_auto.h"
#include "bfs/bfs_opt.h"
#include "cdlp/cdlp.h"
#include "cdlp/cdlp_auto.h"
#include "cdlp/cdlp_opt.h"
#include "lcc/lcc_auto.h"
#include "lcc/lcc_opt.h"
#include "pagerank/pagerank_auto.h"
#include "pagerank/pagerank_directed.h"
#include "pagerank/pagerank_opt.h"
#include "sssp/sssp_auto.h"
#include "sssp/sssp_opt.h"
#include "voterank/voterank.h"
#include "wcc/wcc.h"
#include "wcc/wcc_auto.h"
#include "wcc/wcc_opt.h"

#include "apps/bfs/bfs_generic.h"
#include "apps/centrality/degree/degree_centrality.h"
// #include "apps/centrality/eigenvector/eigenvector_centrality.h"
// #include "apps/centrality/katz/katz_centrality.h"
#include "apps/clustering/avg_clustering.h"
#include "apps/clustering/clustering.h"
#include "apps/clustering/transitivity.h"
#include "apps/clustering/triangles.h"
#include "apps/dfs/dfs.h"
#include "apps/hits/hits.h"
#include "apps/kcore/kcore.h"
#include "apps/kshell/kshell.h"
#include "apps/sssp/sssp_average_length.h"
#include "apps/sssp/sssp_has_path.h"
#include "apps/sssp/sssp_path.h"
#include "core/flags.h"
#include "core/fragment/dynamic_fragment.h"

#include "apps/lpa/lpa_u2i.h"
#include "apps/property/auto_sssp_property.h"
#include "apps/property/auto_wcc_property.h"
#include "apps/property/sssp_property.h"
#include "apps/property/wcc_property.h"
#include "apps/sampling_path/sampling_path.h"

namespace bl = boost::leaf;

using oid_t = vineyard::property_graph_types::OID_TYPE;
using vid_t = vineyard::property_graph_types::VID_TYPE;

using FragmentType = vineyard::ArrowFragment<oid_t, vid_t>;
    

namespace gs {

template <typename FRAG_T>
std::shared_ptr<FRAG_T> LoadSimpleGraph(const std::string& efile,
                                        const std::string& vfile,
                                        const grape::CommSpec& comm_spec) {
  grape::LoadGraphSpec graph_spec = grape::DefaultLoadGraphSpec();
  graph_spec.set_directed(FLAGS_directed);
  std::shared_ptr<FRAG_T> fragment =
      grape::LoadGraph<FRAG_T>(efile, vfile, comm_spec, graph_spec);
  return fragment;
}

template <typename OID_T, typename VID_T>
vineyard::ObjectID LoadPropertyGraph(const grape::CommSpec& comm_spec,
                                     vineyard::Client& client,
                                     const std::vector<std::string>& efiles,
                                     const std::vector<std::string>& vfiles,
                                     int directed) {
  vineyard::ObjectID fragment_id = vineyard::InvalidObjectID();
  {
    auto loader = std::make_unique<gs::ArrowFragmentLoader<OID_T, VID_T>>(
        client, comm_spec, efiles, vfiles, directed != 0,
        /* generate_eid */ false, /* retain_oid */ false);
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
            << "] loaded graph to vineyard ... " << fragment_id;
  LOG(INFO) << "peek memory: " << vineyard::get_peak_rss_pretty() << std::endl;

  MPI_Barrier(comm_spec.comm());
  return fragment_id;
}

template <typename FRAG_T, typename PROJECT_FRAG_T>
std::shared_ptr<PROJECT_FRAG_T> ProjectGraph(std::shared_ptr<FRAG_T> fragment,
                                             int v_label = 0, int v_prop = -1,
                                             int e_label = 0, int e_prop = -1) {
  // v_prop is grape::EmptyType, e_prop is grape::EmptyType
  LOG(INFO) << "start project ... memory = " << vineyard::get_rss_pretty()
            << ", peak = " << vineyard::get_peak_rss_pretty();
  auto projected_fragment =
      PROJECT_FRAG_T::Project(fragment, v_label, v_prop, e_label, e_prop);
  LOG(INFO) << "finish project ... memory = " << vineyard::get_rss_pretty()
            << ", peak = " << vineyard::get_peak_rss_pretty();
  return projected_fragment;
}

template <typename FRAG_T, typename APP_T, typename... Args>
void DoQuery(const grape::CommSpec& comm_spec, std::shared_ptr<FRAG_T> fragment,
             const std::string& out_prefix, Args... args) {
  auto app = std::make_shared<APP_T>();
  auto worker = APP_T::CreateWorker(app, fragment);
  auto spec = grape::DefaultParallelEngineSpec();
  worker->Init(comm_spec, spec);
  worker->Query(std::forward<Args>(args)...);

  std::string output_path =
      grape::GetResultFilename(out_prefix, fragment->fid());

  std::ofstream ostream;
  ostream.open(output_path);
  worker->Output(ostream);
  ostream.close();

  worker->Finalize();
  VLOG(1) << "Worker-" << comm_spec.worker_id() << " finished";
  LOG(INFO) << "finish running application ... memory = "
            << vineyard::get_rss_pretty()
            << ", peak = " << vineyard::get_peak_rss_pretty();
}

template <typename FRAG_T>
void RunPropertyApp(std::shared_ptr<FRAG_T> fragment,
                    const grape::CommSpec& comm_spec,
                    const std::string& out_prefix, const std::string& name) {
  if (name == "wcc_property") {
    using AppType = gs::WCCProperty<FRAG_T>;
    DoQuery<FRAG_T, AppType>(comm_spec, fragment, out_prefix);
  } else if (name == "sssp_property") {
    using AppType = gs::SSSPProperty<FRAG_T>;
    DoQuery<FRAG_T, AppType, oid_t>(comm_spec, fragment, out_prefix,
                                          FLAGS_sssp_source);
  } else if (name == "wcc_auto_property") {
    using AppType = gs::AutoWCCProperty<FRAG_T>;
    DoQuery<FRAG_T, AppType>(comm_spec, fragment, out_prefix);
  } else if (name == "sssp_auto_property") {
    using AppType = gs::AutoSSSPProperty<FRAG_T>;
    DoQuery<FRAG_T, AppType, oid_t>(comm_spec, fragment, out_prefix,
                                          FLAGS_sssp_source);
  } else if (name == "lpa_u2i_property") {
    using AppType = gs::LPAU2I<FRAG_T>;
    DoQuery<FRAG_T, AppType>(comm_spec, fragment, out_prefix);
  }
}



template <typename FRAG_T>
void RunProjectedApp(std::shared_ptr<FRAG_T> fragment,
                     const grape::CommSpec& comm_spec,
                     const std::string& out_prefix, const std::string& name) {

  if (name == "sssp_projected") {
    using PROJECTED_FRAG_T = gs::ArrowProjectedFragment<oid_t, vid_t, grape::EmptyType, int64_t>;
    auto projected = ProjectGraph<FRAG_T, PROJECTED_FRAG_T>(
        fragment, 0, -1, 0, 2);
    using AppType = grape::SSSPOpt<PROJECTED_FRAG_T>;
    DoQuery<PROJECTED_FRAG_T, AppType, oid_t>(
        comm_spec, projected, out_prefix, FLAGS_sssp_source);
  } else {
    using PROJECTED_FRAG_T = gs::ArrowProjectedFragment<oid_t, vid_t, grape::EmptyType,
                               grape::EmptyType>;
    auto projected = ProjectGraph<FRAG_T, PROJECTED_FRAG_T>(
        fragment, 0, -1, 0, -1);
    if (name == "wcc_projected") {
      using AppType = grape::WCCOpt<PROJECTED_FRAG_T>;
      DoQuery<PROJECTED_FRAG_T, AppType>(comm_spec, projected, out_prefix);
    } else if (name == "cdlp_projected") {
      // TODO(siyuan): uncomment once latest libgrape-lite is released.
      using AppType = grape::CDLPOpt<PROJECTED_FRAG_T, int64_t>;
      DoQuery<PROJECTED_FRAG_T, AppType, int>(comm_spec, projected,
                                                   out_prefix, FLAGS_max_round);
    } else if (name == "bfs_projected") {
      using AppType = grape::BFSOpt<PROJECTED_FRAG_T>;
      DoQuery<PROJECTED_FRAG_T, AppType, oid_t>(
          comm_spec, projected, out_prefix, FLAGS_bfs_source);
    } else if (name == "lcc_projected") {
      using AppType = grape::LCCOpt<PROJECTED_FRAG_T>;
      DoQuery<PROJECTED_FRAG_T, AppType>(comm_spec, projected, out_prefix);
    } else if (name == "pagerank_projected") {
      using AppType = grape::PageRankOpt<PROJECTED_FRAG_T>;
      DoQuery<PROJECTED_FRAG_T, AppType, double, int>(
          comm_spec, projected, out_prefix, FLAGS_pagerank_delta,
          FLAGS_max_round);
    } else if (name == "wcc_auto_projected") {
      using AppType = grape::WCCAuto<PROJECTED_FRAG_T>;
      DoQuery<PROJECTED_FRAG_T, AppType>(comm_spec, projected, out_prefix);
    }
  }
}

/**
    * @brief Run application in batch mode
    * @example ./grape_engine -batch_mode -vineyard_socket /tmp/vineyard.sock -efile p2p-31.e#label=e#src_label=v#dst_label=v#delimiter=' ' -vfile p2p-31.v#label=v#delimiter=' ' -application wcc -out_prefix ret
 */
void RunApp() {
  std::string ipc_socket = FLAGS_vineyard_socket;

  std::vector<std::string> efiles;
  boost::split(efiles, FLAGS_efile, boost::is_any_of(","));

  std::vector<std::string> vfiles;
  boost::split(vfiles, FLAGS_vfile, boost::is_any_of(","));

  bool directed = FLAGS_directed;
  std::string app_name = FLAGS_application;

  std::vector<std::string> available_apps = {
      "wcc_property", "sssp_property", "wcc_auto_property", "sssp_auto_property",
      "lpa_u2i_property", "wcc_projected", "cdlp_projected", "bfs_projected",
      "lcc_projected", "pagerank_projected", "wcc_auto_projected", "sssp_projected"};
      
if (std::find(available_apps.begin(), available_apps.end(), app_name) ==
      available_apps.end()) {
    LOG(FATAL) << "Application " << app_name << " is not supported.";
  }
  {
    grape::CommSpec comm_spec;
    comm_spec.Init(MPI_COMM_WORLD);

    vineyard::Client client;
    VINEYARD_CHECK_OK(client.Connect(ipc_socket));
    LOG(INFO) << "Connected to IPCServer: " << ipc_socket;

    auto fragment_id =
        LoadPropertyGraph(comm_spec, client, efiles, vfiles, directed);

    std::shared_ptr<FragmentType> fragment =
        std::dynamic_pointer_cast<FragmentType>(client.GetObject(fragment_id));

    RunPropertyApp(fragment, comm_spec, FLAGS_out_prefix, FLAGS_application);
    RunProjectedApp(fragment, comm_spec, FLAGS_out_prefix, FLAGS_application);
    MPI_Barrier(comm_spec.comm());
  }
}

}  // namespace gs

template class gs::ArrowProjectedFragment<int64_t, uint64_t, grape::EmptyType,
                                          grape::EmptyType>;
template class gs::ArrowProjectedFragment<int64_t, uint64_t, grape::EmptyType,
                                          int64_t>;
