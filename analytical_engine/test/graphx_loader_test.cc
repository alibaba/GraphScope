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

#include "core/config.h"
#include "core/fragment/arrow_projected_fragment.h"
#include "core/fragment/arrow_projected_fragment_mapper.h"
#include "core/java/fragment_getter.h"
#include "core/java/graphx_loader.h"
#include "core/java/graphx_raw_data.h"
#include "core/loader/arrow_fragment_loader.h"

std::string getHostName() { return boost::asio::ip::host_name(); }

void initVerticesEdges(gs::fid_t fid, gs::fid_t fnum, int32_t vnums,
                       std::vector<int64_t>& oids,
                       std::vector<int64_t>& src_oids,
                       std::vector<int64_t>& dst_oids) {
  for (gs::fid_t i = fid; i < fnum * vnums; i += fnum) {
    oids.push_back(i);
  }
  for (gs::fid_t i = fid; i < fnum * (vnums - 1); i += fnum) {
    src_oids.push_back(i);
    dst_oids.push_back(i + 1);
  }
}

void initLongData(std::vector<int64_t>& result, int cnt) {
  for (int i = 0; i < cnt; ++i) {
    result.push_back(i);
  }
}
void initStringData(std::vector<char>& result, std::vector<int32_t>& offset,
                    int cnt) {
  offset.resize(cnt);
  for (int i = 0; i < cnt; ++i) {
    offset[i] = 4;
  }
  int ind = 0;
  result.resize(4 * cnt);
  for (int i = 0; i < cnt; ++i) {
    for (int j = 0; j < 4; ++j) {
      result[ind] = j;
      ind += 1;
    }
  }
}

int main(int argc, char** argv) {
  if (argc != 2) {
    printf("usage: ./graphx_loader_test <ipc_socket>\n");
    return 1;
  }
  int index = 1;
  std::string ipc_socket = std::string(argv[index++]);

  vineyard::Client client;
  VINEYARD_CHECK_OK(client.Connect(ipc_socket));

  LOG(INFO) << "Connected to IPCServer: " << ipc_socket;

  grape::InitMPIComm();

  int vertices_num = 5;
  int edges_num = 4;
  vineyard::ObjectID arrow_frag_id;
  grape::CommSpec comm_spec;
  comm_spec.Init(MPI_COMM_WORLD);
  gs::FakePartitioner<int64_t> partitioner;
  std::vector<int> pid2Fid;
  for (int i = 0; i < comm_spec.fnum(); ++i) {
    pid2Fid.push_back(i);
  }
  partitioner.Init(pid2Fid);
  {
    std::vector<int64_t> oids, vdatas, src_oids, dst_oids, edatas;
    initVerticesEdges(comm_spec.fid(), comm_spec.fnum(), vertices_num, oids,
                      src_oids, dst_oids);
    initLongData(vdatas, vertices_num);
    initLongData(edatas, edges_num);
    LOG(INFO) << "Finish init data";
    gs::GraphXRawDataBuilder<int64_t, uint64_t, int64_t, int64_t> builder(
        client, oids, vdatas, src_oids, dst_oids, edatas);
    auto res = builder.MySeal(client);
    LOG(INFO) << "Built raw data: " << res->id()
              << ", edge num: " << res->GetEdgeNum()
              << ", vertex num: " << res->GetVertexNum();

    gs::GraphXLoader<int64_t, uint64_t, int64_t, int64_t> loader(
        res->id(), client, comm_spec, partitioner);
    arrow_frag_id = boost::leaf::try_handle_all(
        [&loader]() { return loader.LoadFragment(); },
        [](const vineyard::GSError& e) {
          LOG(FATAL) << e.error_msg;
          return 0;
        },
        [](const boost::leaf::error_info& unmatched) {
          LOG(FATAL) << "Unmatched error " << unmatched;
          return 0;
        });
  }
  LOG(INFO) << "Got pritive arrow fragment id: " << arrow_frag_id;
  {
    std::vector<int64_t> oids, src_oids, dst_oids;
    std::vector<char> vdata_buffer, edata_buffer;
    std::vector<int32_t> vdata_offset, edata_offset;
    initVerticesEdges(comm_spec.fid(), comm_spec.fnum(), 5, oids, src_oids,
                      dst_oids);
    initStringData(vdata_buffer, vdata_offset, vertices_num);
    initStringData(edata_buffer, edata_offset, edges_num);
    LOG(INFO) << "Finish init data";
    gs::GraphXRawDataBuilder<int64_t, uint64_t, std::string, std::string>
        builder(client, oids, vdata_buffer, vdata_offset, src_oids, dst_oids,
                edata_buffer, edata_offset);
    auto res = builder.MySeal(client);
    LOG(INFO) << "Built raw data: " << res->id()
              << ", edge num: " << res->GetEdgeNum()
              << ", vertex num: " << res->GetVertexNum();

    gs::GraphXLoader<int64_t, uint64_t, std::string, std::string> loader(
        res->id(), client, comm_spec, partitioner);
    arrow_frag_id = boost::leaf::try_handle_all(
        [&loader]() { return loader.LoadFragment(); },
        [](const vineyard::GSError& e) {
          LOG(FATAL) << e.error_msg;
          return 0;
        },
        [](const boost::leaf::error_info& unmatched) {
          LOG(FATAL) << "Unmatched error " << unmatched;
          return 0;
        });
  }
  LOG(INFO) << "Got string arrow fragment id: " << arrow_frag_id;

  grape::FinalizeMPIComm();
  return 0;
}
