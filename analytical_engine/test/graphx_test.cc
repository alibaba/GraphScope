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

#include <memory>
#include <string>
#include <utility>

#include "arrow/array.h"
#include "arrow/array/builder_primitive.h"
#include "glog/logging.h"

#include "vineyard/client/client.h"

#include "core/java/graphx/edge_data.h"
#include "core/java/graphx/graphx_csr.h"
#include "core/java/graphx/graphx_fragment.h"
#include "core/java/graphx/local_vertex_map.h"
#include "core/java/graphx/vertex_data.h"

boost::leaf::result<void> generateData(arrow::Int64Builder& srcBuilder,
                                       arrow::Int64Builder& dstBuilder,
                                       arrow::Int64Builder& edataBuilder,
                                       grape::CommSpec& comm_spec);

vineyard::ObjectID getLocalVM(vineyard::Client& client,
                              grape::CommSpec& comm_spec) {
  vineyard::ObjectID vmap_id;
  {
    arrow::Int64Builder inner, outer;
    arrow::Int32Builder pid;
    CHECK(inner.Reserve(3).ok());
    CHECK(outer.Reserve(3).ok());
    CHECK(pid.Reserve(2).ok());
    pid.UnsafeAppend(0);
    pid.UnsafeAppend(1);
    pid.UnsafeAppend(0);
    pid.UnsafeAppend(1);
    if (comm_spec.worker_id() == 1) {
      inner.UnsafeAppend(2);
      inner.UnsafeAppend(4);
      inner.UnsafeAppend(6);
      outer.UnsafeAppend(1);
      outer.UnsafeAppend(3);
      outer.UnsafeAppend(5);
      gs::BasicLocalVertexMapBuilder<int64_t, uint64_t> builder(client, inner,
                                                                outer, pid);
      auto vmap =
          std::dynamic_pointer_cast<gs::LocalVertexMap<int64_t, uint64_t>>(
              builder.Seal(client));

      VINEYARD_CHECK_OK(client.Persist(vmap->id()));
      vmap_id = vmap->id();
      LOG(INFO) << "Worker [" << comm_spec.worker_id()
                << "Persist local vmap id: " << vmap->id();
    } else {
      inner.UnsafeAppend(1);
      inner.UnsafeAppend(3);
      inner.UnsafeAppend(5);
      outer.UnsafeAppend(2);
      outer.UnsafeAppend(4);
      outer.UnsafeAppend(6);
      gs::BasicLocalVertexMapBuilder<int64_t, uint64_t> builder(client, inner,
                                                                outer, pid);
      auto vmap =
          std::dynamic_pointer_cast<gs::LocalVertexMap<int64_t, uint64_t>>(
              builder.Seal(client));

      VINEYARD_CHECK_OK(client.Persist(vmap->id()));
      vmap_id = vmap->id();
      LOG(INFO) << "Worker [" << comm_spec.worker_id()
                << "Persist local vmap id: " << vmap->id();
    }
  }
  return vmap_id;
}
gs::GraphXVertexMap<int64_t, uint64_t> TestGraphXVertexMap(
    vineyard::Client& client) {
  grape::CommSpec comm_spec;
  comm_spec.Init(MPI_COMM_WORLD);

  if (comm_spec.worker_num() != 2) {
    LOG(FATAL) << "Expect worker num == 2";
  }
  vineyard::ObjectID vm_id;
  {
    vineyard::ObjectID partial_map = getLocalVM(client, comm_spec);
    LOG(INFO) << "Worker: " << comm_spec.worker_id()
              << " local vm: " << partial_map;
    gs::BasicGraphXVertexMapBuilder<int64_t, uint64_t> builder(
        client, comm_spec, comm_spec.worker_num() - comm_spec.worker_id() - 1,
        partial_map);
    auto graphx_vm =
        std::dynamic_pointer_cast<gs::GraphXVertexMap<int64_t, uint64_t>>(
            builder.Seal(client));

    VINEYARD_CHECK_OK(client.Persist(graphx_vm->id()));
    vm_id = graphx_vm->id();
    LOG(INFO) << "Persist csr id: " << graphx_vm->id();
  }
  std::shared_ptr<gs::GraphXVertexMap<int64_t, uint64_t>> vm =
      std::dynamic_pointer_cast<gs::GraphXVertexMap<int64_t, uint64_t>>(
          client.GetObject(vm_id));
  LOG(INFO) << "worker " << comm_spec.worker_id() << " Got graphx vm "
            << vm->id();
  LOG(INFO) << "worker " << comm_spec.worker_id()
            << " total vnum: " << vm->GetTotalVertexSize();
  uint64_t gid;
  for (int64_t i = 1; i <= 6; ++i) {
    vm->GetGid(i, gid);
    LOG(INFO) << "worker " << comm_spec.worker_id() << "oid " << i << "gid "
              << gid;
  }
  return *vm;
}

vineyard::ObjectID TestGraphXCSR(
    vineyard::Client& client, gs::GraphXVertexMap<int64_t, uint64_t>& graphx_vm,
    arrow::Int64Builder& srcBuilder, arrow::Int64Builder& dstBuilder) {
  grape::CommSpec comm_spec;
  comm_spec.Init(MPI_COMM_WORLD);

  vineyard::ObjectID csr_id;
  {
    gs::BasicGraphXCSRBuilder<int64_t, uint64_t> builder(client);
    std::vector<int64_t> src{1, 2, 3, 4};
    std::vector<int64_t> dst{2, 3, 4, 5};
    builder.LoadEdges(src, dst, graphx_vm, comm_spec.local_num());
    auto csr = std::dynamic_pointer_cast<gs::GraphXCSR<uint64_t>>(
        builder.Seal(client));

    // VINEYARD_CHECK_OK(client.Persist(csr->id()));
    auto tmp_id = csr->id();
    LOG(INFO) << "Persist csr id: " << csr->id();
    std::shared_ptr<gs::GraphXCSR<uint64_t>> csr_ =
        std::dynamic_pointer_cast<gs::GraphXCSR<uint64_t>>(
            client.GetObject(tmp_id));
    LOG(INFO) << "Got csr " << csr_->id();
    csr_id = csr_->id();
  }
  std::shared_ptr<gs::GraphXCSR<uint64_t>> csr =
      std::dynamic_pointer_cast<gs::GraphXCSR<uint64_t>>(
          client.GetObject(csr_id));
  LOG(INFO) << "Got csr " << csr->id();
  LOG(INFO) << "in num edges: " << csr->GetInEdgesNum()
            << "out num edges: " << csr->GetOutEdgesNum() << " vs "
            << csr->GetPartialOutEdgesNum(
                   0, graphx_vm.GetInnerVertexSize(comm_spec.fid()));
  LOG(INFO) << "lid 0 degreee: " << csr->GetOutDegree(0) << ", "
            << csr->GetPartialOutEdgesNum(0, 1);
  return csr->id();
}

vineyard::ObjectID TestGraphXVertexData(vineyard::Client& client) {
  vineyard::ObjectID id;
  {
    gs::VertexDataBuilder<uint64_t, int64_t> builder(client, 6);
    auto& arrayBuilder = builder.GetArrayBuilder();
    for (int i = 0; i < 6; ++i) {
      arrayBuilder[i] = i;
    }
    auto vd = builder.MySeal(client);
    id = vd->id();
  }

  std::shared_ptr<gs::VertexData<uint64_t, int64_t>> vd =
      std::dynamic_pointer_cast<gs::VertexData<uint64_t, int64_t>>(
          client.GetObject(id));
  LOG(INFO) << "vnum: " << vd->VerticesNum();
  LOG(INFO) << "vdata : " << vd->GetData(0);
  auto vdArray = vd->GetVdataArray();
  LOG(INFO) << "vd length: " << vdArray.GetLength();
  return vd->id();
}

vineyard::ObjectID TestGraphXEdgeData(vineyard::Client& client,
                                      std::vector<int64_t>& edata_builder) {
  vineyard::ObjectID id;
  {
    gs::EdgeDataBuilder<uint64_t, int64_t> builder(client, edata_builder);
    auto ed = builder.MySeal(client);
    id = ed->id();
  }

  std::shared_ptr<gs::EdgeData<uint64_t, int64_t>> ed =
      std::dynamic_pointer_cast<gs::EdgeData<uint64_t, int64_t>>(
          client.GetObject(id));
  LOG(INFO) << "vnum: " << ed->GetEdgeNum();
  LOG(INFO) << "edata : " << ed->GetEdgeDataByEid(0);
  auto edArray = ed->GetEdataArray();
  LOG(INFO) << "ed length: " << edArray.GetLength();
  return ed->id();
}

void TestGraphXFragment(vineyard::Client& client, vineyard::ObjectID vm_id,
                        vineyard::ObjectID csr_id, vineyard::ObjectID vdata_id,
                        vineyard::ObjectID edata_id) {
  gs::GraphXFragmentBuilder<int64_t, uint64_t, int64_t, int64_t> builder(
      client, vm_id, csr_id, vdata_id, edata_id);
  auto res = std::dynamic_pointer_cast<
      gs::GraphXFragment<int64_t, uint64_t, int64_t, int64_t>>(
      builder.Seal(client));
  LOG(INFO) << "Succesfully construct fragment: " << res->id();
}

boost::leaf::result<void> generateData(arrow::Int64Builder& srcBuilder,
                                       arrow::Int64Builder& dstBuilder,
                                       std::vector<int64_t>& edataBuilder) {
  grape::CommSpec comm_spec;
  comm_spec.Init(MPI_COMM_WORLD);

  // if (comm_spec.worker_id() == 0) {
  ARROW_OK_OR_RAISE(srcBuilder.Reserve(6));
  ARROW_OK_OR_RAISE(dstBuilder.Reserve(6));
  edataBuilder.resize(6);
  srcBuilder.UnsafeAppend(1);
  srcBuilder.UnsafeAppend(1);
  srcBuilder.UnsafeAppend(2);
  srcBuilder.UnsafeAppend(3);
  srcBuilder.UnsafeAppend(4);
  srcBuilder.UnsafeAppend(5);

  dstBuilder.UnsafeAppend(2);
  dstBuilder.UnsafeAppend(3);
  dstBuilder.UnsafeAppend(3);
  dstBuilder.UnsafeAppend(4);
  dstBuilder.UnsafeAppend(6);
  dstBuilder.UnsafeAppend(4);

  edataBuilder[0] = 1;
  edataBuilder[1] = 2;
  edataBuilder[2] = 3;
  edataBuilder[3] = 4;
  edataBuilder[4] = 5;
  edataBuilder[5] = 6;
  return {};
}
void Init() {
  grape::InitMPIComm();
  grape::CommSpec comm_spec;
  comm_spec.Init(MPI_COMM_WORLD);
}

void Finalize() { grape::FinalizeMPIComm(); }

int main(int argc, char* argv[]) {
  if (argc != 2) {
    printf("usage: ./graphx_test <ipc_socket>\n");
    return 1;
  }
  std::string ipc_socket = std::string(argv[1]);
  FLAGS_stderrthreshold = 0;
  google::InitGoogleLogging("graphx_test");
  google::InstallFailureSignalHandler();
  vineyard::Client client;
  VINEYARD_CHECK_OK(client.Connect(ipc_socket));
  LOG(INFO) << "Connected to IPCServer: ";
  Init();

  arrow::Int64Builder srcBuilder, dstBuilder;
  std::vector<int64_t> edataBuilder;
  generateData(srcBuilder, dstBuilder, edataBuilder);

  // TestLocalVertexMap(client);
  auto graphx_vm = TestGraphXVertexMap(client);
  auto csr_id = TestGraphXCSR(client, graphx_vm, srcBuilder, dstBuilder);
  auto vdata_id = TestGraphXVertexData(client);
  auto edata_id = TestGraphXEdgeData(client, edataBuilder);
  TestGraphXFragment(client, graphx_vm.id(), csr_id, vdata_id, edata_id);
  VLOG(1) << "Finish Querying.";
  Finalize();

  google::ShutdownGoogleLogging();
  return 0;
}
