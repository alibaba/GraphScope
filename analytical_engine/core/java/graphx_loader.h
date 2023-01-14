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

#ifndef ANALYTICAL_ENGINE_CORE_JAVA_GRAPHX_LOADER_H_
#define ANALYTICAL_ENGINE_CORE_JAVA_GRAPHX_LOADER_H_

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "arrow/array.h"
#include "arrow/array/builder_binary.h"

#include "grape/grape.h"
#include "grape/worker/comm_spec.h"
#include "vineyard/basic/ds/array.h"
#include "vineyard/basic/ds/arrow.h"
#include "vineyard/basic/ds/arrow_utils.h"
#include "vineyard/client/client.h"
#include "vineyard/graph/loader/basic_ev_fragment_loader.h"
#include "vineyard/graph/loader/basic_ev_fragment_loader_impl.h"
#include "vineyard/graph/utils/table_shuffler_impl.h"

#include "core/java/graphx_raw_data.h"

namespace gs {

/**
 * @brief A Partitioner for loading graphx graph, where vertex shuffling is done
 * in spark, so the result of getPartitionId should equal to the comm_rank of
 * ourself.
 *
 * @tparam OID_T oid type.
 */
template <typename OID_T>
class GraphXPartitioner {
 public:
  using oid_t = OID_T;

  GraphXPartitioner() {
    fnum_ = 1;
    pid2Fid_ = {0};
  }

  void Init(std::vector<int> pid2Fid) {
    fnum_ = pid2Fid.size();
    pid2Fid_ = pid2Fid;
  }

  inline fid_t GetPartitionId(const OID_T& oid) const {
    fid_t pid = static_cast<fid_t>(static_cast<uint64_t>(oid) % fnum_);
    return pid2Fid_[pid];
  }

  GraphXPartitioner& operator=(const GraphXPartitioner& other) {
    if (this == &other) {
      return *this;
    }
    fnum_ = other.fnum_;
    pid2Fid_ = other.pid2Fid_;
    return *this;
  }

  GraphXPartitioner(const GraphXPartitioner& other) {
    fnum_ = other.fnum_;
    pid2Fid_ = other.pid2Fid_;
  }

  GraphXPartitioner& operator=(GraphXPartitioner&& other) {
    if (this == &other) {
      return *this;
    }
    fnum_ = other.fnum_;
    pid2Fid_ = other.pid2Fid_;
    return *this;
  }

 private:
  fid_t fnum_;
  std::vector<int> pid2Fid_;
};

template <typename OID_T, typename VID_T, typename VDATA_T, typename EDATA_T>
class GraphXLoader
    : public vineyard::BasicEVFragmentLoader<OID_T, VID_T,
                                             GraphXPartitioner<OID_T>> {
  using raw_data_t = GraphXRawData<OID_T, VID_T, VDATA_T, EDATA_T>;

 public:
  explicit GraphXLoader(vineyard::ObjectID objId, vineyard::Client& client,
                        const grape::CommSpec& comm_spec,
                        const GraphXPartitioner<OID_T>& partitioner,
                        bool directed = true, bool generate_eid = false,
                        bool retain_oid = false)
      : vineyard::BasicEVFragmentLoader<OID_T, VID_T, GraphXPartitioner<OID_T>>(
            client, comm_spec, partitioner, directed, generate_eid,
            retain_oid) {
    this->raw_data =
        std::dynamic_pointer_cast<raw_data_t>(client.GetObject(objId));
  }
  ~GraphXLoader() {}

  boost::leaf::result<vineyard::ObjectID> LoadFragment() {
    // make table from raw data
    auto oids = raw_data->GetOids();
    auto src_oids = raw_data->GetSrcOids();
    auto dst_oids = raw_data->GetDstOids();
    auto vdatas = raw_data->GetVdataArray();
    auto edatas = raw_data->GetEdataArray();

    std::vector<std::shared_ptr<arrow::Field>> vertex_schema_vector = {
        arrow::field("ID", vineyard::ConvertToArrowType<OID_T>::TypeValue()),
        arrow::field("VALUE",
                     vineyard::ConvertToArrowType<VDATA_T>::TypeValue())};

    auto vertex_schema = std::make_shared<arrow::Schema>(vertex_schema_vector);

    std::shared_ptr<arrow::Table> vertex_table =
        arrow::Table::Make(vertex_schema, {oids, vdatas});

    LOG(INFO) << "Finish built vertex table";

    std::vector<std::shared_ptr<arrow::Field>> edge_schema_vector = {
        arrow::field("SRC", vineyard::ConvertToArrowType<OID_T>::TypeValue()),
        arrow::field("DST", vineyard::ConvertToArrowType<OID_T>::TypeValue()),
        arrow::field("VALUE",
                     vineyard::ConvertToArrowType<EDATA_T>::TypeValue())};

    auto edge_schema = std::make_shared<arrow::Schema>(edge_schema_vector);

    std::shared_ptr<arrow::Table> edge_table =
        arrow::Table::Make(edge_schema, {src_oids, dst_oids, edatas});

    LOG(INFO) << "Finish built edge table";
    // load fragment from tables.
    BOOST_LEAF_CHECK(this->AddVertexTable("v0", vertex_table));
    LOG(INFO) << "Fnish adding vertices";

    BOOST_LEAF_CHECK(this->ConstructVertices());
    LOG(INFO) << "Fnish Construct vertices";

    BOOST_LEAF_CHECK(this->AddEdgeTable("v0", "v0", "e0", edge_table));
    LOG(INFO) << "Fnish adding edges";

    BOOST_LEAF_CHECK(this->ConstructEdges());
    LOG(INFO) << "Fnish Construct edges";

    return this->ConstructFragment();
  }

 private:
  std::shared_ptr<raw_data_t> raw_data;
};

}  // namespace gs

#endif  // ANALYTICAL_ENGINE_CORE_JAVA_GRAPHX_LOADER_H_
