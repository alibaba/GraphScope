/** Copyright 2022 Alibaba Group Holding Limited.
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

#ifndef ANALYTICAL_ENGINE_CORE_JAVA_RDD_TRANSFER_CLIENT_H_
#define ANALYTICAL_ENGINE_CORE_JAVA_RDD_TRANSFER_CLIENT_H_

#include <mpi.h>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "google/protobuf/empty.pb.h"
#include "grpc/grpc.h"
#include "grpcpp/channel.h"
#include "grpcpp/client_context.h"
#include "grpcpp/create_channel.h"
#include "grpcpp/security/credentials.h"

#include "arrow/array.h"
#include "arrow/array/builder_binary.h"
#include "arrow/array/builder_primitive.h"
#include "arrow/table.h"
#include "arrow/type.h"

#include "rdd.grpc.pb.h"  // NOLINT(build/include_subdir)

using grpc::Channel;
using grpc::ClientContext;
using grpc::ClientReader;
using grpc::ClientReaderWriter;
using grpc::ClientWriter;
using grpc::Status;

using RDDReaderTransfer::array_type;
using RDDReaderTransfer::basic_type;
using RDDReaderTransfer::CloseRequest;
using RDDReaderTransfer::CloseResponse;
using RDDReaderTransfer::essential_type;
using RDDReaderTransfer::GetArray;
using RDDReaderTransfer::ItemRequest;
using RDDReaderTransfer::PartInfoRequest;
using RDDReaderTransfer::PartitionInfo;
using RDDReaderTransfer::PartitionItem;

namespace gs {

class RDDReaderClient {
 public:
  explicit RDDReaderClient(std::shared_ptr<Channel> channel)
      : stub_(GetArray::NewStub(channel)), part_id_(0) {
    vertex_schema_vector_ = {arrow::field("ID", arrow::int64()),
                             arrow::field("VALUE", arrow::utf8())};

    edge_schema_vector_ = {arrow::field("SRC", arrow::int64()),
                           arrow::field("DST", arrow::int64()),
                           arrow::field("VALUE", arrow::utf8())};
  }

  void RequestPartitionInfo() {
    PartInfoRequest info_req;
    info_req.set_req(true);

    PartitionInfo part_info;
    ClientContext context;
    Status status = stub_->GetPartitionInfo(&context, info_req, &part_info);
    if (status.ok()) {
      part_id_ = part_info.partitionid();
      std::string rdd_data_type = part_info.datatype();
      part_data_type_ = str_split(rdd_data_type, ":");
    }
  }

  bool RequestArrItem() {
    ItemRequest item_req;
    item_req.set_req(true);

    PartitionItem item_reply;
    ClientContext context;
    Status status;
    std::unique_ptr<ClientReader<PartitionItem>> reader(
        stub_->GetPartitionItem(&context, item_req));

    int item_cnt = 0;
    while (reader->Read(&item_reply)) {
      resolve_item_data(item_reply);
      item_cnt++;
    }

    status = reader->Finish();
    if (status.ok()) {
      arrow::Int64Builder id_builder;
      id_builder.AppendValues(oid_vec1_);

      auto id_maybe_array = id_builder.Finish();
      std::shared_ptr<arrow::Array> id_array = *id_maybe_array;

      arrow::StringBuilder str_builder;
      str_builder.AppendValues(data_vec_);
      auto str_maybe_array = str_builder.Finish();
      std::shared_ptr<arrow::Array> data_array = *str_maybe_array;
      if (get_node_data_) {
        auto vertex_schema =
            std::make_shared<arrow::Schema>(vertex_schema_vector_);
        vertex_table_ =
            arrow::Table::Make(vertex_schema, {id_array, data_array});
      } else {
        auto edge_schema = std::make_shared<arrow::Schema>(edge_schema_vector_);
        arrow::Int64Builder dst_builder;
        dst_builder.AppendValues(oid_vec2_);
        auto dst_maybe_array = dst_builder.Finish();
        std::shared_ptr<arrow::Array> dst_array = *dst_maybe_array;

        edge_table_ =
            arrow::Table::Make(edge_schema, {id_array, dst_array, data_array});
      }
      return true;
    } else {
      return false;
    }
  }

  bool SendClose() {
    ClientContext context;
    CloseRequest close_req;
    close_req.set_req(true);

    CloseResponse response;
    Status status = stub_->RpcClose(&context, close_req, &response);
    if (status.ok()) {
      return true;
    } else {
      return false;
    }
  }

  int GetPartId() { return part_id_; }

  void GetEdgeData() { get_node_data_ = false; }

  std::shared_ptr<arrow::Table> get_vertex_table() { return vertex_table_; }

  std::shared_ptr<arrow::Table> get_edge_table() { return edge_table_; }

 private:
  std::vector<std::string> str_split(std::string str, std::string sep) {
    std::vector<std::string> ret;
    int posi = str.find_first_of(sep);
    while (posi != std::string::npos) {
      std::string tmp = str.substr(0, posi);
      ret.push_back(tmp);
      str = str.substr(posi + 1);
      posi = str.find_first_of(sep);
    }
    if (str != "") {
      ret.push_back(str);
    }
    return ret;
  }

  void resolve_item_data(const PartitionItem& data) {
    for (int i = 1; i < part_data_type_.size(); i++) {
      if (part_data_type_[i].substr(0, 5) == "Array") {
        array_type array_data = data.basic_data(i - 1).array();
        int item_cnt = array_data.item_size();

        std::string attr = "";
        for (int j = 0; j < item_cnt; j++) {
          std::string str = array_data.item(j).string_data();
          attr = attr + "," + str;
        }
        data_vec_.push_back(attr);
      } else {
        essential_type essen_data = data.basic_data(i - 1).essen();
        if (part_data_type_[i] == "long") {
          int64_t vid = essen_data.long_data();
          if (i == 1) {
            oid_vec1_.push_back(vid);
          } else {
            oid_vec2_.push_back(vid);
          }
        } else {
          std::cout << "type error, id type should be long" << std::endl;
        }
      }
    }
  }

 private:
  std::unique_ptr<GetArray::Stub> stub_;
  int part_id_;
  bool get_node_data_ = true;
  std::vector<std::string> part_data_type_;
  std::vector<std::shared_ptr<arrow::Field>> vertex_schema_vector_;
  std::vector<std::shared_ptr<arrow::Field>> edge_schema_vector_;

  std::vector<int64_t> oid_vec1_;
  std::vector<int64_t> oid_vec2_;
  std::vector<std::string> data_vec_;

  std::shared_ptr<arrow::Table> vertex_table_;
  std::shared_ptr<arrow::Table> edge_table_;
};

}  // namespace gs

#endif  // ANALYTICAL_ENGINE_CORE_JAVA_RDD_TRANSFER_CLIENT_H_
