/**
 * Copyright 2021 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <thread>
#include "lgraph/client/graph_client.h"
#include "lgraph/log_subscription/subscriber.h"

std::string GetPropValueAsStr(lgraph::PropertyId pid, const lgraph::log_subscription::PropertyInfo &p, const lgraph::Schema &schema) {
  auto &prop_def = schema.GetPropDef(pid);
  switch (prop_def.GetDataType()) {
  case lgraph::INT:
    return std::to_string(p.GetAsInt32());
  case lgraph::LONG:
    return std::to_string(p.GetAsInt64());
  case lgraph::FLOAT:
    return std::to_string(p.GetAsFloat());
  case lgraph::DOUBLE:
    return std::to_string(p.GetAsDouble());
  case lgraph::STRING: {
    return std::string{p.GetAsStr()};
  }
  default:
    return "";
  }
}

std::string GetVertexInsertInfo(const lgraph::log_subscription::Operation &op, const lgraph::Schema &schema) {
  auto vertex_info = op.GetInfoAsVertexInsertOp();
  std::string v_id = std::to_string(vertex_info.GetVertexId());
  std::string info;
  info += "<VertexID: " + v_id + ">";
  info += "<Label: " + schema.GetTypeDef(vertex_info.GetLabelId()).GetLabelName() + ">";
  for (auto &entry : vertex_info.GetPropMap()) {
    info += "<" + schema.GetPropDef(entry.first).GetPropName() + ": " + GetPropValueAsStr(entry.first, entry.second, schema) + ">";
  }
  return info;
}

std::string GetEdgeInsertInfo(const lgraph::log_subscription::Operation &op, const lgraph::Schema &schema) {
  auto edge_info = op.GetInfoAsEdgeInsertOp();
  auto &edge_id = edge_info.GetEdgeId();
  auto &edge_rel = edge_info.GetEdgeRelation();
  std::string info;
  info += "<EdgeID: " + std::to_string(edge_id.edge_inner_id) + ">";
  info += "<SrcID: " + std::to_string(edge_id.src_vertex_id) + ">";
  info += "<DstID: " + std::to_string(edge_id.dst_vertex_id) + ">";
  info += "<EdgeLabel: " + schema.GetTypeDef(edge_rel.edge_label_id).GetLabelName() + ">";
  info += "<SrcLabel: " + schema.GetTypeDef(edge_rel.src_vertex_label_id).GetLabelName() + ">";
  info += "<DstLabel: " + schema.GetTypeDef(edge_rel.dst_vertex_label_id).GetLabelName() + ">";
  info += "<Direction: " + std::string(edge_info.IsForward()? "forward" : "reverse") + ">";
  for (auto &entry : edge_info.GetPropMap()) {
    info += "<" + schema.GetPropDef(entry.first).GetPropName() + ": " + GetPropValueAsStr(entry.first, entry.second, schema) + ">";
  }
  return info;
}

void PrintLogMsg(int subscriber_id, const lgraph::log_subscription::LogMessage& msg, const lgraph::Schema &schema) {
  if (msg.IsError()) {
    std::cout << "Got Error log Message: " + msg.GetErrorMsg() + "\n";
    return;
  }
  auto parser = msg.GetParser();
  std::string info = "---------- [Subscriber " + std::to_string(subscriber_id) + "] Log Entry Polled With Snapshot Id ["
      + std::to_string(parser.GetSnapshotId()) + "] ----------\n";
  auto operations = parser.GetOperations();
  for (auto &op : operations) {
    auto op_type = op.GetOpType();
    if (op_type == lgraph::OpType::MARKER) {
      info += "[Marker Op] Ignore\n";
    } else if (op_type == lgraph::OpType::CREATE_VERTEX_TYPE) {
      info += "[Ddl Op] Create Vertex Type\n";
    } else if (op_type == lgraph::OpType::CREATE_EDGE_TYPE) {
      info += "[Ddl Op] Create Edge Type\n";
    } else if (op_type == lgraph::OpType::ADD_EDGE_KIND) {
      info += "[Ddl Op] Add Edge Kind\n";
    } else if (op_type == lgraph::OpType::OVERWRITE_VERTEX || op_type == lgraph::OpType::UPDATE_VERTEX) {
      info += "[VertexInsert Op] " + GetVertexInsertInfo(op, schema) + "\n";
    } else if (op_type == lgraph::OpType::OVERWRITE_EDGE || op_type == lgraph::OpType::UPDATE_EDGE) {
      info += "[EdgeInsert Op] " + GetEdgeInsertInfo(op, schema) + "\n";
    } else {
      info += "[Unconcerned Op] Ignore\n";
    }
  }
  std::cout << info;
}

void PollLogBatch(int subscriber_id, lgraph::log_subscription::Subscriber* subscriber, const lgraph::Schema &schema) {
  int count = 0;
  while (true) {
    auto msg = subscriber->Poll(500);
    if (msg) {
      PrintLogMsg(subscriber_id, msg, schema);
      if (++count >= 150) {
        break;
      }
    }
  }
}

int main() {
  lgraph::client::GraphClient graph_client("localhost:55556");
  lgraph::Schema schema = graph_client.GetGraphSchema();
  lgraph::LoggerInfo logger_info = graph_client.GetLoggerInfo();
  std::cout << "*** Client: got the logger info: [kafka_servers: " + logger_info.kafka_servers + "], [topic: "
    + logger_info.topic + "], [queue_num: " + std::to_string(logger_info.queue_number) + "]\n";
  std::vector<lgraph::log_subscription::Subscriber*> subscribers;
  std::vector<std::thread> threads;
  subscribers.reserve(logger_info.queue_number);
  for (int i = 0; i < logger_info.queue_number; i++) {
    subscribers.push_back(
        new lgraph::log_subscription::Subscriber(logger_info.kafka_servers, logger_info.topic, i, 0));
  }
  threads.reserve(logger_info.queue_number);
  for (int i = 0; i < logger_info.queue_number; i++) {
    threads.emplace_back(std::thread{PollLogBatch, i, subscribers[i], schema});
  }
  for (auto &t : threads) {
    t.join();
  }
  for (auto &subscriber : subscribers) {
    delete subscriber;
  }
}

