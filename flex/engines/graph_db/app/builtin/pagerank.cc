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
#include "flex/engines/graph_db/app/builtin/pagerank.h"

namespace gs {


bool PageRank::DoQuery(GraphDBSession& sess, Decoder& input,
                            Encoder& output) {
  // First get the read transaction.
  auto txn = sess.GetReadTransaction();
  // We expect one param of type string from decoder.
  if (input.empty()) {
    return false;
  }
  std::string vertex_label{input.get_string()};
  std::string edge_label{input.get_string()};

  if(!input.empty()) {
    damping_factor_ = input.get_double();
    max_iterations_ = input.get_int();
    epsilon_ = input.get_double();
  }

  vertex_label_id_ = sess.schema().get_vertex_label_id(vertex_label);
  edge_label_id_ = sess.schema().get_edge_label_id(edge_label);

  // 统计顶点数量
  auto num_vertices = txn.GetVertexNum(vertex_label_id_);

  // 初始化每个顶点的PageRank值为 1.0 / num_vertices
  std::unordered_map<vid_t, double> pagerank;
  std::unordered_map<vid_t, double> new_pagerank;

  auto vertex_iter = txn.GetVertexIterator(vertex_label_id_);

  while (vertex_iter.IsValid()) {
      vid_t vid = vertex_iter.GetIndex();
      pagerank[vid] = 1.0 / num_vertices;
      new_pagerank[vid] = 0.0;
      vertex_iter.Next();
  }

  // 获取点的出度
  std::unordered_map<vid_t, double> outdegree;

  // 开始迭代计算PageRank值
  for (int iter = 0; iter < max_iterations_; ++iter) {
      // 初始化新的PageRank值
      for (auto& kv : new_pagerank) {
          kv.second = 0.0;
      }

      // 遍历所有顶点
      auto vertex_iter = txn.GetVertexIterator(vertex_label_id_);
      while (vertex_iter.IsValid()) {
      vid_t v = vertex_iter.GetIndex();
      
      // 遍历所有出边并累加其PageRank贡献值
      double sum = 0.0;
      auto edges = txn.GetInEdgeIterator(vertex_label_id_, v, vertex_label_id_, edge_label_id_);
      while(edges.IsValid()){
          auto neighbor = edges.GetNeighbor();
          if(outdegree[neighbor] == 0){
              auto out_edges = txn.GetOutEdgeIterator(vertex_label_id_, neighbor, vertex_label_id_, edge_label_id_);
              while(out_edges.IsValid()){
                  outdegree[neighbor]++;
                  out_edges.Next();
              }
          }
          sum += pagerank[neighbor] / outdegree[neighbor];
          edges.Next();
      }

      // 计算新的PageRank值
      new_pagerank[v] = damping_factor_ * sum + (1.0 - damping_factor_) / num_vertices;
      vertex_iter.Next();
      }

      // 检查收敛
      double diff = 0.0;
      for (const auto& kv : pagerank) {
          diff += std::abs(new_pagerank[kv.first] - kv.second);
      }

      // 如果收敛，则停止迭代
      if (diff < epsilon_) {
          break;
      }

      // 交换pagerank与new_pagerank的内容
      std::swap(pagerank, new_pagerank);
  }

  std::ostringstream output_stream;
  for(auto kv : pagerank) {
      auto id =txn.GetVertexId(vertex_label_id_, kv.first).AsInt64();
      // output.put_int(static_cast<int>(id));
      // output.put_double(kv.second);
      output_stream << "vertex: " << id << ", pagerank: " << kv.second << "\n";
  }
  std::string res_string = output_stream.str();
  results::CollectiveResults results;
    auto result = results.add_results();
  
    result->mutable_record()
    ->add_columns()
    ->mutable_entry()
    ->mutable_element()
    ->mutable_object()
    ->set_str(res_string);
    
    output.put_string_view(results.SerializeAsString());

  txn.Commit();
  return true;
}

AppWrapper PageRankFactory::CreateApp(const GraphDB& db) {
  return AppWrapper(new PageRank(), NULL);
}
}  // namespace gs
