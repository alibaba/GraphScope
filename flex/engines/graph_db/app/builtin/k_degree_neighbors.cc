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
#include "flex/engines/graph_db/app/builtin/k_degree_neighbors.h"

namespace gs {

bool KNeighbors::DoQuery(GraphDBSession& sess, Decoder& input,
                            Encoder& output) {
  // 创建一个CollectiveResults对象
    auto txn = sess.GetReadTransaction(); 
    Schema schema_ = txn.schema();
    if (input.empty()) {
        return false;
    }
    this->vertex_id_=input.get_int();
    std::string label_name { input.get_string()};
    this->k=input.get_int();
    
    if(k<=0) {
        output.put_string_view("k must be greater than 0.");
        return false;
    }
    if(!schema_.has_vertex_label(label_name)){
        output.put_string_view("The requested label doesn't exits.");
        return false;  // The requested label doesn't exits.
    }
    this->vertex_label_=schema_.get_vertex_label_id(label_name);


    std::set<int64_t> k_neighbors;

    int vertex_size_ = (int)schema_.vertex_label_num();
    int edge_size_ = (int)schema_.edge_label_num();
    
    std::vector<vid_t> nei_index_;
    std::vector<label_t> nei_label_;
    std::vector<vid_t> next_nei_indexs_;
    std::vector<label_t> next_nei_label_;

    nei_label_.push_back(vertex_label_);
    vid_t vertex_index{};
    if(!txn.GetVertexIndex(vertex_label_, (int64_t)vertex_id_, vertex_index)){
        output.put_string_view("get index fail.");
        return false;
    }
    nei_index_.push_back(vertex_index);
    //层次遍历得到k跳邻居
    while(!nei_index_.empty() && k > 0) {
        for(long unsigned int i = 0 ; i<nei_index_.size();i++){
            for(int j = 0 ; j<vertex_size_;j++){
                for(int k = 0 ; k<edge_size_;k++){
                    std::string vertex_name = schema_.get_vertex_label_name(label_t(nei_label_[i]));
                    std::string nei_name = schema_.get_vertex_label_name(label_t(j));
                    std::string edge_name = schema_.get_edge_label_name(label_t(k));
                    if(schema_.has_edge_label(vertex_name,nei_name,edge_name)){
                        auto outedges = txn.GetOutEdgeIterator(nei_label_[i],nei_index_[i],j,k);//1.self_label 2.self_index 3.edge_label 4.nei_label
                        while(outedges.IsValid()){
                            auto neighbor = outedges.GetNeighbor();
                            next_nei_label_.push_back(j);
                            next_nei_indexs_.push_back(neighbor);
                            k_neighbors.insert(txn.GetVertexId(j, neighbor).AsInt64());
                            outedges.Next();
                        }
                    }
                    if(schema_.has_edge_label(nei_name,vertex_name,edge_name)){
                        auto inedges = txn.GetInEdgeIterator(nei_label_[i],nei_index_[i],j,k);//1.self_label 2.self_index 3.edge_label 4.nei_label
                        while(inedges.IsValid()){
                            auto neighbor = inedges.GetNeighbor();
                            next_nei_label_.push_back(j);
                            next_nei_indexs_.push_back(neighbor);
                            k_neighbors.insert(txn.GetVertexId(j, neighbor).AsInt64());
                            inedges.Next();
                        }
                    }
              }
          }
        }
        nei_index_ = next_nei_indexs_;
        nei_label_ = next_nei_label_;
        next_nei_label_.clear();
        next_nei_indexs_.clear();
        k--;
    }
    std::string res = "";
    for(auto it:k_neighbors){
        res+=(std::to_string(it)+" ");
    }
    results::CollectiveResults results;
    auto result = results.add_results();
        result->mutable_record()
        ->add_columns()
        ->mutable_entry()
        ->mutable_element()
        ->mutable_object()
        ->set_str(res);
    output.put_string_view(results.SerializeAsString());
    txn.Commit();
    return true;
}

AppWrapper KNeighborsFactory::CreateApp(const GraphDB& db) {
  return AppWrapper(new KNeighbors(), NULL);
}
}  // namespace gs
