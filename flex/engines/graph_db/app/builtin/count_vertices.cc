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
#include "flex/engines/graph_db/app/builtin/count_vertices.h"

namespace gs {

bool CountVertices::DoQuery(GraphDBSession& sess, Decoder& input,
                            Encoder& output) {
  // First get the read transaction.
  auto txn = sess.GetReadTransaction();
  // We expect one param of type string from decoder.
  if (input.empty()) {
    return false;
  }
  std::string label_name{input.get_string()};
  const auto& schema = txn.schema();
  if (!schema.has_vertex_label(label_name)) {
    output.put_string_view("The requested label doesn't exits.");
    return false;  // The requested label doesn't exits.
  }
  auto label_id = schema.get_vertex_label_id(label_name);
  // The vertices are labeled internally from 0 ~ vertex_label_num, accumulate
  auto vertex_num = txn.GetVertexNum(label_id);
  // the count.
  results::CollectiveResults results;
  auto result = results.add_results();
  result->mutable_record()
      ->add_columns()
      ->mutable_entry()
      ->mutable_element()
      ->mutable_object()
      ->set_i32(vertex_num);

  output.put_string_view(results.SerializeAsString());
  txn.Commit();
  return true;
}

AppWrapper CountVerticesFactory::CreateApp(const GraphDB& db) {
  return AppWrapper(new CountVertices(), NULL);
}
}  // namespace gs
