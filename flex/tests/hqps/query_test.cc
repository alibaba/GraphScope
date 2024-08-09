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
#include "flex/engines/hqps_db/core/sync_engine.h"
#include "flex/engines/hqps_db/database/mutable_csr_interface.h"
#include "flex/tests/hqps/match_query.h"
#include "flex/tests/hqps/sample_query.h"

#include <time.h>

int main(int argc, char** argv) {
  // <oid> <label_name>
  if (argc != 3) {
    LOG(ERROR) << "Usage: ./query_test <graph_schema> "
                  "<data_dir>";
    return 1;
  }
  auto graph_schema = std::string(argv[1]);
  auto data_dir = std::string(argv[2]);

  auto& db = gs::GraphDB::get();
  auto schema_res = gs::Schema::LoadFromYaml(graph_schema);
  if (!schema_res.ok()) {
    LOG(ERROR) << "Fail to load graph schema file: "
               << schema_res.status().error_message();
    return -1;
  }
  db.Open(schema_res.value(), data_dir, 1);
  auto& sess = gs::GraphDB::get().GetSession(0);

  {
    auto& graph = sess.graph();
    auto max_v_num = graph.vertex_num(1);
    std::vector<gs::MutableCSRInterface::vertex_id_t> vids(max_v_num);
    for (gs::MutableCSRInterface::vertex_id_t i = 0; i < max_v_num; ++i) {
      vids[i] = i;
    }
    gs::MutableCSRInterface interface(sess);
    std::array<std::string, 1> prop_names{"creationDate"};
    auto edges =
        interface.GetEdges<int64_t>(1, 1, 8, vids, "Both", INT_MAX, prop_names);
    double t = -grape::GetCurrentTime();
    size_t cnt = 0;
    for (size_t i = 0; i < vids.size(); ++i) {
      auto adj_list = edges.get(i);
      for (auto iter : adj_list) {
        VLOG(10) << iter.neighbor() << ", " << gs::to_string(iter.properties());
        cnt += 1;
      }
    }
    t += grape::GetCurrentTime();
    LOG(INFO) << "visiting edges: cost: " << t << ", num edges: " << cnt;

    // visiting vertices properties
    auto vertex_prop =
        interface.GetVertexPropsFromVid<int64_t>(1, vids, {"id"});
    for (size_t i = 0; i < 10; ++i) {
      VLOG(10) << "vid: " << vids[i]
               << ", prop: " << gs::to_string(vertex_prop[i]);
    }
  }

  {
    gs::SampleQuery query;
    std::vector<char> encoder_array;
    gs::Encoder input_encoder(encoder_array);
    input_encoder.put_long(19791209300143);
    input_encoder.put_long(1354060800000);
    std::vector<char> output_array;
    gs::Encoder output(output_array);
    gs::Decoder input(encoder_array.data(), encoder_array.size());

    query.Query(sess, input, output);
    LOG(INFO) << "Finish Sample query";
  }
  {
    gs::MatchQuery query;
    std::vector<char> encoder_array;
    gs::Encoder input_encoder(encoder_array);
    std::vector<char> output_array;
    gs::Encoder output(output_array);
    gs::Decoder input(encoder_array.data(), encoder_array.size());

    query.Query(sess, input, output);
    LOG(INFO) << "Finish MatchQuery test";
  }

  {
    gs::MatchQuery1 query;
    std::vector<char> encoder_array;
    gs::Encoder input_encoder(encoder_array);
    std::vector<char> output_array;
    gs::Encoder output(output_array);
    gs::Decoder input(encoder_array.data(), encoder_array.size());

    query.Query(sess, input, output);
    LOG(INFO) << "Finish MatchQuery1 test";
  }

  {
    gs::MatchQuery2 query;
    std::vector<char> encoder_array;
    gs::Encoder input_encoder(encoder_array);
    std::vector<char> output_array;
    gs::Encoder output(output_array);
    gs::Decoder input(encoder_array.data(), encoder_array.size());

    query.Query(sess, input, output);
    LOG(INFO) << "Finish MatchQuery2 test";
  }

  {
    gs::MatchQuery3 query;
    std::vector<char> encoder_array;
    gs::Encoder input_encoder(encoder_array);
    std::vector<char> output_array;
    gs::Encoder output(output_array);
    gs::Decoder input(encoder_array.data(), encoder_array.size());

    query.Query(sess, input, output);
    LOG(INFO) << "Finish MatchQuery3 test";
  }

  {
    gs::MatchQuery4 query;
    std::vector<char> encoder_array;
    gs::Encoder input_encoder(encoder_array);
    std::vector<char> output_array;
    gs::Encoder output(output_array);
    gs::Decoder input(encoder_array.data(), encoder_array.size());

    query.Query(sess, input, output);
    LOG(INFO) << "Finish MatchQuery4 test";
  }

  {
    gs::MatchQuery5 query;
    std::vector<char> encoder_array;
    gs::Encoder input_encoder(encoder_array);
    std::vector<char> output_array;
    gs::Encoder output(output_array);
    gs::Decoder input(encoder_array.data(), encoder_array.size());

    query.Query(sess, input, output);
    LOG(INFO) << "Finish MatchQuery5 test";
  }

  {
    gs::MatchQuery7 query;
    std::vector<char> encoder_array;
    gs::Encoder input_encoder(encoder_array);
    std::vector<char> output_array;
    gs::Encoder output(output_array);
    gs::Decoder input(encoder_array.data(), encoder_array.size());

    query.Query(sess, input, output);
    LOG(INFO) << "Finish MatchQuery7 test";
  }

  {
    gs::MatchQuery9 query;
    std::vector<char> encoder_array;
    gs::Encoder input_encoder(encoder_array);
    std::vector<char> output_array;
    gs::Encoder output(output_array);
    gs::Decoder input(encoder_array.data(), encoder_array.size());

    query.Query(sess, input, output);
    LOG(INFO) << "Finish MatchQuery9 test";
  }

  {
    gs::MatchQuery10 query;
    std::vector<char> encoder_array;
    gs::Encoder input_encoder(encoder_array);
    std::vector<char> output_array;
    gs::Encoder output(output_array);
    gs::Decoder input(encoder_array.data(), encoder_array.size());

    query.Query(sess, input, output);
    LOG(INFO) << "Finish MatchQuery10 test";
  }

  {
    gs::MatchQuery11 query;
    std::vector<char> encoder_array;
    gs::Encoder input_encoder(encoder_array);
    std::vector<char> output_array;
    gs::Encoder output(output_array);
    gs::Decoder input(encoder_array.data(), encoder_array.size());

    query.Query(sess, input, output);
    LOG(INFO) << "Finish MatchQuery11 test";
  }

  {
    gs::MatchQuery12 query;
    std::vector<char> encoder_array;
    gs::Encoder input_encoder(encoder_array);
    std::vector<char> output_array;
    gs::Encoder output(output_array);
    gs::Decoder input(encoder_array.data(), encoder_array.size());

    query.Query(sess, input, output);
    LOG(INFO) << "Finish MatchQuery12 test";
  }

  {
    gs::MatchQuery14 query;
    std::vector<char> encoder_array;
    gs::Encoder input_encoder(encoder_array);
    std::vector<char> output_array;
    gs::Encoder output(output_array);
    gs::Decoder input(encoder_array.data(), encoder_array.size());

    query.Query(sess, input, output);
    LOG(INFO) << "Finish MatchQuery14 test";
  }

  {
    // test PathExpand with multiple edge triplets.
    gs::MatchQuery15 query;
    std::vector<char> encoder_array;
    gs::Encoder input_encoder(encoder_array);
    std::vector<char> output_array;
    gs::Encoder output(output_array);
    gs::Decoder input(encoder_array.data(), encoder_array.size());

    query.Query(sess, input, output);
    LOG(INFO) << "Finish MatchQuery15 test";
  }

  {
    gs::MatchQuery16 query;
    std::vector<char> encoder_array;
    gs::Encoder input_encoder(encoder_array);
    std::vector<char> output_array;
    gs::Encoder output(output_array);
    gs::Decoder input(encoder_array.data(), encoder_array.size());

    query.Query(sess, input, output);
    LOG(INFO) << "Finish MatchQuery16 test";
  }

  LOG(INFO) << "Finish context test.";
}
