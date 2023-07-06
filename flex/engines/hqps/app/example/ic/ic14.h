#ifndef ENGINES_HPQS_APP_EXAMPLE_IC_IC14_H_
#define ENGINES_HPQS_APP_EXAMPLE_IC_IC14_H_

#include <unordered_map>
#include <vector>

#include <boost/functional/hash.hpp>
#include <boost/property_tree/json_parser.hpp>
#include <boost/property_tree/ptree.hpp>
#include "flex/engines/hqps/engine/context.h"
#include "flex/engines/hqps/engine/hqps_utils.h"
#include "flex/utils/app_utils.h"

#include "flex/engines/hqps/engine/sync_engine.h"

namespace boost {
namespace property_tree {
template <typename Ch, typename Traits>
struct customize_stream<Ch, Traits, double, void> {
  static void insert(std::basic_ostream<Ch, Traits>& s, const double& e) {
    s.precision(std::numeric_limits<double>::digits10 - 1);
    s << e;
  }
  static void extract(std::basic_istream<Ch, Traits>& s, double& e) {
    s.precision(std::numeric_limits<double>::digits10 - 1);
    s >> e;
    if (!s.eof()) {
      s >> std::ws;
    }
  }
};
}  // namespace property_tree
}  // namespace boost
namespace gs {

class IC14Expression0 {
 public:
  IC14Expression0(oid_t oid) : oid_(oid) {}

  bool operator()(const oid_t& data) const { return data == oid_; }

 private:
  oid_t oid_;
};

template <typename GRAPH_INTERFACE>
class QueryIC14 {
  using oid_t = typename GRAPH_INTERFACE::outer_vertex_id_t;
  using vertex_id_t = typename GRAPH_INTERFACE::vertex_id_t;
  using vertex_pair = std::pair<vertex_id_t, vertex_id_t>;
  using nbr_t = typename GRAPH_INTERFACE::nbr_t;
  using label_id_t = typename GRAPH_INTERFACE::label_id_t;

  // static gs::oid_t oid_param = 6597069767117;
  std::string person_label = "PERSON";
  std::string knows_label = "KNOWS";
  std::string post_label = "POST";
  std::string comment_label = "COMMENT";
  std::string has_creator_label = "HASCREATOR";
  std::string replyOf_label = "REPLYOF";

  // static std::string_view firstName = "Jack";
  void get_post_person_nbrs(
      vertex_id_t src, const GRAPH_INTERFACE& graph,
      std::unordered_map<vertex_id_t, std::vector<vertex_id_t>>&
          person_to_perons_via_post) const {
    std::vector<vertex_id_t> src_vec{src};
    auto comment_nbr_list_array = graph.GetOtherVertices(
        comment_label, person_label, has_creator_label, src_vec, "In", INT_MAX);
    std::vector<vertex_id_t> cmt_ids;
    for (auto v : comment_nbr_list_array.get(0)) {
      cmt_ids.push_back(v.neighbor());
    }
    //
    auto post_nbr_list_array = graph.GetOtherVertices(
        comment_label, post_label, replyOf_label, cmt_ids, "Out", INT_MAX);
    std::vector<vertex_id_t> post_ids;
    for (auto i = 0; i < post_nbr_list_array.size(); ++i) {
      for (auto nbr : post_nbr_list_array.get(i)) {
        post_ids.push_back(nbr.neighbor());
      }
    }
    auto person_nbr_list_array = graph.GetOtherVertices(
        post_label, person_label, has_creator_label, post_ids, "Out", INT_MAX);
    std::vector<vertex_id_t> person_ids;
    for (auto i = 0; i < person_nbr_list_array.size(); ++i) {
      for (auto nbr : person_nbr_list_array.get(i)) {
        person_ids.push_back(nbr.neighbor());
      }
    }
    person_to_perons_via_post.insert({src, person_ids});
  }

  void get_cmt_person_nbrs(
      vertex_id_t src, const GRAPH_INTERFACE& graph,
      std::unordered_map<vertex_id_t, std::vector<vertex_id_t>>&
          person_to_perons_via_cmt) const {
    std::vector<vertex_id_t> src_vec{src};
    auto comment_nbr_list_array = graph.GetOtherVertices(
        comment_label, person_label, has_creator_label, src_vec, "In", INT_MAX);
    std::vector<vertex_id_t> cmt_ids;
    for (auto v : comment_nbr_list_array.get(0)) {
      cmt_ids.push_back(v.neighbor());
    }
    //
    auto post_nbr_list_array = graph.GetOtherVertices(
        comment_label, comment_label, replyOf_label, cmt_ids, "Out", INT_MAX);
    std::vector<vertex_id_t> post_ids;
    for (auto i = 0; i < post_nbr_list_array.size(); ++i) {
      for (auto nbr : post_nbr_list_array.get(i)) {
        post_ids.push_back(nbr.neighbor());
      }
    }
    auto person_nbr_list_array =
        graph.GetOtherVertices(comment_label, person_label, has_creator_label,
                               post_ids, "Out", INT_MAX);
    std::vector<vertex_id_t> person_ids;
    for (auto i = 0; i < person_nbr_list_array.size(); ++i) {
      for (auto nbr : person_nbr_list_array.get(i)) {
        person_ids.push_back(nbr.neighbor());
      }
    }
    person_to_perons_via_cmt.insert({src, person_ids});
  }

  double get_post_score(
      const GRAPH_INTERFACE& graph, vertex_id_t src, vertex_id_t dst,
      std::unordered_map<vertex_id_t, std::vector<vertex_id_t>>&
          person_to_perons_via_post) const {
    if (person_to_perons_via_post.find(src) ==
        person_to_perons_via_post.end()) {
      get_post_person_nbrs(src, graph, person_to_perons_via_post);
    }
    if (person_to_perons_via_post.find(dst) ==
        person_to_perons_via_post.end()) {
      get_post_person_nbrs(dst, graph, person_to_perons_via_post);
    }
    size_t cnt = 0;
    for (auto nbr : person_to_perons_via_post[src]) {
      if (nbr == dst) {
        cnt += 1;
      }
    }
    for (auto nbr : person_to_perons_via_post[dst]) {
      if (nbr == src) {
        cnt += 1;
      }
    }
    return (double) cnt;
  }

  double get_cmt_score(
      const GRAPH_INTERFACE& graph, vertex_id_t src, vertex_id_t dst,
      std::unordered_map<vertex_id_t, std::vector<vertex_id_t>>&
          person_to_perons_via_cmt) const {
    if (person_to_perons_via_cmt.find(src) == person_to_perons_via_cmt.end()) {
      get_cmt_person_nbrs(src, graph, person_to_perons_via_cmt);
    }
    if (person_to_perons_via_cmt.find(dst) == person_to_perons_via_cmt.end()) {
      get_cmt_person_nbrs(dst, graph, person_to_perons_via_cmt);
    }
    size_t cnt = 0;
    for (auto nbr : person_to_perons_via_cmt[src]) {
      if (nbr == dst) {
        cnt += 1;
      }
    }
    for (auto nbr : person_to_perons_via_cmt[dst]) {
      if (nbr == src) {
        cnt += 1;
      }
    }
    return (double) cnt;
  }

  double cacl_score_impl(
      const GRAPH_INTERFACE& graph, vertex_pair& pair,
      std::unordered_map<vertex_id_t, std::vector<vertex_id_t>>&
          person_to_person_via_post,
      std::unordered_map<vertex_id_t, std::vector<vertex_id_t>>&
          person_to_pson_via_cmt) const {
    double score = 0;
    auto tmp = get_post_score(graph, pair.first, pair.second,
                              person_to_person_via_post) *
               1.0;
    // VLOG(10) << "pair: " << pair.first << ", " << pair.second
    //          << ", post score:" << tmp;
    score += tmp;
    tmp =
        get_cmt_score(graph, pair.first, pair.second, person_to_pson_via_cmt) *
        0.5;
    // VLOG(10) << "pair: " << pair.first << ", " << pair.second
    //          << ", cmt score:" << tmp;
    score += tmp;
    return score;
  }

  // the cache should give same result for <l,r> and <r,l>
  double calc_score(
      const GRAPH_INTERFACE& graph, const Path<vertex_id_t>& path,
      std::unordered_map<vertex_pair, double, boost::hash<vertex_pair>>& cache,
      std::unordered_map<vertex_id_t, std::vector<vertex_id_t>>&
          person_to_person_via_post,
      std::unordered_map<vertex_id_t, std::vector<vertex_id_t>>&
          person_to_pson_via_cmt) const {
    double res = 0.0;
    auto& vertices = path.GetVertices();
    // std::unordered_map<vertex_id_t, std::vector<nbr_t>> vid_to_cmt_nbrs;

    for (auto i = 0; i < vertices.size() - 1; ++i) {
      auto src = std::min(vertices[i], vertices[i + 1]);
      auto dst = std::max(vertices[i], vertices[i + 1]);
      auto pair = std::make_pair(src, dst);
      if (cache.find(pair) == cache.end()) {
        // VLOG(10) << "calc for pair: " << i;
        cache[pair] = cacl_score_impl(graph, pair, person_to_person_via_post,
                                      person_to_pson_via_cmt);
      }
      res += cache[pair];
    }
    // VLOG(10) << "Totally: " << pair_set.size() << " pairs";
    // VLOG(10) << "path: " << gs::to_string(vertices) << ", score: " << res;
    return res;
  }

 public:
  void Query(const GRAPH_INTERFACE& graph, int64_t ts,
             const boost::property_tree::ptree& input,
             boost::property_tree::ptree& output) const {
    oid_t src_id = input.get<oid_t>("person1IdQ14StartNode");
    oid_t dst_id = input.get<oid_t>("person2IdQ14EndNode");

    std::vector<char> input_buffer, output_buffer;
    Encoder input_encoder(input_buffer);
    input_encoder.put_long(src_id);
    input_encoder.put_long(dst_id);
    Decoder input_decoder(input_buffer.data(), input_buffer.size());

    Encoder output_encoder(output_buffer);
    Query(graph, ts, input_decoder, output_encoder);

    Decoder output_decoder(output_buffer.data(), output_buffer.size());
    while (!output_decoder.empty()) {
      boost::property_tree::ptree node;
      // output.put("shortestPathLength", output_decoder.get_int());  // id
      int32_t size = output_decoder.get_int();
      // VLOG(10) << "size: " << size;

      boost::property_tree::ptree child_node;
      for (auto i = 0; i < size; ++i) {
        // node_ids.emplace_back(output_decoder.get_long());
        boost::property_tree::ptree tmp_node;
        auto l = output_decoder.get_long();
        tmp_node.put("", l);
        child_node.push_back(std::make_pair("", tmp_node));
      }
      node.add_child("personIdsInPath", child_node);

      double weight = (double) output_decoder.get_double();
      // boost::property_tree::ptree tmp_node;
      // tmp_node.put<double>("", weight);
      std::stringstream stream;
      stream << std::fixed << std::setprecision(1) << weight;
      std::string s = stream.str();
      node.put("pathWeight", s);
      output.push_back(std::make_pair("", node));
    }
  }
  void Query(const GRAPH_INTERFACE& graph, int64_t ts, Decoder& input,
             Encoder& output) const {
    std::setprecision(1);
    int64_t src_id = input.get_long();
    int64_t dst_id = input.get_long();

    using Engine = SyncEngine<GRAPH_INTERFACE>;
    using label_id_t = typename GRAPH_INTERFACE::label_id_t;
    label_id_t person_label_id = graph.GetVertexLabelId(person_label);
    label_id_t knows_label_id = graph.GetEdgeLabelId(knows_label);

    auto ctx0 = Engine::template ScanVertexWithOid<AppendOpt::Temp>(
        graph, person_label_id, src_id);
    // message

    auto edge_expand_opt6 = gs::make_edge_expandv_opt(
        gs::Direction::Both, knows_label_id, person_label_id);
    auto get_v_opt = gs::make_getv_opt(
        gs::VOpt::End, std::array<label_id_t, 1>{person_label_id});

    auto filter = gs::make_filter(IC14Expression0(dst_id),
                                  gs::PropertySelector<oid_t>("id"));

    auto shortest_path_opt = gs::make_shortest_path_opt(
        std::move(edge_expand_opt6), std::move(get_v_opt),
        gs::Range(0, INT_MAX), std::move(filter), PathOpt::Simple,
        ResultOpt::AllV);

    auto ctx1 =
        Engine::template ShortestPath<AppendOpt::Temp, INPUT_COL_ID(-1)>(
            graph, std::move(ctx0), std::move(shortest_path_opt));

    // apply sub query on path.
    auto paths = ctx1.template GetNode<-1>();
    std::vector<double> scores;

    std::unordered_map<vertex_pair, double, boost::hash<vertex_pair>> cache;
    std::unordered_map<vertex_id_t, std::vector<vertex_id_t>>
        vid_to_post_via_post;
    std::unordered_map<vertex_id_t, std::vector<vertex_id_t>>
        vid_to_post_via_cmt;

    for (auto iter : paths) {
      scores.push_back(calc_score(graph, iter.GetElement(), cache,
                                  vid_to_post_via_post, vid_to_post_via_cmt));
    }
    // VLOG(10) << gs::to_string(scores);

    std::vector<size_t> inds;
    for (auto i = 0; i < scores.size(); ++i) {
      inds.emplace_back(i);
    }
    sort(inds.begin(), inds.end(), [&scores](const int& a, const int& b) {
      return scores[a] > scores[b];
    });

    for (auto i = 0; i < inds.size(); ++i) {
      auto& path = paths.get(inds[i]);
      auto& vec = path.GetVertices();
      output.put_int((int) vec.size());

      std::array<std::string, 1> props{"id"};
      auto oids =
          graph.template GetVertexPropsFromVid<oid_t>(person_label, vec, props);
      for (auto j = 0; j < vec.size(); ++j) {
        output.put_long(std::get<0>(oids[j]));
      }

      // VLOG(10) << gs::to_string(vec) << "," << scores[inds[i]];
      output.put_double(scores[inds[i]]);
    }
  }
};
}  // namespace gs

#endif  // ENGINES_HPQS_APP_EXAMPLE_IC_IC14_H_