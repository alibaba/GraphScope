#ifndef ENGINES_HPQS_APP_EXAMPLE_IC_IC10_H_
#define ENGINES_HPQS_APP_EXAMPLE_IC_IC10_H_

#include <time.h>
#include <boost/property_tree/json_parser.hpp>
#include <boost/property_tree/ptree.hpp>
#include "flex/utils/app_utils.h"
#include "flex/engines/hqps/engine/context.h"
#include "flex/engines/hqps/engine/hqps_utils.h"

#include "flex/engines/hqps/engine/sync_engine.h"

namespace gs {

template <typename TAG_PROP>
class IC10Expression2 {
 public:
  using tag_prop_t = std::tuple<TAG_PROP>;
  IC10Expression2(int32_t month, TAG_PROP&& props)
      : month_(month), props_(props) {}

  bool operator()(int64_t time_stamp) const {
    auto millon_second = time_stamp / 1000;
    auto tm = gmtime((time_t*) (&millon_second));
    return (tm->tm_mon + 1 == month_ && tm->tm_mday >= 21) ||
           ((month_ <= 11 && tm->tm_mon == month_ && tm->tm_mday < 22) ||
            (tm->tm_mon == 0 && month_ == 12 && tm->tm_mday < 22));
  }

  tag_prop_t Properties() { return std::make_tuple(props_); }

 private:
  int32_t month_;
  TAG_PROP props_;
};

template <typename TAG_PROP>
class IC10Expression3 {
 public:
  using tag_prop_t = std::tuple<TAG_PROP>;
  IC10Expression3(oid_t oid, TAG_PROP&& props) : oid_(oid), props_(props) {}

  bool operator()(oid_t oid) const { return oid == oid_; }

  tag_prop_t Properties() const { return std::make_tuple(props_); }

 private:
  oid_t oid_;
  TAG_PROP props_;
};

template <typename TAG_PROP0, typename TAG_PROP1>
class IC10Expression4 {
 public:
  using result_t = int32_t;
  IC10Expression4(TAG_PROP0&& props0, TAG_PROP1&& props1)
      : props0_(props0), props1_(props1) {}
  using tag_prop_t = std::tuple<TAG_PROP0, TAG_PROP1>;

  int32_t operator()(int32_t a, int32_t b) { return a - b; }

  tag_prop_t Properties() const { return std::make_tuple(props0_, props1_); }

 private:
  TAG_PROP0 props0_;
  TAG_PROP1 props1_;
};

template <typename GRAPH_INTERFACE>
class IC10 {
  using oid_t = typename GRAPH_INTERFACE::outer_vertex_id_t;
  using vertex_id_t = typename GRAPH_INTERFACE::vertex_id_t;

  // static gs::oid_t oid_param = 6597069767117;
  std::string person_label = "person";
  std::string knows_label = "knows";
  std::string post_label = "post";
  std::string comment_label = "comment";
  std::string has_creator_label = "hasCreator";
  std::string reply_of_label = "replyOf";
  std::string forum_label = "forum";
  std::string likes_label = "likes";
  std::string has_member_label = "hasMember";
  std::string container_of_label = "containerOf";
  std::string tag_label = "tag";
  std::string has_tag_label = "hasTag";
  std::string has_interest_in_label = "hasInterest";
  std::string is_located_in_label = "isLocatedIn";
  std::string place_label = "place";
  // static std::string_view firstName = "Jack";

 public:
  void Query(const GRAPH_INTERFACE& graph, int64_t ts,
             const boost::property_tree::ptree& input,
             boost::property_tree::ptree& output) const {
    oid_t id = input.get<oid_t>("personIdQ10");
    int32_t month = input.get<int64_t>("month");
    int32_t limit = input.get<int32_t>("limit");

    std::vector<char> input_buffer, output_buffer;
    Encoder input_encoder(input_buffer);
    input_encoder.put_long(id);
    input_encoder.put_int(month);
    input_encoder.put_int(limit);
    Decoder input_decoder(input_buffer.data(), input_buffer.size());

    Encoder output_encoder(output_buffer);
    Query(graph, ts, input_decoder, output_encoder);

    Decoder output_decoder(output_buffer.data(), output_buffer.size());
    while (!output_decoder.empty()) {
      boost::property_tree::ptree node;
      node.put("personId", output_decoder.get_long());           // id
      node.put("personFirstName", output_decoder.get_string());  // post cnt
      node.put("personLastName", output_decoder.get_string());
      node.put("commonInterestScore", output_decoder.get_int());
      node.put("personGender", output_decoder.get_string());
      node.put("personCityName", output_decoder.get_string());

      output.push_back(std::make_pair("", node));
    }
  }
  void Query(const GRAPH_INTERFACE& graph, int64_t time_stamp, Decoder& input,
             Encoder& output) const {
    int64_t id = input.get_long();
    int32_t month = input.get_int();
    int32_t limit = 10;

    using label_id_t = typename GRAPH_INTERFACE::label_id_t;
    label_id_t person_label_id = graph.GetVertexLabelId(person_label);
    label_id_t knows_label_id = graph.GetEdgeLabelId(knows_label);
    label_id_t post_label_id = graph.GetVertexLabelId(post_label);
    label_id_t comment_label_id = graph.GetVertexLabelId(comment_label);
    label_id_t has_creator_label_id = graph.GetEdgeLabelId(has_creator_label);
    label_id_t reply_of_label_id = graph.GetEdgeLabelId(reply_of_label);
    label_id_t forum_label_id = graph.GetVertexLabelId(forum_label);
    label_id_t likes_label_id = graph.GetEdgeLabelId(likes_label);
    label_id_t has_member_label_id = graph.GetEdgeLabelId(has_member_label);
    label_id_t container_of_label_id = graph.GetEdgeLabelId(container_of_label);
    label_id_t tag_label_id = graph.GetVertexLabelId(tag_label);
    label_id_t has_tag_label_id = graph.GetEdgeLabelId(has_tag_label);
    label_id_t has_interest_in_label_id =
        graph.GetEdgeLabelId(has_interest_in_label);
    label_id_t is_located_in_label_id =
        graph.GetEdgeLabelId(is_located_in_label);
    label_id_t place_label_id = graph.GetVertexLabelId(place_label);

    using Engine = SyncEngine<GRAPH_INTERFACE>;
    auto ctx0 = Engine::template ScanVertexWithOid<-1>(time_stamp, graph,
                                                       person_label_id, id);
    // message

    // foaf
    auto edge_expand_opt = gs::make_edge_expandv_opt(
        gs::Direction::Both, knows_label_id, person_label_id);
    auto get_v_opt = gs::make_getv_opt(
        gs::VOpt::End, std::array<label_id_t, 1>{person_label_id});
    auto path_expand_opt = gs::make_path_expand_opt(
        std::move(edge_expand_opt), std::move(get_v_opt), gs::Range(2, 3));
    auto ctx1 = Engine::template PathExpandV<0, -1>(
        time_stamp, graph, std::move(ctx0), std::move(path_expand_opt));

    // filter person with birthday.
    gs::NamedProperty<int64_t> tag_prop("birthday");
    IC10Expression2 expr2(month, std::move(tag_prop));
    auto ctx4 =
        Engine::Select(time_stamp, graph, std::move(ctx1), std::move(expr2));

    LOG(INFO) << "after select: cnt: " << ctx4.GetHead().Size();
    for (auto iter : ctx4) {
      LOG(INFO) << gs::to_string(iter.GetAllElement());
    }
    vertex_id_t error_vid;
    if constexpr (GRAPH_INTERFACE::is_grape) {
      LOG(FATAL) << "not set";
    } else {
      error_vid = -4692869459758829936l;
    }

    // calc common score.
    auto lambda_x = [&](auto&& inner_ctx0) {
      auto edge_expand_opt1 = gs::make_edge_expandv_opt(
          gs::Direction::In, has_creator_label_id, post_label_id);
      //  post
      auto inner_ctx1 = Engine::template EdgeExpandV<1, 0>(
          time_stamp, graph, std::move(inner_ctx0),
          std::move(edge_expand_opt1));

      for (auto iter : inner_ctx1) {
        auto tuple = iter.GetAllElement();
        auto vid = std::get<0>(tuple);
        if (vid == error_vid) {
          LOG(INFO) << gs::to_string(tuple);
        }
      }

      auto edge_expand_opt2 = gs::make_edge_expandv_opt(
          gs::Direction::Out, has_tag_label_id, tag_label_id);
      //  tag
      auto inner_ctx2 = Engine::template EdgeExpandV<-1, 1>(
          time_stamp, graph, std::move(inner_ctx1),
          std::move(edge_expand_opt2));

      auto edge_expand_opt3 = gs::make_edge_expandv_opt(
          gs::Direction::In, has_interest_in_label_id, person_label_id);
      //  person
      auto inner_ctx3 = Engine::template EdgeExpandV<-1, -1>(
          time_stamp, graph, std::move(inner_ctx2),
          std::move(edge_expand_opt3));
      LOG(INFO) << "Before matching person";
      for (auto iter : inner_ctx3) {
        auto tuple = iter.GetAllElement();
        auto vid = std::get<0>(tuple);
        if (vid == error_vid) {
          LOG(INFO) << gs::to_string(tuple);
        }
      }

      // get v
      gs::NamedProperty<oid_t> id_prop2("id");
      IC10Expression3 expr3(id, std::move(id_prop2));
      auto get_v_opt4 = gs::make_getv_opt(
          gs::VOpt::Itself, std::array<label_id_t, 1>{person_label_id},
          std::move(expr3));
      auto inner_ctx4 = Engine::template GetV<-1, -1>(
          time_stamp, graph, std::move(inner_ctx3), std::move(get_v_opt4));

      LOG(INFO) << "after matching person";
      for (auto iter : inner_ctx4) {
        auto tuple = iter.GetAllElement();
        auto vid = std::get<0>(tuple);
        if (vid == error_vid) {
          LOG(INFO) << gs::to_string(tuple);
        }
      }

      // dedup post
      //  to dedup the record, we can just rely on offset array.
      // we need to iterator over the context, for each distinct tag_col value,
      // record the indices of later tags.
      // then subset each set with indices.
      // finially renew the offset array. later ones are one-2-one mapping, and
      // tag-1 <-> tag are based on the merge result of relative array.
      auto inner_ctx5 = Engine::template Dedup<1>(std::move(inner_ctx4));

      LOG(INFO) << "after dedup post";
      for (auto iter : inner_ctx5) {
        auto tuple = iter.GetAllElement();
        auto vid = std::get<0>(tuple);
        if (vid == error_vid) {
          LOG(INFO) << gs::to_string(tuple);
        }
      }

      // fold to get count.
      auto agg =
          gs::make_aggregate_prop<2, gs::AggFunc::COUNT, grape::EmptyType>(
              gs::PropNameArray<grape::EmptyType>{"None"},
              std::integer_sequence<int32_t, 1>{});
      auto fold_opt = gs::make_fold_opt(std::move(agg));
      return Engine::GroupByWithoutKey(time_stamp, graph, std::move(inner_ctx5),
                                       std::move(fold_opt));
    };

    auto ctx5 = Engine::template Apply<1, JoinKind::InnerJoin>(
        std::move(ctx4), std::move(lambda_x));

    gs::ProjectSelf<0, 2> other_person2;
    auto proj_opt2 = gs::make_project_opt(std::move(other_person2));
    auto ctx6 = Engine::template Project<true>(
        time_stamp, graph, std::move(ctx5), std::move(proj_opt2));

    // lambda in lambda
    auto lambda_y = [&](auto&& inner_ctx0) {
      auto edge_expand_opt7 = gs::make_edge_expandv_opt(
          gs::Direction::In, has_creator_label_id, post_label_id);
      //  post
      auto ctx7 = Engine::template EdgeExpandV<3, 2>(
          time_stamp, graph, std::move(inner_ctx0),
          std::move(edge_expand_opt7));

      auto lambda_yy = [&](auto&& iin_ctx0) {
        auto edge_expand_opt8 = gs::make_edge_expandv_opt(
            gs::Direction::Out, has_tag_label_id, tag_label_id);
        //  tag
        auto ctx8 = Engine::template EdgeExpandV<-1, 3>(
            time_stamp, graph, std::move(iin_ctx0),
            std::move(edge_expand_opt8));

        auto edge_expand_opt9 = gs::make_edge_expandv_opt(
            gs::Direction::In, has_interest_in_label_id, person_label_id);
        //  person
        auto ctx9 = Engine::template EdgeExpandV<-1, -1>(
            time_stamp, graph, std::move(ctx8), std::move(edge_expand_opt9));

        // get v
        gs::NamedProperty<oid_t> id_prop2("id");
        // eq id.
        IC10Expression3 expr10(id, std::move(id_prop2));
        auto get_v_opt10 = gs::make_getv_opt(
            gs::VOpt::Itself, std::array<label_id_t, 1>{person_label_id},
            std::move(expr10));

        return Engine::template GetV<-1, -1>(time_stamp, graph, std::move(ctx9),
                                             std::move(get_v_opt10));
      };
      auto ctx8 = Engine::template Apply<-1, JoinKind::AntiJoin>(
          std::move(ctx7), std::move(lambda_yy));

      auto ctx9 = Engine::template Dedup<3>(std::move(ctx8));

      auto agg =
          gs::make_aggregate_prop<4, gs::AggFunc::COUNT, grape::EmptyType>(
              gs::PropNameArray<grape::EmptyType>{"None"},
              std::integer_sequence<int32_t, 3>{});
      auto fold_opt = gs::make_fold_opt(std::move(agg));
      return Engine::GroupByWithoutKey(time_stamp, graph, std::move(ctx9),
                                       std::move(fold_opt));
    };

    auto ctx7 = Engine::template Apply<3, JoinKind::InnerJoin>(
        std::move(ctx6), std::move(lambda_y));

    // sort with

    // project with expression.

    IC10Expression4 expr4(gs::InnerIdProperty<1>{}, gs::InnerIdProperty<3>{});
    auto proj_expr = gs::make_project_expr<4, int32_t>(std::move(expr4));
    gs::ProjectSelf<0, 5> project_person;
    auto proj_opt =
        gs::make_project_opt(std::move(proj_expr), std::move(project_person));
    auto ctx8 = Engine::template Project<true>(
        time_stamp, graph, std::move(ctx7), std::move(proj_opt));

    gs::OrderingPropPair<gs::SortOrder::DESC, 4, int32_t> pair0(
        "none");  // the element itself.
    gs::OrderingPropPair<gs::SortOrder::ASC, 0, gs::oid_t> pair1("id");  // id
    // std::move(pair0),
    auto pairs = gs::make_sort_opt(gs::Range(0, limit), std::move(pair0),
                                   std::move(pair1));
    auto ctx9 =
        Engine::Sort(time_stamp, graph, std::move(ctx8), std::move(pairs));

    // get city
    auto edge_expand_opt10 = gs::make_edge_expandv_opt(
        gs::Direction::Out, is_located_in_label_id, place_label_id);
    auto ctx10 = Engine::template EdgeExpandV<6, 5>(
        time_stamp, graph, std::move(ctx9), std::move(edge_expand_opt10));

    gs::AliasTagProp<0, 7, oid_t, std::string_view, std::string_view,
                     std::string_view>
        prop_col1({"id", "firstName", "lastName", "gender"});
    gs::AliasTagProp<6, 8, std::string_view> prop_col2({"name"});
    auto proj_opt3 =
        gs::make_project_opt(std::move(prop_col1), std::move(prop_col2));
    auto ctx11 = Engine::template Project<true>(
        time_stamp, graph, std::move(ctx10), std::move(proj_opt3));

    {
      for (auto iter : ctx11) {
        auto tuple = iter.GetAllElement();
        LOG(INFO) << "tuple: " << gs::to_string(tuple);
        auto& person = std::get<7>(tuple);
        output.put_long(std::get<0>(person));
        output.put_string_view(std::get<1>(person));
        output.put_string_view(std::get<2>(person));
        output.put_int(std::get<4>(tuple));
        output.put_string_view(std::get<3>(person));
        output.put_string_view(std::get<0>(std::get<8>(tuple)));
      }
    }

    // dedup post and count

    // project the substraction
  }
};  // namespace gs
}  // namespace gs

#endif  // ENGINES_HPQS_APP_EXAMPLE_IC_IC10_H_
