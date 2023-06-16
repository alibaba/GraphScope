#ifndef ENGINES_HPQS_APP_EXAMPLE_IC_IC1_H_
#define ENGINES_HPQS_APP_EXAMPLE_IC_IC1_H_

#include <boost/property_tree/json_parser.hpp>
#include <boost/property_tree/ptree.hpp>
#include "flex/engines/hqps/engine/context.h"
#include "flex/engines/hqps/engine/hqps_utils.h"
#include "flex/engines/hqps/engine/sync_engine.h"
#include "flex/utils/app_utils.h"

namespace gs {

template <typename TAG_COL>
class IC1Expression2 {
 public:
  IC1Expression2(std::string_view param1, TAG_COL&& props)
      : param1_(param1), props_(props) {}

  template <typename TUPLE_T>
  inline bool operator()(TUPLE_T& props) {
    return param1_ == props;
  }

  auto Properties() { return std::make_tuple(props_); }

 private:
  std::string_view param1_;
  TAG_COL props_;
};

template <typename TAG_COL0, typename TAG_COL1, typename TAG_COL2,
          typename TAG_COL3>
class IC1Expression3 {
 public:
  IC1Expression3(TAG_COL0&& prop0, TAG_COL1&& prop1, TAG_COL2&& prop2,
                 TAG_COL3&& prop3)
      : prop0_(prop0), prop1_(prop1), prop2_(prop2), prop3_(prop3) {}

  template <typename TUPLE_T>
  inline auto operator()(const TUPLE_T& edge_tuple,
                         const std::string_view& com_name,
                         const int32_t& work_from,
                         const std::string_view& city_name) const {
    if (edge_tuple == NONE) {
      return NullRecordCreator<
          std::tuple<std::string_view, int32_t, std::string_view>>::GetNull();
    } else {
      return std::tuple<std::string_view, int32_t, std::string_view>{
          com_name, work_from, city_name};
    }
  }

  auto Properties() { return std::make_tuple(prop0_, prop1_, prop2_, prop3_); }

 private:
  std::string_view param1_;
  TAG_COL0 prop0_;
  TAG_COL1 prop1_;
  TAG_COL2 prop2_;
  TAG_COL3 prop3_;
};

template <typename TAG_COL0, typename TAG_COL1, typename TAG_COL2,
          typename TAG_COL3>
class IC1Expression4 {
 public:
  IC1Expression4(TAG_COL0&& prop0, TAG_COL1&& prop1, TAG_COL2&& prop2,
                 TAG_COL3&& prop3)
      : prop0_(prop0), prop1_(prop1), prop2_(prop2), prop3_(prop3) {}

  template <typename TUPLE_T>
  inline auto operator()(const TUPLE_T& edge_tuple,
                         const std::string_view& com_name,
                         const int32_t& work_from,
                         const std::string_view& city_name) const {
    LOG(INFO) << "edge_tuple: " << gs::to_string(edge_tuple);
    LOG(INFO) << "com_name: " << com_name << ", work_from: " << work_from
              << ", city_name: " << city_name;
    if (edge_tuple == NONE) {
      return NullRecordCreator<
          std::tuple<std::string_view, int32_t, std::string_view>>::GetNull();
    } else {
      return std::tuple<std::string_view, int32_t, std::string_view>{
          com_name, work_from, city_name};
    }
  }

  auto Properties() { return std::make_tuple(prop0_, prop1_, prop2_, prop3_); }

 private:
  std::string_view param1_;
  TAG_COL0 prop0_;
  TAG_COL1 prop1_;
  TAG_COL2 prop2_;
  TAG_COL3 prop3_;
};

template <typename Out>
void split(const std::string_view& s, char delim, Out result) {
  std::string str(s.data(), s.size());
  std::istringstream iss(str);
  std::string item;
  while (std::getline(iss, item, delim)) {
    *result++ = item;
  }
}

std::vector<std::string> split(const std::string_view& s, char delim) {
  std::vector<std::string> elems;
  split(s, delim, std::back_inserter(elems));
  return elems;
}

template <typename GRAPH_INTERFACE>
class IC1 {
  using oid_t = typename GRAPH_INTERFACE::outer_vertex_id_t;
  using vertex_id_t = typename GRAPH_INTERFACE::vertex_id_t;

  // static gs::oid_t oid_param = 6597069767117;
  std::string person_label = "person";
  std::string knows_label = "knows";
  std::string isLocatedIn_label = "isLocatedIn";
  std::string place_label = "place";
  std::string workAt_label = "workAt";
  std::string studyAt_label = "studyAt";

  std::string org_label = "organisation";
  // static std::string_view firstName = "Jack";

  using Engine = SyncEngine<GRAPH_INTERFACE>;

 public:
  void Query(const GRAPH_INTERFACE& graph, int64_t ts,
             const boost::property_tree::ptree& input,
             boost::property_tree::ptree& output) const {
    oid_t id = input.get<oid_t>("personIdQ1");
    std::string firstName = input.get<std::string>("firstName");

    std::vector<char> input_buffer, output_buffer;
    Encoder input_encoder(input_buffer);
    input_encoder.put_long(id);
    input_encoder.put_string(firstName);
    Decoder input_decoder(input_buffer.data(), input_buffer.size());

    Encoder output_encoder(output_buffer);
    Query(graph, ts, input_decoder, output_encoder);

    Decoder output_decoder(output_buffer.data(), output_buffer.size());
    while (!output_decoder.empty()) {
      boost::property_tree::ptree node;
      node.put("friendId", output_decoder.get_long());           // id
      node.put("friendLastName", output_decoder.get_string());   // lastName"
      node.put("distanceFromPerson", output_decoder.get_int());  // dist
      node.put("friendBirthday", output_decoder.get_long());     // birthday
      node.put("friendCreationDate",
               output_decoder.get_long());  // creationDate
      node.put("friendGender",
               output_decoder.get_string());  // gender
      node.put("friendBrowserUsed",
               output_decoder.get_string());                      // browserUsed
      node.put("friendLocationIp", output_decoder.get_string());  // locationIP

      boost::property_tree::ptree emails_node;
      std::vector<std::string> emails_list =
          split(output_decoder.get_string(), ';');
      for (auto& str : emails_list) {
        emails_node.push_back(
            std::make_pair("", boost::property_tree::ptree(str)));
      }
      node.add_child("friendEmails", emails_node);

      boost::property_tree::ptree languages_node;
      std::vector<std::string> languages_list =
          split(output_decoder.get_string(), ';');
      for (auto& str : languages_list) {
        languages_node.push_back(
            std::make_pair("", boost::property_tree::ptree(str)));
      }
      node.add_child("friendLanguages", languages_node);
      node.put("friendCityName", output_decoder.get_string());  // cityName

      boost::property_tree::ptree univs_node;
      int univs_num = output_decoder.get_int();
      for (int i = 0; i < univs_num; ++i) {
        boost::property_tree::ptree univ_node;
        univ_node.put("organizationName",
                      output_decoder.get_string());       // "universityName"
        univ_node.put("year", output_decoder.get_int());  // "universityYear"
        univ_node.put("placeName", output_decoder.get_string());

        univs_node.push_back(std::make_pair("", univ_node));
      }
      node.add_child("friendUniversities", univs_node);

      boost::property_tree::ptree companies_node;
      int companies_num = output_decoder.get_int();
      for (int i = 0; i < companies_num; ++i) {
        boost::property_tree::ptree company_node;
        company_node.put("organizationName",
                         output_decoder.get_string());       // "companyName"
        company_node.put("year", output_decoder.get_int());  // "companyYear"
        company_node.put("placeName", output_decoder.get_string());

        companies_node.push_back(std::make_pair("", company_node));
      }
      node.add_child("friendCompanies", companies_node);

      output.push_back(std::make_pair("", node));
    }
  }
  void Query(const GRAPH_INTERFACE& graph, int64_t time_stamp, Decoder& input,
             Encoder& output) const {
    using Engine = SyncEngine<GRAPH_INTERFACE>;
    using label_id_t = typename GRAPH_INTERFACE::label_id_t;
    label_id_t person_label_id = graph.GetVertexLabelId(person_label);
    label_id_t knows_label_id = graph.GetEdgeLabelId(knows_label);
    label_id_t isLocatedIn_label_id = graph.GetEdgeLabelId(isLocatedIn_label);
    label_id_t place_label_id = graph.GetVertexLabelId(place_label);
    label_id_t workAt_label_id = graph.GetEdgeLabelId(workAt_label);
    label_id_t studyAt_label_id = graph.GetEdgeLabelId(studyAt_label);
    label_id_t org_label_id = graph.GetVertexLabelId(org_label);

    int64_t id = input.get_long();
    std::string_view firstName = input.get_string();
    LOG(INFO) << "start ic1 query with: " << id << ", " << firstName;

    auto ctx0 = Engine::template ScanVertexWithOid<0>(time_stamp, graph,
                                                      person_label_id, id);

    double t1 = -grape::GetCurrentTime();
    auto edge_expand_opt = gs::make_edge_expand_opt(
        gs::Direction::Both, knows_label_id, person_label_id);
    auto get_v_opt = gs::make_getv_opt(
        gs::VOpt::End, std::array<label_id_t, 1>{person_label_id});
    auto path_expand_opt = gs::make_path_expand_opt(
        std::move(edge_expand_opt), std::move(get_v_opt), gs::Range(1, 4));
    auto ctx1 = Engine::template PathExpandV<-1, 0>(
        time_stamp, graph, std::move(ctx0), std::move(path_expand_opt));

    IC1Expression2 expr3(firstName,
                         gs::NamedProperty<std::string_view>("firstName"));
    auto get_v_opt3 = gs::make_getv_opt(
        gs::VOpt::Itself, std::array<label_id_t, 1>{person_label_id},
        std::move(expr3));
    auto ctx3 = Engine::template GetV<1, -1>(time_stamp, graph, std::move(ctx1),
                                             std::move(get_v_opt3));
    // // otherPerson -(isLocatedIn) -city
    // auto edge_expand_opt4 = gs::make_edge_expand_opt<>(
    //     gs::Direction::Out, isLocatedIn_label_id, place_label_id);
    // auto ctx4 = Engine::template EdgeExpandV<2, 1>(
    //     time_stamp, graph, std::move(ctx3), std::move(edge_expand_opt4));

    // delete start person
    auto project_opt0 = gs::make_project_opt(gs::ProjectSelf<1, 0>());
    auto ctx5 = Engine::template Project<0>(time_stamp, graph, std::move(ctx3),
                                            std::move(project_opt0));

    // copy ctx4 for two outer join
    auto ctx5_1(ctx5);
    for (auto iter : ctx5_1) {
      auto data = iter.GetAllData();
      LOG(INFO) << "data: " << gs::to_string(data);
    }

    // workAt(company)->islocatedIn
    auto edge_expand_opt5 = gs::make_edge_expande_opt<int32_t>(
        gs::PropNameArray<int32_t>{"workFrom"}, gs::Direction::Out,
        workAt_label_id, org_label_id);
    auto ctx6 = Engine::template EdgeExpandE<1, 0>(
        time_stamp, graph, std::move(ctx5_1), std::move(edge_expand_opt5));

    auto get_v_opt6 = gs::make_getv_opt(
        gs::VOpt::End, std::array<label_id_t, 1>{org_label_id});
    auto ctx7 = Engine::template GetV<2, -1>(time_stamp, graph, std::move(ctx6),
                                             std::move(get_v_opt6));

    auto edge_expand_opt7 = gs::make_edge_expand_opt(
        gs::Direction::Out, isLocatedIn_label_id, place_label_id);
    auto ctx8 = Engine::template EdgeExpandV<3, -1>(
        time_stamp, graph, std::move(ctx7), std::move(edge_expand_opt7));

    // LeftOuterJoin first
    auto ctx9 = Engine::template Join<0, 0, JoinKind::LeftOuterJoin>(
        std::move(ctx5), std::move(ctx8));
    // otherPerson, workAtCompanyEdge, company,
    // workAtCountry

    for (auto iter : ctx9) {
      auto eles = iter.GetAllElement();
      LOG(INFO) << gs::to_string(eles);
    }

    auto proj_opt1 = gs::make_project_opt(
        gs::ProjectSelf<0, 0>(),
        gs::make_project_expr<
            1, std::tuple<std::string_view, int32_t, std::string_view>>(
            IC1Expression3(gs::InnerIdProperty<1>(),
                           gs::NamedProperty<std::string_view, 2>("name"),
                           gs::NamedProperty<int32_t, 1>("workFrom"),
                           gs::NamedProperty<std::string_view, 3>("name"))));
    auto ctx10 = Engine::template Project<false>(
        time_stamp, graph, std::move(ctx9), std::move(proj_opt1));

    // collect after project
    auto group_opt = gs::make_group_opt(
        gs::AliasTagProp<0, 0, grape::EmptyType>({"None"}),
        gs::make_aggregate_prop<1, gs::AggFunc::TO_LIST, grape::EmptyType>(
            {"None"}, std::integer_sequence<int32_t, 1>{}));
    auto ctx10_1 = Engine::template GroupBy(time_stamp, graph, std::move(ctx10),
                                            std::move(group_opt));

    for (auto iter : ctx10_1) {
      auto eles = iter.GetAllElement();
      LOG(INFO) << gs::to_string(eles);
    }

    for (auto iter : ctx10_1) {
      auto data = iter.GetAllData();
      LOG(INFO) << "after group : " << gs::to_string(data);
    }

    // studyAt(university)->islocatedIn
    // otherPerson,  companyInfo
    auto ctx10_2(ctx10_1);

    auto edge_expand_opt8 = gs::make_edge_expande_opt<int32_t>(
        gs::PropNameArray<int32_t>{"classYear"}, gs::Direction::Out,
        studyAt_label_id, org_label_id);
    auto ctx11 = Engine::template EdgeExpandE<2, 0>(
        time_stamp, graph, std::move(ctx10_2), std::move(edge_expand_opt8));

    auto get_v_opt9 = gs::make_getv_opt(
        gs::VOpt::End, std::array<label_id_t, 1>{org_label_id});
    auto ctx12 = Engine::template GetV<3, -1>(
        time_stamp, graph, std::move(ctx11), std::move(get_v_opt9));

    auto edge_expand_opt10 = gs::make_edge_expand_opt(
        gs::Direction::Out, isLocatedIn_label_id, place_label_id);
    auto ctx13 = Engine::template EdgeExpandV<4, -1>(
        time_stamp, graph, std::move(ctx12), std::move(edge_expand_opt10));

    // second left outer join
    auto ctx14 = Engine::template Join<0, 1, 0, 1, JoinKind::LeftOuterJoin>(
        std::move(ctx10_1), std::move(ctx13));

    for (auto iter : ctx14) {
      auto eles = iter.GetAllElement();
      LOG(INFO) << gs::to_string(eles);
    }
    // otherPerson, companyInfo, studyAtEdge, university, univCity
    LOG(INFO) << "Before project";

    auto proj_opt2 = gs::make_project_opt(
        gs::ProjectSelf<0, 0>(), gs::ProjectSelf<1, 1>(),
        gs::make_project_expr<
            2, std::tuple<std::string_view, int32_t, std::string_view>>(
            IC1Expression4(gs::InnerIdProperty<2>(),
                           gs::NamedProperty<std::string_view, 3>("name"),
                           gs::NamedProperty<int32_t, 2>("classYear"),
                           gs::NamedProperty<std::string_view, 4>("name"))));
    auto ctx15 = Engine::template Project<false>(
        time_stamp, graph, std::move(ctx14), std::move(proj_opt2));

    for (auto iter : ctx15) {
      auto eles = iter.GetAllElement();
      LOG(INFO) << "after project: " << gs::to_string(eles);
    }

    // collect after project
    // group by two keys
    auto group_opt2 = gs::make_group_opt2(
        gs::AliasTagProp<0, 0, grape::EmptyType>({"None"}),  // key2
        gs::AliasTagProp<1, 1, grape::EmptyType>({"None"}),  // key1
        gs::make_aggregate_prop<2, gs::AggFunc::TO_LIST, grape::EmptyType>(
            {"None"}, std::integer_sequence<int32_t, 2>{}));
    auto ctx16 = Engine::template GroupBy(time_stamp, graph, std::move(ctx15),
                                          std::move(group_opt2));

    for (auto iter : ctx16) {
      auto eles = iter.GetAllElement();
      LOG(INFO) << "after project: " << gs::to_string(eles);
    }

    // reach out for locationCity
    auto edge_expand_opt11 = gs::make_edge_expand_opt(
        gs::Direction::Out, isLocatedIn_label_id, place_label_id);
    auto ctx17 = Engine::template EdgeExpandV<3, 0>(
        time_stamp, graph, std::move(ctx16), std::move(edge_expand_opt11));

    gs::OrderingPropPair<gs::SortOrder::ASC, 0, Dist> pair0("dist");  // dist
    gs::OrderingPropPair<gs::SortOrder::ASC, 0, std::string_view> pair1(
        "lastName");  // lastName
    gs::OrderingPropPair<gs::SortOrder::ASC, 0, gs::oid_t> pair2(
        "id");  // id auto
    auto sort_opt = gs::make_sort_opt(gs::Range(0, 20), std::move(pair0),
                                      std::move(pair1), std::move(pair2));
    auto ctx18 = Engine::template Sort(time_stamp, graph, std::move(ctx17),
                                       std::move(sort_opt));

    for (auto iter : ctx18) {
      auto data = iter.GetAllData();
      LOG(INFO) << "after sort : " << gs::to_string(data);
    }

    // final project
    auto proj_opt3 = gs::make_project_opt(
        gs::AliasTagProp<0, 0, gs::oid_t>({"id"}),
        gs::AliasTagProp<0, 1, std::string_view>({"lastName"}),
        gs::AliasTagProp<0, 2, int32_t>({"dist"}),
        gs::AliasTagProp<0, 3, int64_t>({"birthday"}),
        gs::AliasTagProp<0, 4, int64_t>({"creationDate"}),
        gs::AliasTagProp<0, 5, std::string_view>({"gender"}),
        gs::AliasTagProp<0, 6, std::string_view>({"browserUsed"}),
        gs::AliasTagProp<0, 7, std::string_view>({"locationIP"}),
        gs::AliasTagProp<0, 8, std::string_view>({"email"}),
        gs::AliasTagProp<0, 9, std::string_view>({"language"}),
        gs::AliasTagProp<3, 10, std::string_view>({"name"}),
        gs::ProjectSelf<1, 11>(), gs::ProjectSelf<2, 12>());
    auto ctx19 = Engine::template Project<false>(
        time_stamp, graph, std::move(ctx18), std::move(proj_opt3));

    LOG(INFO) << "End";
    for (auto iter : ctx19) {
      auto tup = iter.GetAllElement();
      LOG(INFO) << gs::to_string(tup);
      output.put_long(std::get<0>(tup));
      output.put_string_view(std::get<1>(tup));
      output.put_int(std::get<2>(tup));
      output.put_long(std::get<3>(tup));
      output.put_long(std::get<4>(tup));
      output.put_string_view(std::get<5>(tup));
      output.put_string_view(std::get<6>(tup));
      output.put_string_view(std::get<7>(tup));
      output.put_string_view(std::get<8>(tup));
      output.put_string_view(std::get<9>(tup));
      output.put_string_view(std::get<10>(tup));

      auto& univs = std::get<12>(tup);
      output.put_int(univs.size());
      for (auto& u : univs) {
        output.put_string_view(std::get<0>(u));  // name
        output.put_int(std::get<1>(u));          // classYear
        output.put_string_view(std::get<2>(u));  // city
      }

      auto& companies = std::get<11>(tup);
      output.put_int(companies.size());
      for (auto& c : companies) {
        output.put_string_view(std::get<0>(c));  // name
        output.put_int(std::get<1>(c));          // classYear
        output.put_string_view(std::get<2>(c));  // country
      }
    }
    LOG(INFO) << "Finish query";
  }
};

}  // namespace gs

#endif  // ENGINES_HPQS_APP_EXAMPLE_IC_IC1_H_
