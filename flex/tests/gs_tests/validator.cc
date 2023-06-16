#include "flex/utils/app_utils.h"
#include "flex/storages/mutable_csr/grape_graph_interface.h"

#include "flex/engines/hqps/app/example/is/is1.h"
#include "flex/engines/hqps/app/example/is/is2.h"
#include "flex/engines/hqps/app/example/is/is3.h"
#include "flex/engines/hqps/app/example/is/is4.h"
#include "flex/engines/hqps/app/example/is/is5.h"
#include "flex/engines/hqps/app/example/is/is6.h"
#include "flex/engines/hqps/app/example/is/is7.h"

#include "flex/engines/hqps/app/example/ic/ic1.h"
#include "flex/engines/hqps/app/example/ic/ic10.h"
#include "flex/engines/hqps/app/example/ic/ic11.h"
#include "flex/engines/hqps/app/example/ic/ic12.h"
#include "flex/engines/hqps/app/example/ic/ic13.h"
#include "flex/engines/hqps/app/example/ic/ic14.h"
#include "flex/engines/hqps/app/example/ic/ic2.h"
#include "flex/engines/hqps/app/example/ic/ic3.h"
#include "flex/engines/hqps/app/example/ic/ic4.h"
#include "flex/engines/hqps/app/example/ic/ic5.h"
#include "flex/engines/hqps/app/example/ic/ic6.h"
#include "flex/engines/hqps/app/example/ic/ic7.h"
#include "flex/engines/hqps/app/example/ic/ic7.h"
#include "flex/engines/hqps/app/example/ic/ic8.h"
#include "flex/engines/hqps/app/example/ic/ic9.h"


#include <boost/property_tree/json_parser.hpp>
#include <boost/property_tree/ptree.hpp>

void convert_ptree(const boost::property_tree::ptree& pt,
                   const std::string& prefix,
                   std::vector<std::pair<std::string, std::string>>& vec) {
  if (pt.empty()) {
    vec.emplace_back(prefix, pt.data());
  } else {
    for (auto& pair : pt) {
      auto new_prefix = prefix + "|" + pair.first;
      convert_ptree(pair.second, new_prefix, vec);
    }
  }
}

bool same_property_tree(const boost::property_tree::ptree& lhs,
                        const boost::property_tree::ptree& rhs) {
  std::vector<std::pair<std::string, std::string>> lhs_vec, rhs_vec;
  convert_ptree(lhs, "", lhs_vec);
  convert_ptree(rhs, "", rhs_vec);
  std::sort(lhs_vec.begin(), lhs_vec.end());
  std::sort(rhs_vec.begin(), rhs_vec.end());
  return lhs_vec == rhs_vec;
}

template <typename GRAPH_INTERFACE, typename APP_T>
void validate(const GRAPH_INTERFACE& graph, const std::string& filename,
              size_t max_times, bool ignore_mis_match = false) {
  APP_T app;
  auto ts = std::numeric_limits<int64_t>::max() - 1;

  FILE* fin = fopen(filename.c_str(), "r");
  const int kMaxLineSize = 1048576;
  char line[kMaxLineSize];
  double t0 = -grape::GetCurrentTime();
  size_t cnt = 0;
  while (fgets(line, kMaxLineSize, fin) && cnt < max_times) {
    char* ptr = strchr(line, '|');

    std::string input_str(line, ptr - line);
    std::string output_str(ptr + 1, strnlen(ptr + 1, kMaxLineSize));

    std::stringstream ssinput(input_str);
    std::stringstream ssexpected_output(output_str);

    boost::property_tree::ptree ssinput_pt;
    boost::property_tree::read_json(ssinput, ssinput_pt);

    boost::property_tree::ptree ssexpected_output_pt;
    boost::property_tree::read_json(ssexpected_output, ssexpected_output_pt);

    boost::property_tree::ptree ssoutput_pt;
    app.Query(graph, ts, ssinput_pt, ssoutput_pt);
    if (!same_property_tree(ssoutput_pt, ssexpected_output_pt)) {
      LOG(INFO) << "Wrong answer when validating " << filename
                << "on case: " << cnt;
      LOG(INFO) << "Input: ";
      std::stringstream input_ss;
      boost::property_tree::json_parser::write_json(input_ss, ssinput_pt);
      LOG(INFO) << input_ss.str();
      LOG(INFO) << "Output: ";
      std::stringstream output_ss;
      boost::property_tree::json_parser::write_json(output_ss, ssoutput_pt);
      LOG(INFO) << output_ss.str();
      LOG(INFO) << "Expected output: ";
      std::stringstream expected_output_ss;
      boost::property_tree::json_parser::write_json(expected_output_ss,
                                                    ssexpected_output_pt);
      LOG(INFO) << expected_output_ss.str();
      if (ignore_mis_match) {
        LOG(INFO) << "Ignore mis-match";
      } else {
        LOG(FATAL) << "Exited";
      }
      // LOG(FATAL) << "Exited";
    } else {
      LOG(INFO) << "Correct answer when validating <" << input_str << ">";
    }

    cnt += 1;
  }

  t0 += grape::GetCurrentTime();
  LOG(INFO) << "validate: " << filename << " times: " << cnt
            << ", avg time : " << (t0 / cnt);
}

template <typename GRAPH_INTERFACE>
void validate_all(const GRAPH_INTERFACE& graph,
                  const std::string& validate_dir) {
  int max_times = 10;
  // validate<GRAPH_INTERFACE, gs::IC1<GRAPH_INTERFACE>>(
  //     graph, validate_dir + "/" + "validation_params_ic1.csv", max_times);
  // LOG(INFO) << "Finish IC1 test";

  // validate<GRAPH_INTERFACE, gs::IC2<GRAPH_INTERFACE>>(
  //     graph, validate_dir + "/" + "validation_params_ic2.csv", max_times);
  // LOG(INFO) << "Finish IC2 test";

  // validate<GRAPH_INTERFACE, gs::IC3<GRAPH_INTERFACE>>(
  //     graph, validate_dir + "/" + "validation_params_ic3.csv", max_times);
  // LOG(INFO) << "Finish IC3 test";

  // validate<GRAPH_INTERFACE, gs::IC4<GRAPH_INTERFACE>>(
  //     graph, validate_dir + "/" + "validation_params_ic4.csv", max_times);
  // LOG(INFO) << "Finish IC4 test";

  // validate<GRAPH_INTERFACE, gs::IC5<GRAPH_INTERFACE>>(
  //     graph, validate_dir + "/" + "validation_params_ic5.csv", max_times);
  // LOG(INFO) << "Finish IC5 test";

  // validate<GRAPH_INTERFACE, gs::IC6<GRAPH_INTERFACE>>(
  //     graph, validate_dir + "/" + "validation_params_ic6.csv", max_times);
  // LOG(INFO) << "Finish IC6 test";

  // validate<GRAPH_INTERFACE, gs::IC7<GRAPH_INTERFACE>>(
  //     graph, validate_dir + "/" + "validation_params_ic7.csv", max_times);
  // LOG(INFO) << "Finish IC7 test";

  // validate<GRAPH_INTERFACE, gs::IC8<GRAPH_INTERFACE>>(
  //     graph, validate_dir + "/" + "validation_params_ic8.csv", max_times);
  // LOG(INFO) << "Finish IC8 test";

  // validate<GRAPH_INTERFACE, gs::IC9<GRAPH_INTERFACE>>(
  //     graph, validate_dir + "/" + "validation_params_ic9.csv", max_times);
  // LOG(INFO) << "Finish IC9 test";

  // validate<GRAPH_INTERFACE, gs::IC10<GRAPH_INTERFACE>>(
  //     graph, validate_dir + "/" + "validation_params_ic10.csv", max_times);
  // LOG(INFO) << "Finish IC10 test";

  // validate<GRAPH_INTERFACE, gs::IC11<GRAPH_INTERFACE>>(
  //     graph, validate_dir + "/" + "validation_params_ic11.csv", max_times);
  // LOG(INFO) << "Finish IC11 test";

  // validate<GRAPH_INTERFACE, gs::IC12<GRAPH_INTERFACE>>(
  //     graph, validate_dir + "/" + "validation_params_ic12.csv", max_times);
  // LOG(INFO) << "Finish IC12 test";

  // validate<GRAPH_INTERFACE, gs::IC13<GRAPH_INTERFACE>>(
  //     graph, validate_dir + "/" + "validation_params_ic13.csv", max_times);
  // LOG(INFO) << "Finish IC13 test";

  // validate<GRAPH_INTERFACE, gs::IC14<GRAPH_INTERFACE>>(
  //     graph, validate_dir + "/" + "validation_params_ic14.csv", max_times);
  // LOG(INFO) << "Finish IC14 test";
}

template <typename GRAPH_INTERFACE>
void validate_codegen(const GRAPH_INTERFACE& graph,
                      const std::string& validate_dir) {
  int max_times = 100;
  // validate<GRAPH_INTERFACE, gs::IS1<GRAPH_INTERFACE>>(
  //     graph, validate_dir + "/" + "validation_params_is1.csv", max_times);
  // LOG(INFO) << "Finish IS1 test";

  // validate<GRAPH_INTERFACE, gs::IS2<GRAPH_INTERFACE>>(
  //     graph, validate_dir + "/" + "validation_params_is2.csv", max_times);
  // LOG(INFO) << "Finish IS2 test";

  // validate<GRAPH_INTERFACE, gs::IS3<GRAPH_INTERFACE>>(
  //     graph, validate_dir + "/" + "validation_params_is3.csv", max_times);
  // LOG(INFO) << "Finish IS3 test";

  // validate<GRAPH_INTERFACE, gs::IS4<GRAPH_INTERFACE>>(
  //     graph, validate_dir + "/" + "validation_params_is4.csv", max_times);
  // LOG(INFO) << "Finish IS4 test";

  // validate<GRAPH_INTERFACE, gs::IS5<GRAPH_INTERFACE>>(
  //     graph, validate_dir + "/" + "validation_params_is5.csv", max_times);
  // LOG(INFO) << "Finish IS5 test";

  // validate<GRAPH_INTERFACE, gs::IS6<GRAPH_INTERFACE>>(
  //     graph, validate_dir + "/" + "validation_params_is6.csv", max_times);
  // LOG(INFO) << "Finish IS6 test";

  // validate<GRAPH_INTERFACE, gs::IS7<GRAPH_INTERFACE>>(
  //     graph, validate_dir + "/" + "validation_params_is7.csv", max_times);
  // LOG(INFO) << "Finish IS7 test";

  validate<GRAPH_INTERFACE, gs::IC1<GRAPH_INTERFACE>>(
      graph, validate_dir + "/" + "validation_params_ic1.csv", max_times);
  LOG(INFO) << "Finish IC1 test";

  // validate<GRAPH_INTERFACE, gs::QueryIC2<GRAPH_INTERFACE>>(
  //     graph, validate_dir + "/" + "validation_params_ic2.csv", max_times);

  // validate<GRAPH_INTERFACE, gs::QueryIC3<GRAPH_INTERFACE>>(
  //     graph, validate_dir + "/" + "validation_params_ic3.csv", max_times);
  // LOG(INFO) << "Finish IC3 test";

  // validate<GRAPH_INTERFACE, gs::QueryIC4<GRAPH_INTERFACE>>(
  //     graph, validate_dir + "/" + "validation_params_ic4.csv", max_times);
  // LOG(INFO) << "Finish IC4 test";

  // validate<GRAPH_INTERFACE, gs::QueryIC5<GRAPH_INTERFACE>>(
  //     graph, validate_dir + "/" + "validation_params_ic5.csv", max_times);
  // LOG(INFO) << "Finish IC5 test";

  // validate<GRAPH_INTERFACE, gs::QueryIC6<GRAPH_INTERFACE>>(
  //     graph, validate_dir + "/" + "validation_params_ic6.csv", max_times,
  //     true);
  // LOG(INFO) << "Finish IC6 test";

  validate<GRAPH_INTERFACE, gs::QueryIC7<GRAPH_INTERFACE>>(
      graph, validate_dir + "/" + "validation_params_ic7.csv", max_times);
  LOG(INFO) << "Finish IC7 test";

  // validate<GRAPH_INTERFACE, gs::QueryIC8<GRAPH_INTERFACE>>(
  //     graph, validate_dir + "/" + "validation_params_ic8.csv", max_times);
  // LOG(INFO) << "Finish IC8 test";

  // validate<GRAPH_INTERFACE, gs::QueryIC9<GRAPH_INTERFACE>>(
  //     graph, validate_dir + "/" + "validation_params_ic9.csv", max_times);
  // LOG(INFO) << "Finish IC9 test";

  // validate<GRAPH_INTERFACE, gs::QueryIC11<GRAPH_INTERFACE>>(
  //     graph, validate_dir + "/" + "validation_params_ic11.csv", max_times);
  // LOG(INFO) << "Finish IC11 test";

  // validate<GRAPH_INTERFACE, gs::QueryIC12<GRAPH_INTERFACE>>(
  //     graph, validate_dir + "/" + "validation_params_ic12.csv", max_times);
  // LOG(INFO) << "Finish IC12 test";
}

int main(int argc, char** argv) {
  if (argc < 3) {
    LOG(INFO) << "Usage: " << argv[0] << " <validate_dir> <work_dir>";
    return 0;
  }

  std::string validate_dir = argv[1];
  std::string work_dir = argv[2];

  gs::GrapeGraphInterface graph;
  graph.Open(work_dir);
  // validate_all(graph, validate_dir);
  validate_codegen(graph, validate_dir);


  return 0;
}
