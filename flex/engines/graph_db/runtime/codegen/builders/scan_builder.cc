#include "flex/engines/graph_db/runtime/codegen/builders/builders.h"

namespace gs {
namespace runtime {
class ScanBuilder {
 public:
  ScanBuilder(BuildingContext& context) : context_(context) {}

  bool is_find_vertex(const physical::Scan& scan_opr, label_t& label,
                      int64_t& vertex_id, int& alias, bool& scan_oid,
                      std::string& expr_name, std::string& expr_str) const {
    if (scan_opr.scan_opt() != physical::Scan::VERTEX) {
      return false;
    }
    if (scan_opr.has_alias()) {
      alias = scan_opr.alias().value();
    } else {
      alias = -1;
    }
    if (!scan_opr.has_params()) {
      return false;
    }

    const auto& params = scan_opr.params();
    if (params.tables_size() != 1) {
      return false;
    }
    const auto& table = params.tables(0);
    label = static_cast<int>(table.id());
    if (!scan_opr.has_idx_predicate()) {
      return false;
    }
    const auto& idx_predicate = scan_opr.idx_predicate();
    if (idx_predicate.or_predicates_size() != 1) {
      return false;
    }

    if (idx_predicate.or_predicates(0).predicates_size() != 1) {
      return false;
    }
    const auto& predicate = idx_predicate.or_predicates(0).predicates(0);
    if (!predicate.has_key()) {
      return false;
    }
    auto key = predicate.key();
    if (key.has_key()) {
      scan_oid = true;
    } else if (key.has_id()) {
      // scan gid
      scan_oid = false;
    } else {
      LOG(FATAL) << "Invalid key type";
    }
    switch (predicate.value_case()) {
    case algebra::IndexPredicate_Triplet::ValueCase::kConst: {
      RTAnyType type;
      std::tie(expr_str, expr_name, type) =
          value_pb_2_str(context_, predicate.const_());
      break;
    }
    case algebra::IndexPredicate_Triplet::ValueCase::kParam: {
      RTAnyType type;
      std::tie(expr_str, expr_name, type) =
          param_pb_2_str(context_, predicate.param());
      break;
    }
    default:
      LOG(FATAL) << "Invalid value type";
    }
    return true;
  }

  bool parse_idx_predicate(const algebra::IndexPredicate& predicate,
                           std::vector<int64_t>& oids, bool& scan_oid) const {
    if (predicate.or_predicates_size() != 1) {
      return false;
    }

    if (predicate.or_predicates(0).predicates_size() != 1) {
      return false;
    }
    const auto& triplet = predicate.or_predicates(0).predicates(0);
    if (!triplet.has_key()) {
      return false;
    }
    auto key = triplet.key();
    if (key.has_key()) {
      scan_oid = true;
    } else if (key.has_id()) {
      scan_oid = false;
    } else {
      LOG(FATAL) << "unexpected key case";
    }

    if (triplet.cmp() != common::Logical::EQ &&
        triplet.cmp() != common::Logical::WITHIN) {
      return false;
    }

    if (triplet.value_case() ==
        algebra::IndexPredicate_Triplet::ValueCase::kConst) {
      if (triplet.const_().item_case() == common::Value::kI32) {
        oids.push_back(triplet.const_().i32());
      } else if (triplet.const_().item_case() == common::Value::kI64) {
        oids.push_back(triplet.const_().i64());
      } else if (triplet.const_().item_case() == common::Value::kI64Array) {
        const auto& array = triplet.const_().i64_array();
        for (int i = 0; i < array.item_size(); ++i) {
          oids.push_back(array.item(i));
        }

      } else {
        LOG(FATAL) << "unexpected value case" << triplet.const_().item_case();
      }
    }
    return true;
  }

  std::string Build(const physical::Scan& scan_opr) {
    {
      label_t label;
      int64_t vertex_id;
      int alias;
      bool scan_oid;
      std::string expr_name, expr_str;
      std::string ss;
      if (is_find_vertex(scan_opr, label, vertex_id, alias, scan_oid, expr_name,
                         expr_str)) {
        auto ctx = context_.GetCurCtxName();
        context_.set_alias(alias, ContextColumnType::kVertex,
                           RTAnyType::kVertex);
        ss += expr_str + "\n auto ";
        ss += ctx + " = find_vertex(txn, " +
              std::to_string(static_cast<int>(label)) + ", " + expr_name +
              ", " + std::to_string(alias) + ", " +
              (scan_oid ? "true" : "false") + ");\n";
        context_.set_alias(alias, ContextColumnType::kVertex,
                           RTAnyType::kVertex);
        return ss;
      }
    }
    const auto& opt = scan_opr.scan_opt();
    CHECK(opt == physical::Scan::VERTEX) << "Unsupported scan option";
    ScanParams scan_params;
    if (scan_opr.has_alias()) {
      scan_params.alias = scan_opr.alias().value();
    } else {
      scan_params.alias = -1;
    }
    context_.set_alias(scan_params.alias, ContextColumnType::kVertex,
                       RTAnyType::kVertex);
    CHECK(scan_opr.has_params()) << "Scan params is not set";
    const auto& scan_opr_params = scan_opr.params();
    for (const auto& table : scan_opr_params.tables()) {
      scan_params.tables.push_back(table.id());
    }
    auto ctx_name = context_.GetCurCtxName();

    if (scan_opr_params.has_predicate()) {
      auto [name, str] = build_expr(context_, scan_opr_params.predicate(),
                                    VarType::kVertexVar);
      if (scan_opr.has_idx_predicate()) {
        const auto& idx_predicate = scan_opr.idx_predicate();
        std::vector<int64_t> oids;
        bool scan_oid;
        CHECK(parse_idx_predicate(idx_predicate, oids, scan_oid))
            << "Invalid idx predicate";
        std::string ss;
        ss += str + "\n auto ";
        if (scan_oid) {
          ss += ctx_name + " = Scan::filter_oids(txn, " +
                scan_params.to_string() + ", [" + name +
                "](label_t label, vid_t vid){\n return " + name +
                ".typed_eval_vertex(label, vid, 0);\n" + "}, ";
        } else {
          ss += ctx_name + " = Scan::filter_gids(txn, " +
                scan_params.to_string() + ", [" + name +
                "](label_t label, vid_t vid){\n return " + name +
                ".typed_eval_vertex(label, vid, 0);\n" + "}, ";
        }
        ss += vec_2_str(oids) + ");\n";
        return ss;
      } else {
        std::string ss;
        ss += str + "\n auto ";
        ss += ctx_name + " = Scan::scan_vertex(txn, " +
              scan_params.to_string() + ", [" + name +
              "](label_t label, vid_t vid){\n return " + name +
              ".typed_eval_vertex(label, vid, 0);\n" + "});\n";
        return ss;
      }
    }
    if (scan_opr.has_idx_predicate()) {
      const auto& idx_predicate = scan_opr.idx_predicate();
      std::vector<int64_t> oids;
      bool scan_oid;
      CHECK(parse_idx_predicate(idx_predicate, oids, scan_oid))
          << "Invalid idx predicate";
      if (scan_oid) {
        std::string ss{"auto "};

        ss += ctx_name + " = Scan::filter_oids(txn, " +
              scan_params.to_string() + "[](label_t label, vid_t vid){\n" +
              "return true;\n" + "}, " + vec_2_str(oids) + ");\n";
        return ss;
      } else {
        std::string ss{"auto "};
        ss += ctx_name + " = Scan::filter_gids(txn, " +
              scan_params.to_string() +
              "[](label_t label, vid_t vid){\n    return true;\n" + "}, " +
              vec_2_str(oids) + ");\n";
        return ss;
      }

    } else {
      std::string ss{"auto "};
      ss += ctx_name + " = Scan::scan_vertex(txn, " + scan_params.to_string() +
            "[](label_t label, vid_t vid){\n    return true;\n});\n";
      return ss;
    }
    LOG(FATAL) << "not support to reach here";
    return "";
  }

  BuildingContext& context_;
};

std::string build_scan(BuildingContext& context, const physical::Scan& opr) {
  return ScanBuilder(context).Build(opr);
}
}  // namespace runtime
}  // namespace gs