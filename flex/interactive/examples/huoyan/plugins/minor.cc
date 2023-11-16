#include "flex/engines/graph_db/app/app_base.h"
#include "flex/engines/graph_db/database/graph_db_session.h"
#include "grape/util.h"

namespace gs {
class Query0 : public AppBase {
 public:
  static constexpr double timeout_sec = 15;
  static constexpr int32_t REL_TYPE_MAX = 19;  // 0 ~ 18
  Query0(GraphDBSession& graph)
      : comp_label_id_(graph.schema().get_vertex_label_id("company")),
        graph_(graph) {
    size_t num = graph_.graph().vertex_num(comp_label_id_);

    LOG(INFO) << "company num:" << num;

    auto comp_name_col =
        graph_.get_vertex_property_column(comp_label_id_, "vertex_name");
    if (!comp_name_col) {
      LOG(ERROR) << "column vertex_name not found for company";
    }
    typed_comp_named_col_ =
        std::dynamic_pointer_cast<TypedColumn<std::string_view>>(comp_name_col);
    if (!typed_comp_named_col_) {
      LOG(ERROR) << "column vertex_name is not string type for company";
    }
    for (size_t i = 0; i < 5; ++i){
        LOG(INFO) << "i: " << i << typed_comp_named_col_->get_view(i);
    }
    LOG(INFO) << "last one" << typed_comp_named_col_->get_view(num - 1);
  }
  ~Query0() {}


  inline vid_t encode_vid(label_t v_label, vid_t vid) {
    // vid_t is uint32_t, use the first 4 bits to store label id
    return ((vid_t) v_label << 28) | vid;
  }

  inline label_t decode_label(vid_t encoded_vid) { return encoded_vid >> 28; }

  inline vid_t decode_vid(vid_t encoded_vid) {
    return encoded_vid & 0x0FFFFFFF;
  }

  inline int64_t get_oid_from_encoded_vid(ReadTransaction& txn,
                                          vid_t encoded_vid) {
    auto label = decode_label(encoded_vid);
    auto vid = decode_vid(encoded_vid);
    return txn.GetVertexId(label, vid).AsInt64();
  }


#define DEBUG
  bool Query(Decoder& input, Encoder& output) {
    return true;
  }

 private:
  GraphDBSession& graph_;
  label_t comp_label_id_;

  std::shared_ptr<TypedColumn<std::string_view>> typed_comp_named_col_;
};

#undef DEBUG

}  // namespace gs

extern "C" {
void* CreateApp(gs::GraphDBSession& db) {
  gs::Query0* app = new gs::Query0(db);
  return static_cast<void*>(app);
}

void DeleteApp(void* app) {
  gs::Query0* casted = static_cast<gs::Query0*>(app);
  delete casted;
}
}
