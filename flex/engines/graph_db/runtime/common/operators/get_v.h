#ifndef RUNTIME_COMMON_OPERATORS_GET_V_H_
#define RUNTIME_COMMON_OPERATORS_GET_V_H_

#include "flex/engines/graph_db/runtime/common/columns/edge_columns.h"
#include "flex/engines/graph_db/runtime/common/columns/path_columns.h"
#include "flex/engines/graph_db/runtime/common/columns/vertex_columns.h"
#include "flex/engines/graph_db/runtime/common/context.h"

namespace gs {
namespace runtime {

struct GetVParams {
  VOpt opt;
  int tag;
  std::vector<label_t> tables;
  int alias;
};

std::vector<label_t> extract_labels(const std::vector<LabelTriplet>& labels,
                                    const std::vector<label_t>& tables,
                                    VOpt opt) {
  std::vector<label_t> output_labels;
  for (const auto& label : labels) {
    if (opt == VOpt::kStart) {
      if (std::find(tables.begin(), tables.end(), label.src_label) !=
          tables.end()) {
        output_labels.push_back(label.src_label);
      }
    } else if (opt == VOpt::kEnd) {
      if (std::find(tables.begin(), tables.end(), label.dst_label) !=
          tables.end()) {
        output_labels.push_back(label.dst_label);
      }
    } else {
      LOG(FATAL) << "not support";
    }
  }
  return output_labels;
}
class GetV {
 public:
  template <typename PRED_T>
  static Context get_vertex_from_edges(const ReadTransaction& txn,
                                       Context&& ctx, const GetVParams& params,
                                       const PRED_T& pred) {
    std::vector<size_t> shuffle_offset;
    LOG(INFO) << params.tables.size() << " size\n";
    // if (params.tables.size() == 1)
    auto col = ctx.get(params.tag);
    if (col->column_type() == ContextColumnType::kPath) {
      auto& input_path_list =
          *std::dynamic_pointer_cast<GeneralPathColumn>(col);

      MLVertexColumnBuilder builder;
      input_path_list.foreach_path([&](size_t index, const Path& path) {
        auto [label, vid] = path.get_end();
        builder.push_back_vertex(std::make_pair(label, vid));
        shuffle_offset.push_back(index);
      });
      LOG(INFO) << params.alias << "alias";
      ctx.set_with_reshuffle(params.alias, builder.finish(), shuffle_offset);
      LOG(INFO) << ctx.row_num() << " row num\n";
      return ctx;
    }

    auto column = std::dynamic_pointer_cast<IEdgeColumn>(ctx.get(params.tag));
    CHECK(column != nullptr);
    if (column->edge_column_type() == EdgeColumnType::kSDSL) {
      auto& input_edge_list =
          *std::dynamic_pointer_cast<SDSLEdgeColumn>(column);
      // label_t output_vertex_label = params.tables[0];
      label_t output_vertex_label{0};
      auto edge_label = input_edge_list.get_labels()[0];

      if (params.opt == VOpt::kStart) {
        output_vertex_label = edge_label.src_label;
      } else if (params.opt == VOpt::kEnd) {
        output_vertex_label = edge_label.dst_label;
      } else {
        LOG(FATAL) << "not support";
      }
      LOG(INFO) << "output_vertex_label: " << (int) output_vertex_label;
      // params tables size may be 0
      if (params.tables.size() == 1) {
        CHECK(output_vertex_label == params.tables[0]);
      }
      SLVertexColumnBuilder builder(output_vertex_label);
      if (params.opt == VOpt::kStart) {
        input_edge_list.foreach_edge(
            [&](size_t index, const LabelTriplet& label, vid_t src, vid_t dst,
                const Any& edata, Direction dir) {
              if (pred(label.src_label, src, index)) {
                builder.push_back_opt(src);
                shuffle_offset.push_back(index);
              }
            });
      } else if (params.opt == VOpt::kEnd) {
        input_edge_list.foreach_edge(
            [&](size_t index, const LabelTriplet& label, vid_t src, vid_t dst,
                const Any& edata, Direction dir) {
              if (pred(label.dst_label, dst, index)) {
                builder.push_back_opt(dst);
                shuffle_offset.push_back(index);
              }
            });
      } else {
        LOG(FATAL) << "not support";
      }
      ctx.set_with_reshuffle(params.alias, builder.finish(), shuffle_offset);
      LOG(INFO) << ctx.row_num() << " row num " << ctx.col_num();
      return ctx;
    } else if (column->edge_column_type() == EdgeColumnType::kSDML) {
      auto& input_edge_list =
          *std::dynamic_pointer_cast<SDMLEdgeColumn>(column);
      auto labels = extract_labels(input_edge_list.get_labels(), params.tables,
                                   params.opt);
      if (labels.size() == 0) {
        LOG(FATAL) << "labels.size() == 0";
        return ctx;
      }
      if (labels.size() > 1) {
        MLVertexColumnBuilder builder;
        if (params.opt == VOpt::kStart) {
          input_edge_list.foreach_edge([&](size_t index,
                                           const LabelTriplet& label, vid_t src,
                                           vid_t dst, const Any& edata,
                                           Direction dir) {
            if (std::find(labels.begin(), labels.end(), label.src_label) !=
                labels.end()) {
              builder.push_back_vertex(std::make_pair(label.src_label, src));
              shuffle_offset.push_back(index);
            }
          });
        } else if (params.opt == VOpt::kEnd) {
          input_edge_list.foreach_edge([&](size_t index,
                                           const LabelTriplet& label, vid_t src,
                                           vid_t dst, const Any& edata,
                                           Direction dir) {
            if (std::find(labels.begin(), labels.end(), label.dst_label) !=
                labels.end()) {
              builder.push_back_vertex(std::make_pair(label.dst_label, dst));
              shuffle_offset.push_back(index);
            }
          });
        } else {
          LOG(FATAL) << "not support";
        }
        ctx.set_with_reshuffle(params.alias, builder.finish(), shuffle_offset);
        LOG(INFO) << ctx.row_num() << " row num\n";
        return ctx;
      }
    } else if (column->edge_column_type() == EdgeColumnType::kBDSL) {
      auto& input_edge_list =
          *std::dynamic_pointer_cast<BDSLEdgeColumn>(column);
      if (params.tables.size() == 0) {
        auto type = input_edge_list.get_labels()[0];
        if (type.src_label != type.dst_label) {
          MLVertexColumnBuilder builder;
          CHECK(params.opt == VOpt::kOther);
          input_edge_list.foreach_edge([&](size_t index,
                                           const LabelTriplet& label, vid_t src,
                                           vid_t dst, const Any& edata,
                                           Direction dir) {
            if (dir == Direction::kOut) {
              builder.push_back_vertex(std::make_pair(label.dst_label, dst));
            } else {
              builder.push_back_vertex(std::make_pair(label.src_label, src));
            }
            shuffle_offset.push_back(index);
          });
          ctx.set_with_reshuffle(params.alias, builder.finish(),
                                 shuffle_offset);
          return ctx;
        } else {
          SLVertexColumnBuilder builder(type.src_label);
          input_edge_list.foreach_edge(
              [&](size_t index, const LabelTriplet& label, vid_t src, vid_t dst,
                  const Any& edata, Direction dir) {
                if (dir == Direction::kOut) {
                  builder.push_back_opt(dst);
                  shuffle_offset.push_back(index);
                } else {
                  builder.push_back_opt(src);
                  shuffle_offset.push_back(index);
                }
              });
          ctx.set_with_reshuffle(params.alias, builder.finish(),
                                 shuffle_offset);
          return ctx;
        }
      } else {
        std::vector<label_t> labels;
        auto type = input_edge_list.get_labels()[0];
        for (auto& label : params.tables) {
          if (label == type.src_label || label == type.dst_label) {
            labels.push_back(label);
          }
        }
        if (labels.size() == 1) {
          SLVertexColumnBuilder builder(labels[0]);
          input_edge_list.foreach_edge(
              [&](size_t index, const LabelTriplet& label, vid_t src, vid_t dst,
                  const Any& edata, Direction dir) {
                if (dir == Direction::kOut) {
                  if (label.dst_label == labels[0]) {
                    builder.push_back_opt(dst);
                    shuffle_offset.push_back(index);
                  }
                } else {
                  if (label.src_label == labels[0]) {
                    builder.push_back_opt(src);
                    shuffle_offset.push_back(index);
                  }
                }
              });
          ctx.set_with_reshuffle(params.alias, builder.finish(),
                                 shuffle_offset);
          return ctx;
        }
      }
    } else if (column->edge_column_type() == EdgeColumnType::kBDML) {
      auto& input_edge_list =
          *std::dynamic_pointer_cast<BDMLEdgeColumn>(column);
      if (params.tables.size() == 0) {
        MLVertexColumnBuilder builder;
        CHECK(params.opt == VOpt::kOther);
        input_edge_list.foreach_edge(
            [&](size_t index, const LabelTriplet& label, vid_t src, vid_t dst,
                const Any& edata, Direction dir) {
              if (dir == Direction::kOut) {
                builder.push_back_vertex(std::make_pair(label.dst_label, dst));
              } else {
                builder.push_back_vertex(std::make_pair(label.src_label, src));
              }
              shuffle_offset.push_back(index);
            });

        LOG(INFO) << "GetV" << ctx.row_num() << " row num\n";
        ctx.set_with_reshuffle(params.alias, builder.finish(), shuffle_offset);
        return ctx;
      }
    }

    LOG(FATAL) << "not support";
    return ctx;
  }

  template <typename PRED_T>
  static Context get_vertex_from_vertices(const ReadTransaction& txn,
                                          Context&& ctx,
                                          const GetVParams& params,
                                          const PRED_T& pred) {
    std::shared_ptr<IVertexColumn> input_vertex_list_ptr =
        std::dynamic_pointer_cast<IVertexColumn>(ctx.get(params.tag));
    const IVertexColumn& input_vertex_list = *input_vertex_list_ptr;

    size_t num = input_vertex_list.size();

    LOG(INFO) << params.alias << " " << params.tag << " " << num;
    std::vector<size_t> offset;
    if (params.tag == params.alias) {
      foreach_vertex(input_vertex_list,
                     [&](size_t idx, label_t label, vid_t v) {
                       if (pred(label, v, idx)) {
                         offset.push_back(idx);
                       }
                     });
      ctx.reshuffle(offset);
    } else {
      const std::set<label_t>& label_set = input_vertex_list.get_labels_set();
      LOG(INFO) << "label_set.size(): " << label_set.size();
      if (label_set.size() == 1) {
        SLVertexColumnBuilder builder(*label_set.begin());
        foreach_vertex(input_vertex_list,
                       [&](size_t idx, label_t label, vid_t v) {
                         if (pred(label, v, idx)) {
                           builder.push_back_opt(v);
                           offset.push_back(idx);
                         }
                       });
        ctx.set_with_reshuffle(params.alias, builder.finish(), offset);

      } else {
        MLVertexColumnBuilder builder;
        foreach_vertex(input_vertex_list,
                       [&](size_t idx, label_t label, vid_t v) {
                         LOG(INFO) << "label: " << (int) label;
                         if (pred(label, v, idx)) {
                           builder.push_back_vertex(std::make_pair(label, v));
                           offset.push_back(idx);
                         }
                       });
        LOG(INFO) << params.alias << "alias";
        ctx.set_with_reshuffle(params.alias, builder.finish(), offset);
      }
    }
    LOG(INFO) << ctx.row_num() << " row num\n";
    return ctx;
  }
};

}  // namespace runtime

}  // namespace gs

#endif  // RUNTIME_COMMON_OPERATORS_GET_V_H_