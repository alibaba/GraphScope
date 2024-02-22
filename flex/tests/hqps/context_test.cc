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
#include <climits>
#include <cstdlib>
#include <vector>

#include "flex/engines/hqps_db/core/context.h"
#include "flex/engines/hqps_db/core/params.h"
#include "flex/engines/hqps_db/structures/multi_vertex_set/row_vertex_set.h"
#include "flex/engines/hqps_db/structures/multi_vertex_set/two_label_vertex_set.h"
#include "flex/storages/rt_mutable_graph/types.h"

#include "flex/engines/hqps_db/core/base_engine.h"

#include "flex/engines/hqps_db/core/operator/project.h"
#include "flex/engines/hqps_db/database/mutable_csr_interface.h"
#include "flex/storages/rt_mutable_graph/types.h"
#include "flex/utils/property/column.h"
#include "glog/logging.h"
#include "grape/types.h"

struct VertexSetTest {};
struct EdgeSetTest {};

struct TmpComparator {
  inline bool operator()(
      const std::tuple<int32_t, int64_t, double>& left,
      const std::tuple<int32_t, int64_t, double>& right) const {
    if (std::get<0>(left) < std::get<0>(right)) {
      return true;
    }
    if (std::get<0>(left) > std::get<0>(right)) {
      return false;
    }
    if (std::get<1>(left) > std::get<1>(right)) {
      return true;
    }
    if (std::get<1>(left) < std::get<1>(right)) {
      return false;
    }
    return true;
  }
};

using offset_t = gs::offset_t;

auto make_vertex_set_a() {
  std::vector<gs::vid_t> vids{0, 1};
  std::vector<std::tuple<int32_t, double>> datas{std::make_tuple(1, 1.0),
                                                 std::make_tuple(2, 2.0)};
  auto res = gs::make_row_vertex_set(std::move(vids), "0", std::move(datas),
                                     {"a", "b"});
  return res;
}

auto make_vertex_set_b() {
  std::vector<gs::vid_t> vids{2, 3, 4, 5};
  std::vector<std::tuple<int32_t, double>> datas{
      std::make_tuple(2, 2.0), std::make_tuple(3, 3.0), std::make_tuple(4, 4.0),
      std::make_tuple(5, 5.0)};
  auto res = gs::make_row_vertex_set(std::move(vids), "0", std::move(datas),
                                     {"a", "b"});
  return res;
}

auto make_vertex_set_c() {
  std::vector<gs::vid_t> vids{0, 1, 4};
  auto res = gs::make_default_row_vertex_set(std::move(vids), "0");
  return res;
}

auto make_vertex_set_d() {
  std::vector<gs::vid_t> vids{2, 3, 1, 0};
  auto res = gs::make_default_row_vertex_set(std::move(vids), "0");
  return res;
}

// 0,2,6;
// 0,3,7
// 1,4,8
// 1,5,9
// 1,5,10
auto make_sample_context() {}

int main() {
  // test join

  LOG(INFO) << "start join test";
  auto vertex_set_a = make_vertex_set_c();
  auto ctx_a = gs::DefaultContext<decltype(vertex_set_a), -1, grape::EmptyType>(
      std::move(vertex_set_a));

  auto vertex_set_b = make_vertex_set_d();
  auto ctx_b = gs::DefaultContext<decltype(vertex_set_b), -1, grape::EmptyType>(
      std::move(vertex_set_b));

  auto ctx_c = gs::BaseEngine::template Join<-1, -1, gs::JoinKind::InnerJoin>(
      std::move(ctx_a), std::move(ctx_b));
  for (auto iter : ctx_c) {
    VLOG(10) << gs::to_string(iter.GetAllElement());
  }
  LOG(INFO) << "Finish join test";

  {
    // test two label set
    std::vector<int64_t> vec{1, 2, 3, 4, 5, 6};
    std::array<std::string, 2> labels{"a", "b"};
    grape::Bitset set;
    set.init(6);
    {
      set.set_bit(0);
      set.set_bit(2);
      set.set_bit(3);
      set.set_bit(5);
    }
    auto two_label_set = gs::make_two_label_set(
        std::move(vec), std::move(labels), std::move(set));
    {
      int32_t cnt = 1;
      for (auto iter : two_label_set) {
        CHECK(iter.GetElement().second == cnt);
        cnt += 1;
      }
    }
    LOG(INFO) << "Finish two label test";
  }

  {
    std::srand((unsigned) time(NULL));
    size_t limit = 300000;
    std::vector<int32_t> indices;
    indices.reserve(limit);
    unsigned int tmp;
    for (size_t i = 0; i < limit; ++i) {
      rand_r(&tmp);
      indices.emplace_back((int32_t) tmp % limit);
    }
    auto col1 =
        std::make_shared<gs::TypedColumn<int32_t>>(gs::StorageStrategy::kMem);

    col1->resize(limit);
    for (size_t i = 0; i < limit; ++i) {
      col1->set_value(i, (int32_t)(i));
    }
    {
      int32_t res = 0;
      double t0 = -grape::GetCurrentTime();
      for (size_t j = 0; j < 10; ++j) {
        for (auto ind : indices) {
          res += col1->get_view(ind);
        }
      }
      t0 += grape::GetCurrentTime();

      double t1 = -grape::GetCurrentTime();
      res = 0;
      for (auto ind : indices) {
        res += col1->get_view(ind);
      }
      t1 += grape::GetCurrentTime();
      LOG(INFO) << "ptr visit cost: " << t1 << " ,warm up took: " << t0
                << ", res: " << res;
    }

    {
      auto ref_col = std::make_shared<gs::TypedRefColumn<int32_t>>(*col1);
      gs::mutable_csr_graph_impl::SinglePropGetter<int32_t> getter(ref_col);
      double t1 = -grape::GetCurrentTime();
      int32_t res = 0;
      for (auto ind : indices) {
        res += getter.get_view(ind);
      }
      t1 += grape::GetCurrentTime();
      LOG(INFO) << "no get tuple visit cost: " << t1 << ", res: " << res;
    }
  }

  {
    std::srand((unsigned) time(NULL));
    size_t limit = 300000;
    std::vector<int32_t> indices;
    indices.reserve(limit);
    unsigned int tmp;
    for (size_t i = 0; i < limit; ++i) {
      rand_r(&tmp);
      indices.emplace_back(tmp % limit);
    }
    auto col1 =
        std::make_shared<gs::TypedColumn<int32_t>>(gs::StorageStrategy::kMem);
    auto col2 =
        std::make_shared<gs::TypedColumn<int64_t>>(gs::StorageStrategy::kMem);

    col1->resize(limit);
    col2->resize(limit);
    for (size_t i = 0; i < limit; ++i) {
      col1->set_value(i, (int32_t)(i));
      col2->set_value(i, (int64_t) i);
    }
    {
      int32_t res = 0;
      double t0 = -grape::GetCurrentTime();
      for (size_t j = 0; j < 10; ++j) {
        for (auto ind : indices) {
          res += col1->get_view(ind);
        }
      }
      t0 += grape::GetCurrentTime();

      double t1 = -grape::GetCurrentTime();
      res = 0;
      for (auto ind : indices) {
        res += col1->get_view(ind);
      }
      t1 += grape::GetCurrentTime();
      LOG(INFO) << "ptr visit cost: " << t1 << " ,warm up took: " << t0
                << ", res: " << res;
    }

    {
      auto ref_col1 = std::make_shared<gs::TypedRefColumn<int32_t>>(*col1);
      auto ref_col2 = std::make_shared<gs::TypedRefColumn<int64_t>>(*col2);
      gs::mutable_csr_graph_impl::SinglePropGetter<int32_t> getter1(ref_col1);
      gs::mutable_csr_graph_impl::SinglePropGetter<int64_t> getter2(ref_col2);

      double t1 = -grape::GetCurrentTime();
      int32_t res = 0;
      for (auto ind : indices) {
        res += getter1.get_view(ind);
      }
      t1 += grape::GetCurrentTime();
      LOG(INFO) << "bench1 cost: " << t1 << ", res: " << res;
    }

    {
      std::vector<std::vector<int64_t>> vids;
      std::vector<std::vector<gs::mutable_csr_graph_impl::Nbr>> nbrs;
      vids.resize(300);
      nbrs.resize(300);
      {
        double t0 = -grape::GetCurrentTime();
        for (size_t i = 0; i < 300; ++i) {
          for (size_t j = 0; j < 1000; ++j) {
            vids[i].emplace_back(j);
          }
        }
        t0 += grape::GetCurrentTime();
        LOG(INFO) << "emplacing vec: " << t0;
        double t1 = -grape::GetCurrentTime();
        int64_t tmp = 0;
        for (auto vid_vec : vids) {
          for (auto vid : vid_vec) {
            tmp += vid;
          }
        }
        t1 += grape::GetCurrentTime();
        LOG(INFO) << "visiting 2d vec cost: " << t1 << ", res" << tmp;
      }
      {
        double t0 = -grape::GetCurrentTime();
        for (size_t i = 0; i < 300; ++i) {
          for (size_t j = 0; j < 1000; ++j) {
            nbrs[i].emplace_back(gs::mutable_csr_graph_impl::Nbr(j));
          }
        }
        t0 += grape::GetCurrentTime();
        LOG(INFO) << "emplacing nbr: " << t0;
        double t1 = -grape::GetCurrentTime();
        int64_t tmp = 0;
        for (size_t i = 0; i < nbrs.size(); ++i) {
          for (auto nbr : nbrs[i]) {
            tmp += nbr.neighbor();
          }
        }
        t1 += grape::GetCurrentTime();
        LOG(INFO) << "visiting nbrs cost: " << t1 << ", res" << tmp;
      }
    }

    {
      size_t limit = 300000;
      auto col1 =
          std::make_shared<gs::TypedColumn<int32_t>>(gs::StorageStrategy::kMem);
      auto col2 =
          std::make_shared<gs::TypedColumn<int32_t>>(gs::StorageStrategy::kMem);

      col1->resize(limit);
      col2->resize(limit);
      for (size_t i = 0; i < limit; i += 2) {
        col1->set_value(i, (int32_t)(i));
      }
      for (size_t i = 1; i < limit; i += 2) {
        col2->set_value(i, (int32_t)(i));
      }
      // test two label vertex set prop getter.
      auto ref_col1 = std::make_shared<gs::TypedRefColumn<int32_t>>(*col1);
      auto ref_col2 = std::make_shared<gs::TypedRefColumn<int32_t>>(*col2);
      gs::mutable_csr_graph_impl::SinglePropGetter<int32_t> getter1(ref_col1);
      gs::mutable_csr_graph_impl::SinglePropGetter<int32_t> getter2(ref_col2);
      std::array<gs::mutable_csr_graph_impl::SinglePropGetter<int32_t>, 2>
          array{getter1, getter2};

      // generate index ele
      std::vector<std::tuple<size_t, size_t>> index_eles;
      index_eles.reserve(limit);
      for (size_t i = 0; i < limit; ++i) {
        if (i % 2 == 0) {
          index_eles.emplace_back(std::make_tuple(0, i));
        } else {
          index_eles.emplace_back(std::make_tuple(1, i));
        }
      }

      {
        double t1 = -grape::GetCurrentTime();
        int32_t res = 0;
        for (auto ind : index_eles) {
          res += array[std::get<0>(ind)].get_view(std::get<1>(ind));
        }
        t1 += grape::GetCurrentTime();
        LOG(INFO) << "get from two label set cost: " << t1 << ", res: " << res;
      }

      {
        // more locality
        grape::Bitset bitset;
        bitset.init(limit);
        for (size_t i = 0; i < limit; ++i) {
          if (i % 2 == 0) {
            bitset.set_bit(i);
          }
        }
        double t1 = -grape::GetCurrentTime();
        int32_t res = 0;

        auto& first = array[0];
        for (size_t i = 0; i < limit; ++i) {
          if (bitset.get_bit(i)) {
            res += first.get_view(std::get<1>(index_eles[i]));
          }
        }
        auto& second = array[1];
        for (size_t i = 0; i < limit; ++i) {
          if (!bitset.get_bit(i)) {
            res += second.get_view(std::get<1>(index_eles[i]));
          }
        }
        t1 += grape::GetCurrentTime();
        LOG(INFO) << "get with locality cost: " << t1 << ", res: " << res;
      }
    }

    {
      using sort_tuple_t = std::tuple<int32_t, int64_t, double>;
      TmpComparator sorter;

      size_t limit = 100000;
      std::vector<int32_t> vec0;
      std::vector<int64_t> vec1;
      std::vector<double> vec2;
      {
        vec0.reserve(limit);
        vec1.reserve(limit);
        vec2.reserve(limit);
        {
          unsigned int tmp;
          for (size_t i = 0; i < limit; ++i) {
            rand_r(&tmp);
            vec0.emplace_back(tmp % limit);
          }
        }
        {
          unsigned int tmp;
          for (size_t i = 0; i < limit; ++i) {
            rand_r(&tmp);
            vec1.emplace_back((int64_t) tmp % limit);
          }
        }
        {
          unsigned int tmp;
          for (size_t i = 0; i < limit; ++i) {
            rand_r(&tmp);
            vec2.emplace_back((double) (tmp % limit));
          }
        }
      }

      {
        std::priority_queue<sort_tuple_t, std::vector<sort_tuple_t>,
                            TmpComparator>
            pq(sorter);
        double t1 = -grape::GetCurrentTime();
        for (size_t i = 0; i < limit; ++i) {
          if (pq.size() < 20) {
            pq.emplace(vec0[i], vec1[i], vec2[i]);
          } else {
            auto tuple = std::make_tuple(vec0[i], vec1[i], vec2[i]);
            if (sorter(tuple, pq.top())) {
              pq.pop();
              pq.emplace(tuple);
            }
          }
        }
        t1 += grape::GetCurrentTime();
        LOG(INFO) << " emplace tuple: " << t1;
      }

      {
        std::priority_queue<sort_tuple_t, std::vector<sort_tuple_t>,
                            TmpComparator>
            pq(sorter);
        double t1 = -grape::GetCurrentTime();
        // int32_t first;
        // int64_t second;
        // double third;
        sort_tuple_t empty_tuple;
        sort_tuple_t& top_tuple = empty_tuple;
        for (size_t i = 0; i < limit; ++i) {
          if (pq.size() < 20) {
            auto tuple = std::make_tuple(vec0[i], vec1[i], vec2[i]);
            pq.emplace(std::move(tuple));
            top_tuple = pq.top();
          } else {
            if (vec0[0] < std::get<0>(top_tuple) &&
                vec1[1] > std::get<1>(top_tuple)) {
              pq.pop();
              pq.emplace(std::make_tuple(vec0[i], vec1[i], vec2[i]));
              top_tuple = pq.top();
            }
          }
        }
        t1 += grape::GetCurrentTime();
        LOG(INFO) << " emplace tuple: " << t1;
      }
    }
  }

  LOG(INFO) << "Finish context test.";
}
