
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

#include "flex/engines/graph_db/app/app_base.h"
#include "flex/storages/rt_mutable_graph/types.h"
#include "flex/utils/property/types.h"

#include "flex/engines/hqps_db/database/mutable_csr_interface_v2.h"
#include "flex/proto_generated_gie/results.pb.h"

// A unit test case which shows how to customize a graph interface, and write a
// stored procedure and run.

struct MyColumnBase {
  virtual gs::Any get(size_t index) = 0;
};

template <typename T>
struct MyColumn : public MyColumnBase {
  T get_view(size_t index) const { return T(); }
  gs::Any get(size_t index) const { return gs::Any(T()); }
};

template <typename T>
struct PropertyGetter {
  using value_type = T;
  std::shared_ptr<MyColumn<T>> column;
  bool is_valid;
  PropertyGetter(std::shared_ptr<MyColumn<T>> column)
      : column(column), is_valid((column != nullptr)) {}

  bool IsValid() const { return is_valid; }

  inline T Get(size_t index) const { return column->get_view(index); }

  inline T get_view(size_t index) const { return column->get_view(index); }
};

class NbrList {
 public:
  class Iterator {};
  Iterator begin() const;
  Iterator end() const;
  inline size_t size() const;
};

class NbrListArray {
 public:
  NbrListArray() {}
  ~NbrListArray() = default;

  NbrList get(size_t index) const;

  size_t size() const;

  void resize(size_t size);
};

template <typename T>
class AdjList {
  class Iterator {};

 public:
  Iterator begin() const;
  Iterator end() const;
  size_t size() const;
};

//提供自己的实现
class SubGraph {
 public:
  using vid_t = uint64_t;
  using label_id_t = uint8_t;
  class Iterator {
    inline void Next() const;

    inline vid_t GetDstId() const;

    inline vid_t GetSrcId() const;

    inline vid_t GetOtherId() const;

    inline label_id_t GetDstLabel() const;

    inline label_id_t GetSrcLabel() const;

    inline label_id_t GetOtherLabel() const;

    inline gs::Direction GetDirection();

    inline gs::Any GetData() const;
    inline bool IsValid() const;
  };
  inline Iterator get_edges(vid_t vid) const;

  // here the src, dst, refer the sub graph, not the csr.
  label_id_t GetSrcLabel() const;
  label_id_t GetDstLabel() const;
  label_id_t GetEdgeLabel() const;
  gs::Direction GetDirection() const;
};

/**
 * @brief Stores a list of AdjLists, each of which represents the edges of a
 * vertex.
 * @tparam T The type of the property.
 */
template <typename T>
class AdjListArray {
 public:
  size_t size() const;

  AdjList<T> get(size_t i) const;
};

// Real implementation of the storage
class ActualStorage {};

//基于实际的存储实现，封装出访问接口
class TestGraph {
 public:
  using vertex_id_t = uint64_t;
  using label_id_t = uint8_t;

  TestGraph(const ActualStorage& storage) : storage_(storage) {}

  template <typename T>
  using adj_list_array_t = AdjListArray<T>;

  using nbr_list_array_t = NbrListArray;

  using sub_graph_t = SubGraph;

  template <typename T>
  using prop_getter_t = gs::mutable_csr_graph_impl::PropertyGetter<T>;

  using untyped_prop_getter_t =
      gs::mutable_csr_graph_impl::UntypedPropertyGetter;

  //////////////////////////////Graph Metadata Related////////////
  inline size_t VertexLabelNum() const {
    throw std::runtime_error("Not implemented");
  }

  inline size_t EdgeLabelNum() const {
    throw std::runtime_error("Not implemented");
  }

  inline size_t VertexNum() const {
    throw std::runtime_error("Not implemented");
  }

  inline size_t VertexNum(const label_id_t& label) const {
    throw std::runtime_error("Not implemented");
  }

  inline size_t EdgeNum() const { throw std::runtime_error("Not implemented"); }

  inline size_t EdgeNum(label_id_t src_label, label_id_t dst_label,
                        label_id_t edge_label) const {
    throw std::runtime_error("Not implemented");
  }

  label_id_t GetVertexLabelId(const std::string& label) const {
    throw std::runtime_error("Not implemented");
  }

  label_id_t GetEdgeLabelId(const std::string& label) const {
    throw std::runtime_error("Not implemented");
  }

  std::string GetVertexLabelName(label_id_t index) const {
    throw std::runtime_error("Not implemented");
  }

  std::string GetEdgeLabelName(label_id_t index) const {
    throw std::runtime_error("Not implemented");
  }

  bool ExitVertexLabel(const std::string& label) const {
    throw std::runtime_error("Not implemented");
  }

  bool ExitEdgeLabel(const std::string& edge_label) const {
    throw std::runtime_error("Not implemented");
  }

  bool ExitEdgeTriplet(const label_id_t& src_label, const label_id_t& dst_label,
                       const label_id_t& edge_label) const {
    throw std::runtime_error("Not implemented");
  }

  std::vector<std::pair<std::string, gs::PropertyType>>
  GetEdgeTripletPropertyMeta(const label_id_t& src_label,
                             const label_id_t& dst_label,
                             const label_id_t& label) const {
    throw std::runtime_error("Not implemented");
  }

  std::vector<std::pair<std::string, gs::PropertyType>> GetVertexPropertyMeta(
      label_id_t label) const {
    throw std::runtime_error("Not implemented");
  }

  //////////////////////////////Vertex-related Interface////////////

  /**
   * @brief
    Scan all points with label label_id, for each point, get the properties
   specified by selectors, and input them into func. The function signature of
   func_t should be: void func(vertex_id_t v, const std::tuple<xxx>& props)
    Users implement their own logic in the function. This function has no return
   value. In the example below, we scan all person points, find all points with
   age , and save them to a vector. std::vector<vertex_id_t> vids;
       graph.ScanVertices(person_label_id,
   gs::PropertySelector<int32_t>("age"),
       [&vids](vertex_id_t vid, const std::tuple<int32_t>& props){
          if (std::get<0>(props) == 18){
              vids.emplace_back(vid);
          }
       });
    It is important to note that the properties specified by selectors will be
   input into the lambda function in a tuple manner.
   * @tparam FUNC_T
   * @tparam SELECTOR
   * @param label_id
   * @param selectors The Property selectors. The selected properties will be
   * fed to the function
   * @param func The lambda function for filtering.
   */
  // TODO(zhanglei): fix filter_null in scan.h
  template <typename FUNC_T, typename... T>
  void ScanVertices(const label_id_t& label_id,
                    const std::tuple<gs::PropertySelector<T>...>& selectors,
                    const FUNC_T& func) const {
    throw std::runtime_error("Not implemented");
  }

  /**
   * @brief ScanVertices scans all vertices with the given label with give
   * original id.
   * @param label_id The label id.
   * @param oid The original id.
   * @param vid The result internal id.
   */
  bool ScanVerticesWithOid(const label_id_t& label_id, gs::Any oid,
                           vertex_id_t& vid) const {
    throw std::runtime_error("Not implemented");
  }

  /**
   * @brief GetVertexPropertyGetter gets the property getter for the given
   * vertex label and property name.
   * @tparam T The property type.
   * @param label_id The vertex label id.
   * @param prop_name The property name.
   * @return The property getter.
   */
  template <typename T>
  gs::mutable_csr_graph_impl::PropertyGetter<T> GetVertexPropertyGetter(
      const label_id_t& label_id, const std::string& prop_name) const {
    throw std::runtime_error("Not implemented");
  }

  gs::mutable_csr_graph_impl::UntypedPropertyGetter
  GetUntypedVertexPropertyGetter(const label_id_t& label_id,
                                 const std::string& prop_name) const {
    throw std::runtime_error("Not implemented");
  }

  //////////////////////////////Edge-related Interface////////////

  /**
   * @brief GetEdges gets the edges with the given label and edge label, and
   * with the starting vertex internal ids.
   * When the direction is "out", the edges are from the source label to the
   * destination label, and vice versa when the direction is "in". When the
   * direction is "both", the src and dst labels SHOULD be the same.
   */
  template <typename T>
  AdjListArray<T> GetEdges(const label_id_t& src_label_id,
                           const label_id_t& dst_label_id,
                           const label_id_t& edge_label_id,
                           const std::vector<vertex_id_t>& vids,
                           const gs::Direction& direction,
                           size_t limit = INT_MAX) const {
    throw std::runtime_error("Not implemented");
  }

  /**
   * @brief Get vertices on the other side of edges, via the given edge label
   * and the starting vertex internal ids.
   * When the direction is "out", the vertices are on the destination label side
   * of the edges, and vice versa when the direction is "in". When the direction
   * is "both", the src and dst labels SHOULD be the same.
   */
  NbrListArray GetOtherVertices(const label_id_t& src_label_id,
                                const label_id_t& dst_label_id,
                                const label_id_t& edge_label_id,
                                const std::vector<vertex_id_t>& vids,
                                const gs::Direction& direction,
                                size_t limit = INT_MAX) const {
    throw std::runtime_error("Not implemented");
  }

  //////////////////////////////Subgraph-related Interface////////////
  gs::mutable_csr_graph_impl::SubGraph GetSubGraph(
      const label_id_t src_label_id, const label_id_t dst_label_id,
      const label_id_t edge_label_id, const gs::Direction& direction) const {
    throw std::runtime_error("Not implemented");
  }

 private:
  const ActualStorage& storage_;
};

//查询通过procedure的方式去实现。这块虚基类的接口还未确定好，但是确定的是就是一个query函数。
class ReadExample {
 public:
  using vertex_id_t = TestGraph::vertex_id_t;
  using label_id_t = TestGraph::label_id_t;

  ReadExample() {}
  // Query function for query class
  results::CollectiveResults Query(TestGraph& graph) const {
    // Query the graph
    // Get the vertex label id
    label_id_t person_label_id = graph.GetVertexLabelId("person");
    // Get the property getter for the vertex label
    auto prop_getter =
        graph.GetVertexPropertyGetter<int32_t>(person_label_id, "age");
    // Get the property getter for the vertex label
    auto prop_getter2 =
        graph.GetVertexPropertyGetter<std::string>(person_label_id, "name");

    results::CollectiveResults results;
    // find the person with id 1
    vertex_id_t vid;
    if (graph.ScanVerticesWithOid(person_label_id, 1, vid)) {
      // Get the age of the person
      int32_t age = prop_getter.Get(vid);
      // Get the name of the person
      std::string name = prop_getter2.Get(vid);
      // Print the age and name
      std::cout << "Person with id 1 has age: " << age << " and name: " << name
                << std::endl;
      auto record = results.add_results()->mutable_record();
      {
        auto col = record->add_columns();
        col->mutable_name_or_id()->set_name("age");
        col->mutable_entry()->mutable_element()->mutable_object()->set_i32(age);
      }
      {
        auto col = record->add_columns();
        col->mutable_name_or_id()->set_name("name");
        col->mutable_entry()->mutable_element()->mutable_object()->set_str(
            name);
      }

    } else {
      std::cout << "Person with id 1 not found" << std::endl;
    }
    return results;
  }
};

int main(int argc, char** argv) {
  //

  ActualStorage storage;
  TestGraph graph(storage);
  ReadExample app;
  auto results = app.Query(graph);
  return 0;
}