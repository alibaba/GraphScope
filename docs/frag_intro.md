# 一、ArrowFragment
# 0. 术语

标签(LABEL)：表示某一类顶点或者边
属性(PROPERTY)：表示顶点或边所关联的数据
原始ID(OID)：原始数据集中的id，不同标签的顶点允许有相同的OID
子图ID(FID)：子图的唯一编号
分区器(Partitioner)：能够确定顶点oid到分区id（FID）的映射
内部点(Inner vertex)：属于本Fragment的顶点
外部点(Outer vertex)：不属于本Fragment的顶点
本地ID(LID)，只在某个Fragment内部有意义
全局ID(GID)：能够表示任意一个顶点，被Fragment共识
VertexMap: 用于存储GID <--> OID的映射关系
子图(Fragment): 通过切边得到的子图，存储了顶点、边、顶点数据、边数据等信息

GID编码：FID   | VERTEX LABEL ID | OFFSET，其中VERTEX LABEL ID和OFFSET共同组成LID
OFFSET：某一类顶点在某个Fragment的顶点**序号**
LID编码：0填充 | VERTEX LABEL ID | OFFSET，**其中fid总是0**
## 1. 载图

按照执行流程编号，描述了载图的主要逻辑
### 1.1 初始化partitoner
   分为HashPartitioner和SegmentedPartitoned, 初始化他们需要给出fragment的数量，SegmentedPartitioner还需给出全部的oid。
### 1.2. 构建原始arrow table
    每个进程读取一部分v、e文件，得到多个vertex arrow table(vtable)和edge arrow table(etable)，其中vtable按照顶点标签(vertex label)分组，etable先按照边标签(edge label)分组，同一个边标签下面又按照src和dst的顶点标签分组
```cpp
// 原始的顶点表、边表，每个进程持有部分的数据
std::vector<std::shared_ptr<arrow::Table>> partial_v_tables;
std::vector<std::vector<std::shared_ptr<arrow::Table>>> partial_e_tables;

partial_v_tables.size() == vertex label num;
partial_e_tables.size() == edge label num;

partial_e_tables[vertex label id] <= vertex label num * vertex label num
```

   vertex arrow table包含oid、属性
   edge arrow table包括src和dst的oid、属性

### 1.3. Shuffle vtable
访问vtable的oid列，根据oid查询partition id,将表中的记录分发给顶点的拥有者
### 1.4. 构建Vertex map(BasicArrowVertexMapBuilder)
从shuffle后的vtable取出oid执行all gather操作，使得每个进程都能够获取的所有oid
根据frag数量、vertex label数量和全部oid，每个进程构建vertex map
```cpp
for (fid_t i = 0; i < fnum_; ++i) {
    for (label_id_t j = 0; j < label_num_; ++j) {
        vineyard::HashmapBuilder<oid_t, vid_t> builder(client);

        auto array = oid_arrays_[j][i];
        vid_t cur_gid = id_parser_.GenerateId(i, j, 0); // 起始gid
        int64_t vnum = array->length();
        // 遍历全部内部点，生成gid
        for (int64_t k = 0; k < vnum; ++k) {
            builder.emplace(array->GetView(k), cur_gid);
            ++cur_gid;
        }
    }
}
```
### 1.5. 构建Fragment准备
#### 1.5.1. 处理edge table(BasicEVFragmentLoader)

- 按照elabel遍历每一张edge table，获取到src和dst的label
- 访问每个src的oid，根据partitioner获取fid。根据fid，src label id和oid查询gid，生成gid数组；处理dst的逻辑同理
- 得到src和dst两个gid数组后，替换掉原etable的oid数组
- **对etable执行shuffle**，将相应的行传输给正确的进程
- 将shuffle过的vtable和etable送给BasicArrowFragmentBuilder构建CSR
#### 1.5.2 FragmentBuilder数据初始化 (BasicArrowFragmentBuilder)

- 处理顶点：

1) 初始化id parser，初始化ivnum、ovnum和tvnum数组，按照vertex label分组
2) 遍历每一种顶点，向vertex map查询本fragment的内部点数量，并设置对应的ivnum；将vertex table合并成一个chunk便于后续访问
```cpp
vid_parser_.Init(fnum_, vertex_label_num_);

vertex_tables_.resize(vertex_label_num_);
ivnums_.resize(vertex_label_num_);
ovnums_.resize(vertex_label_num_);
tvnums_.resize(vertex_label_num_);

for (size_t i = 0; i < vertex_tables.size(); ++i) {
  vertex_tables[i]->CombineChunks(arrow::default_memory_pool(), &vertex_tables_[i]);
  ivnums_[i] = vm_ptr_->GetInnerVertexSize(fid_, i);
}
```

- 处理边：

      1) 遍历每一种边，通过src gid列和dst gid列查询出本fragment所有的外部点，并按照vertex label分组
```cpp
for (size_t i = 0; i < edge_tables.size(); ++i) {
  // edge arrow table合并成一个chunk
  edge_tables[i]->CombineChunks(arrow::default_memory_pool(), &edge_tables[i]);
  // 找出本fragment全部外部点的gid
  collect_outer_vertices(
      std::dynamic_pointer_cast<
          typename vineyard::ConvertToArrowType<vid_t>::ArrayType>(
          edge_tables[i]->column(0)->chunk(0)));
  collect_outer_vertices(
      std::dynamic_pointer_cast<
          typename vineyard::ConvertToArrowType<vid_t>::ArrayType>(
          edge_tables[i]->column(1)->chunk(0)));
}
```
      2) 根据外部点的gid，为每一种顶点构建gid到lid的映射与去重的gid数组（外部点的lid由label id和顶点序号组成，顶点序号从ivnum开始计数）；为每一种顶点填充ovnum和tvnum
```cpp
ovg2l_maps_.resize(vertex_label_num_);
ovgid_lists_.resize(vertex_label_num_);

for (label_id_t i = 0; i < vertex_label_num_; ++i) {
    auto& cur_list = collected_ovgids_[i];
    // 排序gid
    std::sort(cur_list.begin(), cur_list.end());

    auto& cur_map = ovg2l_maps_[i];
    typename ConvertToArrowType<vid_t>::BuilderType vec_builder;

    // 生成外部点的lid，其中fid为0，offset从ivnum开始编号
    vid_t cur_id = vid_parser_.GenerateId(0, i, ivnums_[i]);
    // 处理第一个元素
    if (!cur_list.empty()) {
        cur_map.emplace(cur_list[0], cur_id);
        vec_builder.Append(cur_list[0]);
        ++cur_id;
    }

    size_t cur_list_length = cur_list.size();
    for (size_t k = 1; k < cur_list_length; ++k) {
        // 找到不重复的两个gid
        if (cur_list[k] != cur_list[k - 1]) {
            cur_map.emplace(cur_list[k], cur_id);
            vec_builder.Append(cur_list[k]);
            ++cur_id;
        }
    }

	vec_builder.Finish(&ovgid_lists_[i]);

    ovnums_[i] = ovgid_lists_[i]->length();
    tvnums_[i] = ivnums_[i] + ovnums_[i];
}
```
      3) 根据上一步构成的gid到lid的映射，将src和dst的gid都转换成lid。删除edge table的src列和dst列
```cpp
// gid到lid转换函数
std::shared_ptr<arrow::Array> generate_local_id_list(std::shared_ptr<arrow::Array> gid_list) {
    std::shared_ptr<arrow::Array> lid_list;
    for (int64_t i = 0; i < length; ++i) {
        vid_t gid = vec[i];
        if (vid_parser_.GetFid(gid) == fid_) {
            // 生成内部点lid
            builder.Append(vid_parser_.GenerateId(
                0, vid_parser_.GetLabelId(gid), vid_parser_.GetOffset(gid)));
        } else {
            // 查询g2l map，通过gid找到外部点lid
            builder.Append(ovg2l_maps_[vid_parser_.GetLabelId(gid)].at(gid));
        }
    }
    builder.Finish(&lid_list);
    return lid_list;
}

for (size_t i = 0; i < edge_tables.size(); ++i) {
    // 根据src和dst列的gid生成lid
    edge_src_[i] = generate_local_id_list(edge_tables[i]->column(0)->chunk(0));
    edge_dst_[i] = generate_local_id_list(edge_tables[i]->column(1)->chunk(0));
    // 删除edge table的src列和gid列，只保留属性
    std::shared_ptr<arrow::Table> tmp_table0;
    edge_tables[i]->RemoveColumn(0, &tmp_table0));
    tmp_table0->RemoveColumn(0, &edge_tables_[i]);
}
```
  4) 构建CSR
      对于有向图，构建出边和入边两个CSR。构建出边csr：
          统计每种源顶点的出度，构建row offset数组；访问每条边，使用row offset数组定位到目的顶点nbr的地址，填充dst lid和eid到nbr
          按照目的顶点的lid，将每一个源顶点的目的顶点排序
```cpp
generate_directed_csr(
      std::shared_ptr<vid_array_t> src_list,
      std::shared_ptr<vid_array_t> dst_list,
      std::vector<std::shared_ptr<arrow::FixedSizeBinaryArray>>& edges,
      std::vector<std::shared_ptr<arrow::Int64Array>>& edge_offsets) {
    
    std::vector<std::vector<int>> degree(vertex_label_num_);
    std::vector<int64_t> actual_edge_num(vertex_label_num_, 0);
    for (label_id_t v_label = 0; v_label != vertex_label_num_; ++v_label) {
      degree[v_label].resize(tvnums_[v_label], 0);
    }
    auto edge_num = src_list->length();
    auto* src_list_ptr = src_list->raw_values();
    auto* dst_list_ptr = dst_list->raw_values();
	// 计算源顶点的度
    for (int64_t i = 0; i < edge_num; ++i) {
      vid_t src_id = src_list_ptr[i];
      ++degree[vid_parser_.GetLabelId(src_id)][vid_parser_.GetOffset(src_id)];
    }
    // 为每一种顶点构建row offset数组
    std::vector<std::vector<int64_t>> offsets(vertex_label_num_);
    
    for (label_id_t v_label = 0; v_label != vertex_label_num_; ++v_label) {
      auto tvnum = tvnums_[v_label];
      auto& offset_vec = offsets[v_label];
      auto& degree_vec = degree[v_label];
      arrow::Int64Builder builder;

      offset_vec.resize(tvnum + 1);
      offset_vec[0] = 0;

      for (vid_t i = 0; i < tvnum; ++i) {
        offset_vec[i + 1] = offset_vec[i] + degree_vec[i];
      }
    }
    
    std::vector<vineyard::PodArrayBuilder<nbr_unit_t>> edge_builders(vertex_label_num_);
    
    for (int64_t i = 0; i < edge_num; ++i) {
        vid_t src_id = src_list_ptr[i];
        label_id_t v_label = vid_parser_.GetLabelId(src_id);
        int64_t v_offset = vid_parser_.GetOffset(src_id);
        // 获取目的顶点指针
        nbr_unit_t* ptr =
            edge_builders[v_label].MutablePointer(offsets[v_label][v_offset]);
        // 填充目的顶点lid、eid，其中eid用于之后获取边上的属性
        ptr->vid = dst_list->Value(i);
        ptr->eid = static_cast<eid_t>(i);
        // 更新offset，指向源顶点的下一条出边
        ++offsets[v_label][v_offset];
    }
    
    // 对临接点按照lid排序
    for (label_id_t v_label = 0; v_label != vertex_label_num_; ++v_label) {
      auto& builder = edge_builders[v_label];
      auto tvnum = tvnums_[v_label];
      const int64_t* offsets_ptr = edge_offsets[v_label]->raw_values();
            
      for (vid_t i = 0; i < tvnum; ++i) {
          nbr_unit_t* begin = builder.MutablePointer(offsets_ptr[i]);
          nbr_unit_t* end = builder.MutablePointer(offsets_ptr[i + 1]);
          std::sort(begin, end,
                    [](const nbr_unit_t& lhs, const nbr_unit_t& rhs) {
                      return lhs.vid < rhs.vid;
                    });
        }
    }
}
```
      构建入边csr和出边同理。对于无向图，**出边和入边共用一个CSR。**逻辑和有向图类似，但是要同时统计源顶点和目的顶点的度来构建row offset数组。访问每条边，使用row offset数组定位到源顶点的nbr填充入边的src lid和eid；同时还要使用offset数组定位到目的顶点的nbr填充dst lid和eid

5) 封装
   准备好顶点数量、CSR、ovgid和g2l_map后，将他们封装成各种VineyardObject。

### 1.6 Fragment构建
根据Metadata从Vineyard查询和构建以下数据结构作为ArrowFragment的成员：
也就是说，以下数据都是存储在vineyard中的
```cpp
fid_t fid_, fnum_; // 当前fragment id，fragment总数
bool directed_;    // 是否为有向图
label_id_t vertex_label_num_; // 顶点种类数量
label_id_t edge_label_num_;  // 边种类数量
PropertyGraphSchema schema_; // shcema
vineyard::Array<vid_t> ivnums_, ovnums_, tvnums_; // 内部点、外部点、内部和外部点总数
std::vector<std::shared_ptr<arrow::Table>> vertex_tables_; // 顶点属性表，不包含id列
std::vector<std::shared_ptr<vid_array_t>> ovgid_lists_;  // 外部点gid
std::vector<std::shared_ptr<vineyard::Hashmap<vid_t, vid_t>>> ovg2l_maps_; // gid到lid映射
std::vector<std::shared_ptr<arrow::Table>> edge_tables_; // 边属性，不包含id列
  std::vector<std::vector<std::shared_ptr<arrow::FixedSizeBinaryArray>>>
      ie_lists_, oe_lists_; // CSR结构的Column index，对于无向图只有oe_lists_被使用
  std::vector<std::vector<std::shared_ptr<arrow::Int64Array>>>
      ie_offsets_lists_, oe_offsets_lists_; // CSR结构的Row offset
std::shared_ptr<vertex_map_t> vm_ptr_; // vertex map
```
剩下的成员将根据以上信息来构建
```cpp
std::vector<std::vector<const void*>> edge_tables_columns_; // 每一种边的属性列地址
std::vector<const void**> flatten_edge_tables_columns_; // 同上
std::vector<std::vector<const void*>> vertex_tables_columns_; // 每一种顶点的属性列地址
std::vector<std::vector<const nbr_unit_t*>> ie_ptr_lists_, oe_ptr_lists_; // 每一种顶点的每一种入/出边的起始地址（也就是column index的起始地址）
std::vector<std::vector<const int64_t*>> ie_offsets_ptr_lists_, oe_offsets_ptr_lists_; // 每一种顶点的每一种入/出边的row offset起始地址（也就是column offset的起始地址）

// 以下是每一种顶点、每一种边的源顶点fid、目的顶点fid、源顶点和目的顶点fid构成的CSR结构
std::vector<std::vector<std::vector<fid_t>>> idst_, odst_, iodst_;
std::vector<std::vector<std::vector<fid_t*>>> idoffset_, odoffset_, iodoffset_;
```
## 2. 访问Fragment
描述了访问顶点、边、顶点数据和边数据的API和实现
### 2.1 访问顶点
#### 2.1.1 访问内部点
内部点范围由起始lid和终止lid构成，lid的编码包含了顶点label id和顶点序号。内部点的顶点序号范围为[0, ivnum)；外部点序号范围为[ivnum, tvnum)；全部顶点序号为[0, tvnum)。
```cpp
vertex_range_t InnerVertices(label_id_t label_id) const {
    // 生成lid的起始和结束范围，结束范围是开区间
    return vertex_range_t(
        vid_parser_.GenerateId(0, label_id, 0),
        vid_parser_.GenerateId(0, label_id, ivnums_[label_id]));
}
```
#### 2.1.2 范围外部点
```cpp
vertex_range_t OuterVertices(label_id_t label_id) const {
    return vertex_range_t(
        vid_parser_.GenerateId(0, label_id, ivnums_[label_id]),
        vid_parser_.GenerateId(0, label_id, tvnums_[label_id]));
}
```
#### 2.1.3 访问全部点
```cpp
vertex_range_t Vertices(label_id_t label_id) const {
    return vertex_range_t(
        vid_parser_.GenerateId(0, label_id, 0),
        vid_parser_.GenerateId(0, label_id, tvnums_[label_id]));
}
```
#### 2.1.4 访问顶点数据
根据顶点lid获取顶点类型，属性id（列号）获取到列指针，然后从lid获取到顶点序号（行号）即可访问到顶点的某个属性。
```cpp
  template <typename T>
  T GetData(const vertex_t& v, prop_id_t prop_id) const {
    return property_graph_utils::ValueGetter<T>::Value(
        vertex_tables_columns_[vid_parser_.GetLabelId(v.GetValue())][prop_id],
        vid_parser_.GetOffset(v.GetValue()));
  }
```
### 2.2 访问边
#### 2.2.1 访问出边
访问出边需要给出源顶点lid和某一种边的类型。而源顶点的类型已经被lid编码所包含，所以不用给出。
```cpp
adj_list_t GetOutgoingAdjList(const vertex_t& v, label_id_t e_label) const {
    vid_t vid = v.GetValue(); // 获取源顶点lid
    label_id_t v_label = vid_parser_.GetLabelId(vid); // 获取源顶点类型
    int64_t v_offset = vid_parser_.GetOffset(vid); // 获取源顶点序号
    const int64_t* offset_array = oe_offsets_ptr_lists_[v_label][e_label]; // 访问row offset
    const nbr_unit_t* oe = oe_ptr_lists_[v_label][e_label];  // 获取出边起始地址
    return adj_list_t(&oe[offset_array[v_offset]],
                      &oe[offset_array[v_offset + 1]],
                      flatten_edge_tables_columns_[e_label]); // 生成AdjList对象，分别为出边起始地址、结束地址、边属性列地址
}
```
#### 2.2.2 访问入边
和出边同理，只需要把oe相关变量名换成ie即可
```cpp
adj_list_t GetIncomingAdjList(const vertex_t& v, label_id_t e_label) const {
    vid_t vid = v.GetValue();
    label_id_t v_label = vid_parser_.GetLabelId(vid);
    int64_t v_offset = vid_parser_.GetOffset(vid);
    const int64_t* offset_array = ie_offsets_ptr_lists_[v_label][e_label];
    const nbr_unit_t* ie = ie_ptr_lists_[v_label][e_label];
    return adj_list_t(&ie[offset_array[v_offset]],
                      &ie[offset_array[v_offset + 1]],
                      flatten_edge_tables_columns_[e_label]);
}
```
#### 2.2.3 访问边上的数据
因为在AdjList中已经包含了出边属性表的起始地址，每条出边还包含了边id（对应到行号），因此再给出属性的列号就能够去得到具体的数据。
```cpp
例如：
// sssp，获取edge weight
for (label_id_t j = 0; j < e_label_num; ++j) {
      auto es = frag.GetOutgoingAdjList(source, j);
      for (auto& e : es) {
        auto u = e.neighbor();
        auto u_dist = static_cast<double>(e.template get_data<int64_t>(0));
      }
}

template <typename VID_T, typename EID_T>
struct Nbr {
  template <typename T>
  T get_data(prop_id_t prop_id) const {
    // 获取属性列的地址，和eid就能够获取到具体的属性
    return ValueGetter<T>::Value(edata_arrays_[prop_id], nbr_->eid);
  }
}
```
