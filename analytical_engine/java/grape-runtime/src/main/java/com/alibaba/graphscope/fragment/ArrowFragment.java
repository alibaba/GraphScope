/*
 * Copyright 2021 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.graphscope.fragment;

import static com.alibaba.graphscope.utils.CppClassName.GRAPE_VERTEX;
import static com.alibaba.graphscope.utils.CppClassName.GRAPE_VERTEX_RANGE;

import com.alibaba.fastffi.CXXHead;
import com.alibaba.fastffi.CXXReference;
import com.alibaba.fastffi.CXXTemplate;
import com.alibaba.fastffi.CXXValue;
import com.alibaba.fastffi.FFIConst;
import com.alibaba.fastffi.FFIGen;
import com.alibaba.fastffi.FFINameAlias;
import com.alibaba.fastffi.FFIPointer;
import com.alibaba.fastffi.FFISkip;
import com.alibaba.fastffi.FFITypeAlias;
import com.alibaba.graphscope.ds.DestList;
import com.alibaba.graphscope.ds.EdgeDataColumn;
import com.alibaba.graphscope.ds.PropertyAdjList;
import com.alibaba.graphscope.ds.PropertyRawAdjList;
import com.alibaba.graphscope.ds.Vertex;
import com.alibaba.graphscope.ds.VertexDataColumn;
import com.alibaba.graphscope.ds.VertexRange;
import com.alibaba.graphscope.utils.CppClassName;
import com.alibaba.graphscope.utils.CppHeaderName;
import com.alibaba.graphscope.utils.JNILibraryName;

/**
 * ArrowFragment is the java wrapper for <a href=
 * "https://github.com/v6d-io/v6d/blob/main/modules/graph/fragment/arrow_fragment.h#L177">vineyard::ArrowFragment</a>.
 *
 * <p>LABEL_ID_TYPE=int,PROP_ID_TYPE=int EID_TYPE=uint64_t
 *
 * @param <OID_T> original vertex id type
 */
@FFIGen(library = JNILibraryName.JNI_LIBRARY_NAME)
@CXXHead(CppHeaderName.ARROW_FRAGMENT_H)
@CXXHead(CppHeaderName.CORE_JAVA_TYPE_ALIAS_H)
@CXXHead(system = "stdint.h")
@FFITypeAlias(CppClassName.ARROW_FRAGMENT)
@CXXTemplate(
        cxx = {"int64_t"},
        java = {"Long"})
@CXXTemplate(
        cxx = {"int32_t"},
        java = {"Integer"})
public interface ArrowFragment<OID_T> extends FFIPointer {
    int fid();

    int fnum();

    @FFINameAlias("vertex_label")
    int vertexLabel(
            @FFIConst @FFITypeAlias(GRAPE_VERTEX + "<uint64_t>") @CXXReference Vertex<Long> vertex);

    @FFINameAlias("vertex_offset")
    long vertexOffset(
            @FFIConst @FFITypeAlias(GRAPE_VERTEX + "<uint64_t>") @CXXReference Vertex<Long> vertex);

    @FFINameAlias("vertex_label_num")
    int vertexLabelNum();

    @FFINameAlias("edge_label_num")
    int edgeLabelNum();

    @FFINameAlias("vertex_property_num")
    int vertexPropertyNum(int label);

    // @FFINameAlias("vertex_property_type")

    @FFINameAlias("edge_property_num")
    int edgePropertyNum(int label);
    // @FFINameAlias("edge_property_type")

    // @FFINameAlias("vertex_property_type")
    // @CXXValue SharedPtr<ArrowDataType> vertexPropertyType(int labelId, int
    // propertyId);

    // @FFINameAlias("edge_property_type")
    // @CXXValue SharedPtr<ArrowDataType> edgePropertyType(int labelId, int
    // propertyId);

    // @FFINameAlias("vertex_data_table")
    // @CXXValue SharedPtr<ArrowTable> vertexDataTable(int labelId);
    //
    // @FFINameAlias("edge_data_table")
    // @CXXValue SharedPtr<ArrowTable> EdgeDataTable(int labelId);

    @FFINameAlias("Vertices")
    @CXXValue
    @FFITypeAlias(GRAPE_VERTEX_RANGE + "<uint64_t>")
    VertexRange<Long> vertices(int labelId);

    @FFINameAlias("InnerVertices")
    @CXXValue
    @FFITypeAlias(GRAPE_VERTEX_RANGE + "<uint64_t>")
    VertexRange<Long> innerVertices(int labelId);

    @FFINameAlias("OuterVertices")
    @CXXValue
    @FFITypeAlias(GRAPE_VERTEX_RANGE + "<uint64_t>")
    VertexRange<Long> outerVertices(int labelId);

    /**
     * Get the number of vertices in this fragment, i.e. ivnum + ovnum.
     *
     * @param labelId vertex label id
     * @return number of vertices labeled with labelId
     */
    @FFINameAlias("GetVerticesNum")
    long getVerticesNum(int labelId);

    @FFINameAlias("GetInnerVerticesNum")
    long getInnerVerticesNum(int labelId);

    @FFINameAlias("GetOuterVerticesNum")
    long getOuterVerticesNum(int labelId);

    @FFINameAlias("GetTotalNodesNum")
    int getTotalNodesNum();

    @FFINameAlias("GetTotalVerticesNum")
    int getTotalVerticesNum();

    @FFINameAlias("GetTotalVerticesNum")
    int getTotalVerticesNum(int labelId);

    /**
     * Get vertex's oid with lid.
     *
     * @param vertex querying vertex
     * @return original id for vertex
     */
    @FFINameAlias("GetId")
    @CXXValue
    OID_T getOid(@FFITypeAlias(GRAPE_VERTEX + "<uint64_t>") @CXXReference Vertex<Long> vertex);

    /**
     * Get vertex's lid with oid provided, set int vertex.
     *
     * @param labelId label for oid.
     * @param oid querying oid.
     * @param vertex vertex hanlder
     * @return true if vertex with original id oid exists in this fragment.
     */
    @FFINameAlias("GetVertex")
    boolean getVertex(
            int labelId,
            OID_T oid,
            @FFITypeAlias(GRAPE_VERTEX + "<uint64_t>") @CXXReference Vertex<Long> vertex);

    @FFINameAlias("GetFragId")
    int getFragId(
            @FFIConst @FFITypeAlias(GRAPE_VERTEX + "<uint64_t>") @CXXReference Vertex<Long> vertex);

    @FFINameAlias("IsInnerVertex")
    boolean isInnerVertex(
            @FFIConst @FFITypeAlias(GRAPE_VERTEX + "<uint64_t>") @CXXReference Vertex<Long> vertex);

    @FFINameAlias("IsOuterVertex")
    boolean isOuterVertex(
            @FFIConst @FFITypeAlias(GRAPE_VERTEX + "<uint64_t>") @CXXReference Vertex<Long> vertex);

    /**
     * Get the outgoing edges.
     *
     * @param vertex querying vertex.
     * @param edgeLabelId label for the edges you are querying.
     * @return obtained edges.
     */
    @FFINameAlias("GetOutgoingAdjList")
    @CXXValue
    @FFITypeAlias(CppClassName.PROPERTY_ADJ_LIST + "<uint64_t>")
    PropertyAdjList<Long> getOutgoingAdjList(
            @FFIConst @FFITypeAlias(GRAPE_VERTEX + "<uint64_t>") @CXXReference Vertex<Long> vertex,
            int edgeLabelId);

    @FFINameAlias("GetIncomingAdjList")
    @CXXValue
    @FFITypeAlias(CppClassName.PROPERTY_ADJ_LIST + "<uint64_t>")
    PropertyAdjList<Long> getIncomingAdjList(
            @FFIConst @FFITypeAlias(GRAPE_VERTEX + "<uint64_t>") @CXXReference Vertex<Long> vertex,
            int edgeLabelId);

    @FFINameAlias("GetOutgoingRawAdjList")
    @CXXValue
    @FFITypeAlias(CppClassName.PROPERTY_RAW_ADJ_LIST + "<uint64_t>")
    PropertyRawAdjList<Long> getOutgoingRawAdjList(
            @FFIConst @FFITypeAlias(GRAPE_VERTEX + "<uint64_t>") @CXXReference Vertex<Long> vertex,
            int edgeLabelId);

    @FFINameAlias("GetIncomingRawAdjList")
    @CXXValue
    @FFITypeAlias(CppClassName.PROPERTY_RAW_ADJ_LIST + "<uint64_t>")
    PropertyRawAdjList<Long> getIncomingRawAdjList(
            @FFIConst @FFITypeAlias(GRAPE_VERTEX + "<uint64_t>") @CXXReference Vertex<Long> vertex,
            int edgeLabelId);

    @CXXTemplate(
            cxx = {"int64_t"},
            java = {"Long"})
    @CXXTemplate(
            cxx = {"double"},
            java = {"Double"})
    @CXXTemplate(
            cxx = {"int32_t"},
            java = {"Integer"})
    @FFINameAlias("edge_data_column")
    @CXXValue
    <DATA_T> EdgeDataColumn<DATA_T> edgeDataColumn(
            int vertexLabelId, int propertyId, @FFISkip DATA_T unused);

    // @CXXTemplate(
    // cxx = {"uint64_t"},
    // java = {"Long"})
    // @CXXTemplate(
    // cxx = {"double"},
    // java = {"Double"})
    // @CXXTemplate(
    // cxx = {"uint32_t"},
    // java = {"Integer"})
    @FFINameAlias("vertex_data_column")
    @CXXValue
    <DATA_T> VertexDataColumn<DATA_T> vertexDataColumn(
            int vertexLabelId, int propertyId, @FFISkip DATA_T unused);

    @FFINameAlias("GetData<uint64_t>")
    long getLongData(
            @FFITypeAlias(GRAPE_VERTEX + "<uint64_t>") @CXXReference Vertex<Long> vertex,
            int propertyId);

    @FFINameAlias("GetData<uint32_t>")
    long getIntData(
            @FFITypeAlias(GRAPE_VERTEX + "<uint64_t>") @CXXReference Vertex<Long> vertex,
            int propertyId);

    @FFINameAlias("GetData<double>")
    long getDoubleData(
            @FFITypeAlias(GRAPE_VERTEX + "<uint64_t>") @CXXReference Vertex<Long> vertex,
            int propertyId);

    @FFINameAlias("HasChild")
    boolean hasChild(
            @FFITypeAlias(GRAPE_VERTEX + "<uint64_t>") @CXXReference Vertex<Long> vertex,
            int edgeLabelId);

    @FFINameAlias("HasParent")
    boolean hasParent(
            @FFITypeAlias(GRAPE_VERTEX + "<uint64_t>") @CXXReference Vertex<Long> vertex,
            int edgeLabelId);

    @FFINameAlias("GetLocalOutDegree")
    int getLocalOutDegree(
            @FFITypeAlias(GRAPE_VERTEX + "<uint64_t>") @CXXReference Vertex<Long> vertex,
            int edgeLabelId);

    @FFINameAlias("GetLocalInDegree")
    int getLocalInDegree(
            @FFITypeAlias(GRAPE_VERTEX + "<uint64_t>") @CXXReference Vertex<Long> vertex,
            int edgeLabelId);

    @FFINameAlias("Gid2Vertex")
    boolean gid2Vertex(
            Long gid, @FFITypeAlias(GRAPE_VERTEX + "<uint64_t>") @CXXReference Vertex<Long> vertex);

    @FFINameAlias("Vertex2Gid")
    Long vertex2Gid(@CXXReference @FFITypeAlias(GRAPE_VERTEX + "<uint64_t>") Vertex<Long> vertex);

    /**
     * Try to get oid's corresponding vertex, if not inner vertex, return false.
     *
     * @param vertexLabelId vertex label id
     * @param oid input oid
     * @param vertex output vertex
     * @return true if operation succeeds.
     */
    @FFINameAlias("GetInnerVertex")
    boolean getInnerVertex(
            int vertexLabelId,
            OID_T oid,
            @FFITypeAlias(GRAPE_VERTEX + "<uint64_t>") @CXXReference Vertex<Long> vertex);

    @FFINameAlias("GetOuterVertex")
    boolean getOuterVertex(
            int vertexLabelId,
            OID_T oid,
            @FFITypeAlias(GRAPE_VERTEX + "<uint64_t>") @CXXReference Vertex<Long> vertex);

    @FFINameAlias("GetInnerVertexId")
    OID_T getInnerVertexOid(
            @CXXReference @FFITypeAlias(GRAPE_VERTEX + "<uint64_t>") Vertex<Long> vertex);

    @FFINameAlias("GetOuterVertexId")
    OID_T getOuterVertexOid(
            @CXXReference @FFITypeAlias(GRAPE_VERTEX + "<uint64_t>") Vertex<Long> vertex);

    @FFINameAlias("Gid2Oid")
    OID_T gid2Oid(Long gid);

    // Oid2Gid can not be made in java, since java pass primitives, even long in
    // value.
    // @FFINameAlias("Oid2Gid")
    // boolean oid2Gid(int vertexLabelId, OID_T oid, Long gid);
    @FFINameAlias("Oid2Gid")
    boolean oid2Gid(
            int vertexLabelId,
            OID_T oid,
            @FFITypeAlias(GRAPE_VERTEX + "<uint64_t>") @CXXReference Vertex<Long> gid);

    @FFINameAlias("InnerVertexGid2Vertex")
    boolean innerVertexGid2Vertex(
            Long gid, @FFITypeAlias(GRAPE_VERTEX + "<uint64_t>") @CXXReference Vertex<Long> vertex);

    @FFINameAlias("OuterVertexGid2Vertex")
    boolean outerVertexGid2Vertex(
            Long gid, @FFITypeAlias(GRAPE_VERTEX + "<uint64_t>") @CXXReference Vertex<Long> vertex);

    @FFINameAlias("GetOuterVertexGid")
    Long getOuterVertexGid(
            @FFITypeAlias(GRAPE_VERTEX + "<uint64_t>") @CXXReference Vertex<Long> vertex);

    @FFINameAlias("GetInnerVertexGid")
    Long getInnerVertexGid(
            @FFITypeAlias(GRAPE_VERTEX + "<uint64_t>") @CXXReference Vertex<Long> vertex);

    @FFINameAlias("IEDests")
    @CXXValue
    DestList ieDests(
            @FFITypeAlias(GRAPE_VERTEX + "<uint64_t>") @CXXReference Vertex<Long> vertex,
            int edgeLabelId);

    @FFINameAlias("OEDests")
    @CXXValue
    DestList oeDests(
            @FFITypeAlias(GRAPE_VERTEX + "<uint64_t>") @CXXReference Vertex<Long> vertex,
            int edgeLabelId);

    @FFINameAlias("IOEDests")
    @CXXValue
    DestList ioeDests(
            @FFITypeAlias(GRAPE_VERTEX + "<uint64_t>") @CXXReference Vertex<Long> vertex,
            int edgeLabelId);

    @FFINameAlias("directed")
    boolean directed();
}
