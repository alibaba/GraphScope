/**
 * Copyright 2020 Alibaba Group Holding Limited.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.maxgraph.dataload.databuild;

import com.alibaba.maxgraph.compiler.api.exception.InvalidDataException;
import com.alibaba.maxgraph.compiler.api.schema.GraphEdge;
import com.alibaba.maxgraph.compiler.api.schema.GraphElement;
import com.alibaba.maxgraph.compiler.api.schema.GraphSchema;
import com.alibaba.maxgraph.compiler.api.schema.GraphVertex;
import com.alibaba.graphscope.groot.schema.PropertyValue;
import com.alibaba.maxgraph.common.util.PkHashUtils;
import com.alibaba.maxgraph.common.util.SchemaUtils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DataEncoder {

    private static final long SNAPSHOT_ID = ~0L;
    private ByteBuffer scratch = ByteBuffer.allocate(1 << 20);
    private Map<GraphElement, List<Integer>> labelPkIds = new HashMap<>();

    private Map<Integer, Codec> labelToCodec;

    public DataEncoder(GraphSchema graphSchema) {
        this.labelToCodec = buildCodecs(graphSchema);
    }

    public BytesRef encodeVertexKey(GraphVertex type, Map<Integer, PropertyValue> propertiesMap, long tableId) {
        scratch.clear();
        List<Integer> pkIds = labelPkIds.computeIfAbsent(type, k -> SchemaUtils.getVertexPrimaryKeyList(type));
        long hashId = getHashId(type.getLabelId(), propertiesMap, pkIds);
        scratch.putLong(tableId << 1);
        scratch.putLong(hashId);
        scratch.putLong(SNAPSHOT_ID);
        scratch.flip();
        return new BytesRef(scratch.array(), 0, scratch.limit());
    }

    public BytesRef encodeEdgeKey(GraphVertex srcType, Map<Integer, PropertyValue> srcPkMap, GraphVertex dstType,
                                  Map<Integer, PropertyValue> dstPkMap, GraphEdge type,
                                  Map<Integer, PropertyValue> propertiesMap, long tableId, boolean outEdge) {
        scratch.clear();
        List<Integer> srcPkIds = labelPkIds.computeIfAbsent(srcType, k -> SchemaUtils.getVertexPrimaryKeyList(srcType));
        long srcId = getHashId(srcType.getLabelId(), srcPkMap, srcPkIds);
        List<Integer> dstPkIds = labelPkIds.computeIfAbsent(dstType, k -> SchemaUtils.getVertexPrimaryKeyList(dstType));
        long dstId = getHashId(dstType.getLabelId(), dstPkMap, dstPkIds);
        List<Integer> edgePkIds = labelPkIds.computeIfAbsent(type, k -> SchemaUtils.getEdgePrimaryKeyList(type));
        long eid;
        if (edgePkIds != null && edgePkIds.size() > 0) {
            eid = getHashId(type.getLabelId(), propertiesMap, edgePkIds);
        } else {
            eid = System.nanoTime();
        }

        if (outEdge) {
            scratch.putLong(tableId << 1);
            scratch.putLong(srcId);
            scratch.putLong(dstId);
        } else {
            scratch.putLong(tableId << 1 | 1);
            scratch.putLong(dstId);
            scratch.putLong(srcId);
        }
        scratch.putLong(eid);
        scratch.putLong(SNAPSHOT_ID);
        scratch.flip();
        return new BytesRef(scratch.array(), 0, scratch.limit());
    }

    public BytesRef encodeProperties(int labelId, Map<Integer, PropertyValue> propertiesMap) {
        scratch.clear();
        Codec codec = this.labelToCodec.get(labelId);
        codec.encode(propertiesMap, scratch);
        scratch.flip();
        return new BytesRef(scratch.array(), 0, scratch.limit());
    }

    private long getHashId(int labelId, Map<Integer, PropertyValue> operationProperties, List<Integer> pkIds) {
        List<byte[]> pks = new ArrayList<>(pkIds.size());
        for (int pkId : pkIds) {
            PropertyValue propertyValue = operationProperties.get(pkId);
            if (propertyValue == null) {
                throw new InvalidDataException("label [" + labelId + "], propertyId [" + pkId + "]");
            }
            byte[] valBytes = propertyValue.getValBytes();
            pks.add(valBytes);
        }
        return PkHashUtils.hash(labelId, pks);
    }

    private Map<Integer, Codec> buildCodecs(GraphSchema graphSchema) {
        Map<Integer, Codec> res = new HashMap<>();
        for (GraphVertex graphVertex : graphSchema.getVertexList()) {
            res.put(graphVertex.getLabelId(), new Codec(graphVertex));
        }
        for (GraphEdge graphEdge : graphSchema.getEdgeList()) {
            res.put(graphEdge.getLabelId(), new Codec(graphEdge));
        }
        return res;
    }
}
