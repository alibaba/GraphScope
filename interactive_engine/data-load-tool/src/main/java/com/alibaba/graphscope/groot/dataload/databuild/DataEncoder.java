/**
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.graphscope.groot.dataload.databuild;

import com.alibaba.graphscope.groot.common.exception.InvalidDataException;
import com.alibaba.graphscope.groot.common.schema.api.GraphEdge;
import com.alibaba.graphscope.groot.common.schema.api.GraphElement;
import com.alibaba.graphscope.groot.common.schema.api.GraphSchema;
import com.alibaba.graphscope.groot.common.schema.api.GraphVertex;
import com.alibaba.graphscope.groot.common.schema.wrapper.DataType;
import com.alibaba.graphscope.groot.common.schema.wrapper.PropertyValue;
import com.alibaba.graphscope.groot.common.util.PkHashUtils;
import com.alibaba.graphscope.groot.common.util.SchemaUtils;

import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.*;

public class DataEncoder {

    private static final long SNAPSHOT_ID = ~0L;
    private final ByteBuffer scratch = ByteBuffer.allocate(1 << 20);
    private final Map<GraphElement, List<Integer>> labelPkIds = new HashMap<>();

    private final Map<Integer, Codec> labelToCodec;

    public DataEncoder(GraphSchema graphSchema) {
        this.labelToCodec = buildCodecs(graphSchema);
    }

    public BytesRef encodeVertexKey(
            GraphVertex type, Map<Integer, PropertyValue> propertiesMap, long tableId) {
        clear(scratch);
        List<Integer> pkIds =
                labelPkIds.computeIfAbsent(type, k -> SchemaUtils.getVertexPrimaryKeyList(type));
        long hashId = getHashId(type.getLabelId(), propertiesMap, pkIds);
        scratch.putLong(tableId << 1);
        scratch.putLong(hashId);
        scratch.putLong(SNAPSHOT_ID);
        flip(scratch);
        return new BytesRef(scratch.array(), 0, scratch.limit());
    }

    public BytesRef encodeEdgeKey(
            GraphVertex srcType,
            Map<Integer, PropertyValue> srcPkMap,
            GraphVertex dstType,
            Map<Integer, PropertyValue> dstPkMap,
            GraphEdge type,
            Map<Integer, PropertyValue> propertiesMap,
            long tableId,
            boolean outEdge) {
        clear(scratch);
        List<Integer> srcPkIds =
                labelPkIds.computeIfAbsent(
                        srcType, k -> SchemaUtils.getVertexPrimaryKeyList(srcType));
        long srcId = getHashId(srcType.getLabelId(), srcPkMap, srcPkIds);
        List<Integer> dstPkIds =
                labelPkIds.computeIfAbsent(
                        dstType, k -> SchemaUtils.getVertexPrimaryKeyList(dstType));
        long dstId = getHashId(dstType.getLabelId(), dstPkMap, dstPkIds);
        List<Integer> edgePkIds =
                labelPkIds.computeIfAbsent(type, k -> SchemaUtils.getEdgePrimaryKeyList(type));
        long eid;
        if (edgePkIds != null && edgePkIds.size() > 0) {
            List<byte[]> pkBytes = getPkBytes(type.getLabelId(), propertiesMap, edgePkIds);
            eid = PkHashUtils.hash(srcId, dstId, type.getLabelId(), pkBytes);
        } else {
            eid = PkHashUtils.hash(srcId, dstId, type.getLabelId(), System.nanoTime());
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
        flip(scratch);
        return new BytesRef(scratch.array(), 0, scratch.limit());
    }

    public BytesRef encodeProperties(int labelId, Map<Integer, PropertyValue> propertiesMap) {
        clear(scratch);
        Codec codec = this.labelToCodec.get(labelId);
        codec.encode(propertiesMap, scratch);
        flip(scratch);
        return new BytesRef(scratch.array(), 0, scratch.limit());
    }

    private static List<byte[]> getPkBytes(
            int labelId, Map<Integer, PropertyValue> operationProperties, List<Integer> pkIds) {
        List<byte[]> pks = new ArrayList<>(pkIds.size());
        for (int pkId : pkIds) {
            PropertyValue propertyValue = operationProperties.get(pkId);
            if (propertyValue == null) {
                throw new InvalidDataException(
                        "label [" + labelId + "], propertyId [" + pkId + "]");
            }
            byte[] valBytes = propertyValue.getValBytes();
            pks.add(valBytes);
        }
        return pks;
    }

    private static long getHashId(
            int labelId, Map<Integer, PropertyValue> operationProperties, List<Integer> pkIds) {
        List<byte[]> pks = getPkBytes(labelId, operationProperties, pkIds);
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

    public static void clear(Buffer buffer) {
        buffer.clear();
    }

    public static void flip(Buffer buffer) {
        buffer.flip();
    }

    // For testing

    private static void encode(int labelId, long tableId, String pk) {
        Map<Integer, PropertyValue> propertiesMap = new HashMap<>();
        propertiesMap.put(0, new PropertyValue(DataType.STRING, pk));
        List<Integer> pkIds = new ArrayList<>();
        pkIds.add(0);
        ByteBuffer scratch = ByteBuffer.allocate(1 << 20);
        long hashId = getHashId(labelId, propertiesMap, pkIds);
        scratch.putLong(tableId << 1);
        scratch.putLong(hashId);
        scratch.flip();
        System.out.println("TableId: " + scratch.getLong(0) + " | " + scratch.getLong(8));
    }

    private static void encodeTest(int id1, String s1, int id2, String s2, long expected) {
        DataEncoder.encodeTest(id1, 0, s1, id2, 0, s2, 0, expected);
    }

    private static void encodeTest(
            int id1,
            long tableId1,
            String s1,
            int id2,
            long tableId2,
            String s2,
            long expectedTableId,
            long expectedHash) {
        DataEncoder.encode(id1, tableId1, s1);
        DataEncoder.encode(id2, tableId2, s2);
        System.out.println("Expected TableId: " + expectedTableId + "; HashId: " + expectedHash);
    }

    public static void main(String[] args) {
        long expectedHash1 = -3968787722979159891L;
        long expectedTable1 = -9223372036854775698L;
        int labelId1a = 11;
        String s1a = "20230911@META_COLUMN@1@22073998@879591@265939259@4592566627";
        long tableId1 = -4611686018427387857L;
        int labelId1b = 11;
        String s1b = "20230911@META_INDEX@1@16229181@824771@232467064@551697979";
        long tableId2 = -4611686018427387854L;

        long hashId2 = -6575735951890802946L;
        int labelId2a = 5;
        String s2a = "db24f92331e97257e4a0ad4ffc464de2M01";
        int labelId2b = 5;
        String s2b = "889569174b748f2f4514bf6547f12760M01";

        encodeTest(
                labelId1a, tableId1, s1a, labelId1b, tableId2, s1b, expectedTable1, expectedHash1);
        encodeTest(labelId2a, s2a, labelId2b, s2b, hashId2);
    }
}
