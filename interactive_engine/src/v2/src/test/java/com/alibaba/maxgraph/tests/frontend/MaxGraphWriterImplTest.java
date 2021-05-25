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
package com.alibaba.maxgraph.tests.frontend;

import com.alibaba.maxgraph.proto.v2.DdlRequestBatchPb;
import com.alibaba.maxgraph.v2.common.BatchId;
import com.alibaba.maxgraph.v2.common.frontend.api.graph.structure.ElementId;
import com.alibaba.maxgraph.v2.common.frontend.cache.MaxGraphCache;
import com.alibaba.maxgraph.v2.common.operation.LabelId;
import com.alibaba.maxgraph.v2.common.schema.DataType;
import com.alibaba.maxgraph.v2.common.schema.EdgeKind;
import com.alibaba.maxgraph.v2.common.schema.GraphDef;
import com.alibaba.maxgraph.v2.common.schema.PropertyDef;
import com.alibaba.maxgraph.v2.common.schema.PropertyValue;
import com.alibaba.maxgraph.v2.common.schema.TypeDef;
import com.alibaba.maxgraph.v2.common.schema.TypeEnum;
import com.alibaba.maxgraph.v2.common.schema.ddl.DdlExecutors;
import com.alibaba.maxgraph.v2.frontend.MaxGraphWriterImpl;
import com.alibaba.maxgraph.v2.frontend.RealtimeWriter;
import com.alibaba.maxgraph.v2.frontend.SchemaWriter;
import com.alibaba.maxgraph.v2.frontend.SnapshotCache;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

public class MaxGraphWriterImplTest {

    @Test
    @Timeout(3)
    void testWriter() throws ExecutionException, InterruptedException {
        RealtimeWriter realtimeWriter = mock(RealtimeWriter.class);
        SchemaWriter schemaWriter = mock(SchemaWriter.class);
        DdlExecutors ddlExecutors = new DdlExecutors();
        SnapshotCache snapshotCache = new SnapshotCache();
        String sessionId = "test_writer";
        MaxGraphCache cache = new MaxGraphCache();

        MaxGraphWriterImpl maxGraphWriter = new MaxGraphWriterImpl(realtimeWriter, schemaWriter, ddlExecutors,
                snapshotCache, sessionId, false, cache);

        assertThrows(IllegalStateException.class, () -> maxGraphWriter.createEdgeType("", null, null));
        snapshotCache.advanceQuerySnapshotId(0L, GraphDef.newBuilder().build());

        PropertyDef propertyDef = PropertyDef.newBuilder()
                .setId(0)
                .setInnerId(0)
                .setName("id")
                .setPk(true)
                .setDataType(DataType.STRING)
                .build();
        PropertyDef propertyDef2 = PropertyDef.newBuilder()
                .setId(1)
                .setInnerId(1)
                .setName("name")
                .setDataType(DataType.STRING)
                .build();
        String vLabel = "v_type";
        String eLabel = "e_type";
        TypeDef vTypeDef = TypeDef.newBuilder()
                .setLabel(vLabel)
                .setTypeEnum(TypeEnum.VERTEX)
                .setLabelId(new LabelId(1))
                .addPropertyDef(propertyDef)
                .addPropertyDef(propertyDef2)
                .build();
        TypeDef eTypeDef = TypeDef.newBuilder()
                .setLabel(eLabel)
                .setLabelId(new LabelId(2))
                .setTypeEnum(TypeEnum.EDGE)
                .addPropertyDef(propertyDef2)
                .build();
        EdgeKind edgeKind = EdgeKind.newBuilder()
                .setSrcVertexLabel(vLabel)
                .setSrcVertexLabelId(new LabelId(1))
                .setDstVertexLabel(vLabel)
                .setDstVertexLabelId(new LabelId(1))
                .setEdgeLabel(eLabel)
                .setEdgeLabelId(new LabelId(2))
                .build();

        when(schemaWriter.submitBatchDdl(anyString(), eq(sessionId), (DdlRequestBatchPb) any()))
                .thenReturn(1L)
                .thenReturn(9L)
                .thenReturn(10L)
                .thenReturn(11L);
        when(realtimeWriter.writeOperations(anyString(), eq(sessionId), any()))
                .thenReturn(new BatchId(2L))
                .thenReturn(new BatchId(3L))
                .thenReturn(new BatchId(4L))
                .thenReturn(new BatchId(5L))
                .thenReturn(new BatchId(6L))
                .thenReturn(new BatchId(7L))
                .thenReturn(new BatchId(8L));

        Future<Integer> f1 = maxGraphWriter.createVertexType(vLabel, Arrays.asList(propertyDef, propertyDef2),
                Arrays.asList("id"));
        assertThrows(IllegalStateException.class, () -> maxGraphWriter.insertVertex("", null));
        Future<Integer> f2 = maxGraphWriter.createEdgeType(eLabel, Arrays.asList(propertyDef2), Collections.EMPTY_LIST);
        Future<Void> f3 = maxGraphWriter.addEdgeRelation(eLabel, vLabel, vLabel);
        Future<Void> commitFuture = maxGraphWriter.commit();

        GraphDef graphDef = GraphDef.newBuilder()
                .setVersion(3)
                .setLabelIdx(2)
                .addTypeDef(vTypeDef)
                .addTypeDef(eTypeDef)
                .addEdgeKind(edgeKind)
                .build();
        snapshotCache.advanceQuerySnapshotId(1L, graphDef);
        assertNull(commitFuture.get());
        assertEquals(f1.get(), 1);
        assertEquals(f2.get(), 2);
        assertNull(f3.get());

        maxGraphWriter.setAutoCommit(true);
        Future<ElementId> ivF1 = maxGraphWriter.insertVertex(vLabel, Collections.singletonMap("id", "a"));
        Map<Integer, PropertyValue> p1 = Collections.singletonMap(0, new PropertyValue(propertyDef.getDataType(), "a"));
        snapshotCache.advanceQuerySnapshotId(2L, null);
        ElementId v1 = ivF1.get();

        Future<ElementId> ivF2 = maxGraphWriter.insertVertex(vLabel, Collections.singletonMap("id", "b"));
        Map<Integer, PropertyValue> p2 = Collections.singletonMap(0, new PropertyValue(propertyDef.getDataType(), "b"));
        snapshotCache.advanceQuerySnapshotId(3L, null);
        ElementId v2 = ivF2.get();

        assertEquals(v1.labelId(), 1);
        assertEquals(v1.id(), maxGraphWriter.getHashId(1, p1, vTypeDef.getPkIdxs(), vTypeDef.getProperties()));
        assertEquals(v2.labelId(), 1);
        assertEquals(v2.id(), maxGraphWriter.getHashId(1, p2, vTypeDef.getPkIdxs(), vTypeDef.getProperties()));

        Future<ElementId> eFuture = maxGraphWriter.insertEdge(v1, v2, eLabel, Collections.singletonMap("name", "x"));
        snapshotCache.advanceQuerySnapshotId(4L, null);
        ElementId e = eFuture.get();
        assertEquals(e.labelId(), 2);

        Future<Void> ueFuture = maxGraphWriter.updateEdgeProperties(v1, v2, e, Collections.singletonMap("name", "y"));
        snapshotCache.advanceQuerySnapshotId(5L, null);
        assertTimeout(Duration.ofSeconds(1), () -> ueFuture.get());

        Future<Void> uvFuture = maxGraphWriter.updateVertexProperties(v1, Collections.singletonMap("name", "abc"));
        snapshotCache.advanceQuerySnapshotId(6L, null);
        assertTimeout(Duration.ofSeconds(1), () -> uvFuture.get());

        Future<Void> deFuture = maxGraphWriter.deleteEdge(v1, v2, e);
        snapshotCache.advanceQuerySnapshotId(7L, null);
        assertTimeout(Duration.ofSeconds(1), () -> deFuture.get());

        Future<Void> dvFuture = maxGraphWriter.deleteVertex(v1);
        snapshotCache.advanceQuerySnapshotId(8L, null);
        assertTimeout(Duration.ofSeconds(1), () -> dvFuture.get());

        Future<Void> derFuture = maxGraphWriter.dropEdgeRelation(eLabel, vLabel, vLabel);
        graphDef = GraphDef.newBuilder(graphDef)
                .setVersion(4)
                .removeEdgeKind(edgeKind)
                .build();
        snapshotCache.advanceQuerySnapshotId(9L, graphDef);
        assertTimeout(Duration.ofSeconds(1), () -> derFuture.get());

        Future<Void> detFuture = maxGraphWriter.dropEdgeType(eLabel);
        graphDef = GraphDef.newBuilder(graphDef)
                .setVersion(5)
                .removeTypeDef(eLabel)
                .build();
        snapshotCache.advanceQuerySnapshotId(10L, graphDef);
        assertTimeout(Duration.ofSeconds(1), () -> detFuture.get());

        Future<Void> dvtFuture = maxGraphWriter.dropVertexType(vLabel);
        graphDef = GraphDef.newBuilder(graphDef)
                .setVersion(6)
                .removeTypeDef(vLabel)
                .build();
        snapshotCache.advanceQuerySnapshotId(11L, graphDef);
        assertTimeout(Duration.ofSeconds(1), () -> dvtFuture.get());
    }
}
