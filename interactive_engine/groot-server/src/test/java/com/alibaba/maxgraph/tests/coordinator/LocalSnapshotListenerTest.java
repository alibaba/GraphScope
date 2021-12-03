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
package com.alibaba.maxgraph.tests.coordinator;

import com.alibaba.graphscope.groot.SnapshotCache;
import com.alibaba.graphscope.groot.coordinator.LocalSnapshotListener;
import com.alibaba.graphscope.groot.coordinator.SchemaManager;
import com.alibaba.graphscope.groot.schema.GraphDef;
import org.junit.jupiter.api.Test;

import static org.mockito.Mockito.*;

public class LocalSnapshotListenerTest {

    @Test
    void testListener() {
        SchemaManager schemaManager = mock(SchemaManager.class);
        GraphDef graphDef = GraphDef.newBuilder().setVersion(3L).build();
        when(schemaManager.getGraphDef()).thenReturn(graphDef);
        SnapshotCache snapshotCache = mock(SnapshotCache.class);
        LocalSnapshotListener listener =
                new LocalSnapshotListener(schemaManager, snapshotCache);
        listener.snapshotAdvanced(10L, 10L);
        verify(snapshotCache).advanceQuerySnapshotId(eq(10L), eq(graphDef));
        listener.snapshotAdvanced(20L, 10L);
        verify(snapshotCache).advanceQuerySnapshotId(eq(20L), isNull());
    }
}
