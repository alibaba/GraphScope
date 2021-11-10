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
package com.alibaba.maxgraph.tests.frontend;

import com.alibaba.graphscope.groot.SnapshotListener;
import com.alibaba.graphscope.groot.schema.GraphDef;
import com.alibaba.graphscope.groot.SnapshotCache;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

public class SnapshotCacheTest {

    @Test
    void testSnapshotCache() {
        SnapshotCache snapshotCache = new SnapshotCache();
        GraphDef graphDef = null;
        snapshotCache.advanceQuerySnapshotId(5L, graphDef);
        assertEquals(snapshotCache.getSnapshotWithSchema().getSnapshotId(), 5L);

        SnapshotListener listener1 = mock(SnapshotListener.class);
        snapshotCache.addListener(5L, listener1);
        verify(listener1, times(1)).onSnapshotAvailable();

        SnapshotListener listener2 = mock(SnapshotListener.class);
        snapshotCache.addListener(6L, listener2);
        snapshotCache.advanceQuerySnapshotId(6L, graphDef);
        verify(listener2, times(1)).onSnapshotAvailable();

        snapshotCache.advanceQuerySnapshotId(7L, graphDef);
        verify(listener1, times(1)).onSnapshotAvailable();
        verify(listener2, times(1)).onSnapshotAvailable();
    }
}
