package com.alibaba.maxgraph.tests.frontend;

import com.alibaba.maxgraph.v2.common.SnapshotListener;
import com.alibaba.maxgraph.v2.common.schema.GraphDef;
import com.alibaba.maxgraph.v2.frontend.SnapshotCache;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;
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
