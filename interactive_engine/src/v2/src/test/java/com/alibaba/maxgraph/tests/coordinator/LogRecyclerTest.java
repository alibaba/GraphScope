package com.alibaba.maxgraph.tests.coordinator;

import com.alibaba.maxgraph.v2.common.config.Configs;
import com.alibaba.maxgraph.v2.common.config.CoordinatorConfig;
import com.alibaba.maxgraph.v2.common.wal.LogService;
import com.alibaba.maxgraph.v2.coordinator.LogRecycler;
import com.alibaba.maxgraph.v2.coordinator.SnapshotManager;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.*;

public class LogRecyclerTest {

    @Test
    void testRecycler() throws IOException {
        Configs configs = Configs.newBuilder()
                .put(CoordinatorConfig.LOG_RECYCLE_INTERVAL_SECOND.getKey(), "1")
                .build();
        SnapshotManager mockSnapshotManager = mock(SnapshotManager.class);
        when(mockSnapshotManager.getQueueOffsets()).thenReturn(Arrays.asList(1L, 2L, 3L));
        LogService mockLogService = mock(LogService.class);
        LogRecycler logRecycler = new LogRecycler(configs, mockLogService, mockSnapshotManager);
        CountDownLatch latch1 = new CountDownLatch(1);
        CountDownLatch latch2 = new CountDownLatch(1);
        CountDownLatch latch3 = new CountDownLatch(1);
        doAnswer(invocationOnMock -> {
            latch1.countDown();
            return null;
        }).when(mockLogService).deleteBeforeOffset(0, 1L);
        doAnswer(invocationOnMock -> {
            latch2.countDown();
            return null;
        }).when(mockLogService).deleteBeforeOffset(1, 2L);
        doAnswer(invocationOnMock -> {
            latch3.countDown();
            return null;
        }).when(mockLogService).deleteBeforeOffset(2, 3L);
        logRecycler.start();
        assertAll(
                () -> assertTrue(latch1.await(5L, TimeUnit.SECONDS)),
                () -> assertTrue(latch2.await(5L, TimeUnit.SECONDS)),
                () -> assertTrue(latch3.await(5L, TimeUnit.SECONDS))
        );
        logRecycler.stop();
    }
}
