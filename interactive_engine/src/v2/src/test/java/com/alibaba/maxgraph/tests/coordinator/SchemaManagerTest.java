package com.alibaba.maxgraph.tests.coordinator;

import com.alibaba.maxgraph.v2.common.BatchId;
import com.alibaba.maxgraph.v2.common.CompletionCallback;
import com.alibaba.maxgraph.v2.common.MetaService;
import com.alibaba.maxgraph.v2.common.SnapshotListener;
import com.alibaba.maxgraph.v2.common.schema.DataType;
import com.alibaba.maxgraph.v2.common.schema.GraphDef;
import com.alibaba.maxgraph.v2.common.schema.PropertyDef;
import com.alibaba.maxgraph.v2.common.schema.PropertyValue;
import com.alibaba.maxgraph.v2.common.schema.TypeDef;
import com.alibaba.maxgraph.v2.common.schema.TypeEnum;
import com.alibaba.maxgraph.v2.common.schema.request.CreateVertexTypeRequest;
import com.alibaba.maxgraph.v2.common.schema.request.DdlRequestBatch;
import com.alibaba.maxgraph.v2.common.schema.ddl.DdlExecutors;
import com.alibaba.maxgraph.v2.coordinator.DdlWriter;
import com.alibaba.maxgraph.v2.coordinator.GraphDefFetcher;
import com.alibaba.maxgraph.v2.coordinator.SchemaManager;
import com.alibaba.maxgraph.v2.coordinator.SnapshotManager;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.*;

public class SchemaManagerTest {

    @Test
    void testSchemaManager() throws IOException, InterruptedException {
        SnapshotManager mockSnapshotManager = mock(SnapshotManager.class);
        doAnswer(invocationOnMock -> {
            SnapshotListener listener = invocationOnMock.getArgument(1);
            listener.onSnapshotAvailable();
            return null;
        }).when(mockSnapshotManager).addSnapshotListener(anyLong(), any());
        when(mockSnapshotManager.increaseWriteSnapshotId()).thenReturn(1L);
        when(mockSnapshotManager.getCurrentWriteSnapshotId()).thenReturn(1L);

        DdlExecutors ddlExecutors = new DdlExecutors();
        DdlWriter mockDdlWriter = mock(DdlWriter.class);
        when(mockDdlWriter.writeOperations(anyString(), any())).thenReturn(new BatchId(1L));
        MetaService mockMetaService = mock(MetaService.class);

        GraphDefFetcher mockGraphDefFetcher = mock(GraphDefFetcher.class);
        GraphDef initialGraphDef = GraphDef.newBuilder().build();
        when(mockGraphDefFetcher.fetchGraphDef()).thenReturn(initialGraphDef);

        SchemaManager schemaManager = new SchemaManager(mockSnapshotManager, ddlExecutors, mockDdlWriter,
                mockMetaService, mockGraphDefFetcher);
        schemaManager.start();
        assertEquals(initialGraphDef, schemaManager.getGraphDef());

        PropertyValue defaultValue = new PropertyValue(DataType.INT, ByteBuffer.allocate(Integer.BYTES).putInt(1).array());
        PropertyDef propertyDef = new PropertyDef(1, 1, "p1", DataType.INT, defaultValue, true, "property_1");
        TypeDef typeDef = TypeDef.newBuilder()
                .setLabel("vertex1")
                .addPropertyDef(propertyDef)
                .setTypeEnum(TypeEnum.VERTEX)
                .build();

        DdlRequestBatch ddlRequestBatch = DdlRequestBatch.newBuilder()
                .addDdlRequest(new CreateVertexTypeRequest(typeDef))
                .build();

        CountDownLatch latch = new CountDownLatch(1);
        schemaManager.submitBatchDdl("requestId", "sessionId", ddlRequestBatch, new CompletionCallback<Long>() {
            @Override
            public void onCompleted(Long res) {
                latch.countDown();
            }

            @Override
            public void onError(Throwable t) {

            }
        });
        assertTrue(latch.await(5L, TimeUnit.SECONDS));
        schemaManager.stop();
    }
}
