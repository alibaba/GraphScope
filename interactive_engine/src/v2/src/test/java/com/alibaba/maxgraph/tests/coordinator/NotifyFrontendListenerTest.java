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
package com.alibaba.maxgraph.tests.coordinator;

import com.alibaba.maxgraph.groot.common.CompletionCallback;
import com.alibaba.maxgraph.groot.common.schema.GraphDef;
import com.alibaba.maxgraph.groot.coordinator.FrontendSnapshotClient;
import com.alibaba.maxgraph.groot.coordinator.NotifyFrontendListener;
import com.alibaba.maxgraph.groot.coordinator.SchemaManager;
import org.junit.jupiter.api.Test;

import static org.mockito.Mockito.*;

public class NotifyFrontendListenerTest {

    @Test
    void testListener() {
        FrontendSnapshotClient frontendSnapshotClient = mock(FrontendSnapshotClient.class);
        SchemaManager schemaManager = mock(SchemaManager.class);
        GraphDef graphDef = GraphDef.newBuilder().setVersion(3L).build();
        when(schemaManager.getGraphDef()).thenReturn(graphDef);
        doAnswer(invocationOnMock -> {
            long snapshotId = invocationOnMock.getArgument(0);
            CompletionCallback<Long> callback = invocationOnMock.getArgument(2);
            callback.onCompleted(snapshotId - 1);
            return null;
        }).when(frontendSnapshotClient).advanceQuerySnapshot(anyLong(), any(), any());

        NotifyFrontendListener listener = new NotifyFrontendListener(0, frontendSnapshotClient, schemaManager);
        listener.snapshotAdvanced(10L, 10L);
        verify(frontendSnapshotClient).advanceQuerySnapshot(eq(10L), eq(graphDef), any());

        listener.snapshotAdvanced(20L, 10L);
        verify(frontendSnapshotClient).advanceQuerySnapshot(eq(20L), isNull(), any());
    }
}
