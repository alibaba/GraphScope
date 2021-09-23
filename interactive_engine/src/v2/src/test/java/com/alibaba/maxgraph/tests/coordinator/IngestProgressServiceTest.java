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

import com.alibaba.maxgraph.proto.groot.GetTailOffsetsRequest;
import com.alibaba.maxgraph.proto.groot.GetTailOffsetsResponse;
import com.alibaba.graphscope.groot.coordinator.IngestProgressService;
import com.alibaba.graphscope.groot.coordinator.SnapshotManager;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

public class IngestProgressServiceTest {
    @Test
    void testIngestProgressService() {
        SnapshotManager snapshotManager = mock(SnapshotManager.class);
        when(snapshotManager.getTailOffsets(Arrays.asList(1))).thenReturn(Arrays.asList(10L));
        IngestProgressService ingestProgressService = new IngestProgressService(snapshotManager);
        GetTailOffsetsRequest request = GetTailOffsetsRequest.newBuilder().addQueueId(1).build();

        ingestProgressService.getTailOffsets(
                request,
                new StreamObserver<GetTailOffsetsResponse>() {
                    @Override
                    public void onNext(GetTailOffsetsResponse response) {
                        List<Long> offsetsList = response.getOffsetsList();
                        assertEquals(offsetsList.size(), 1);
                        assertEquals(offsetsList.get(0), 10L);
                    }

                    @Override
                    public void onError(Throwable t) {
                        throw new RuntimeException(t);
                    }

                    @Override
                    public void onCompleted() {}
                });
        verify(snapshotManager).getTailOffsets(Arrays.asList(1));
    }
}
