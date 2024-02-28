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
package com.alibaba.graphscope.groot.frontend;

import com.alibaba.graphscope.groot.frontend.write.IngestCallback;
import com.alibaba.graphscope.groot.frontend.write.KafkaAppender;
import com.alibaba.graphscope.groot.operation.OperationBatch;
import com.alibaba.graphscope.proto.groot.*;

import io.grpc.stub.StreamObserver;

public class IngestorWriteService extends IngestorWriteGrpc.IngestorWriteImplBase {

    private final KafkaAppender kafkaAppender;

    public IngestorWriteService(KafkaAppender kafkaAppender) {
        this.kafkaAppender = kafkaAppender;
    }

    @Override
    public void writeIngestor(
            WriteIngestorRequest request, StreamObserver<WriteIngestorResponse> responseObserver) {
        try {
            String requestId = request.getRequestId();
            OperationBatch operationBatch = OperationBatch.parseProto(request.getOperationBatch());
            this.kafkaAppender.ingestBatch(
                    requestId,
                    operationBatch,
                    new IngestCallback() {
                        @Override
                        public void onSuccess(long snapshotId) {
                            WriteIngestorResponse response =
                                    WriteIngestorResponse.newBuilder()
                                            .setSnapshotId(snapshotId)
                                            .build();
                            responseObserver.onNext(response);
                            responseObserver.onCompleted();
                        }

                        @Override
                        public void onFailure(Exception e) {
                            responseObserver.onError(e);
                        }
                    });
        } catch (Exception e) {
            responseObserver.onError(e);
        }
    }
}
