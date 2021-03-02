/**
 * Copyright 2020 Alibaba Group Holding Limited.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.pegasus;

import com.alibaba.pegasus.builder.Plan;
import com.alibaba.pegasus.intf.ResultProcessor;
import com.alibaba.pegasus.service.proto.JobServiceGrpc;
import com.alibaba.pegasus.service.proto.JobServiceGrpc.JobServiceStub;
import com.alibaba.pegasus.service.proto.PegasusClient.JobResponse;
import com.alibaba.pegasus.service.proto.PegasusClient.JobRequest;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public class RpcClient {
    private static final Logger logger = Logger.getLogger(Plan.class.getName());

    private final JobServiceStub asyncStub;

    private final ManagedChannel channel;

    public RpcClient(String host, int port) {
        this(ManagedChannelBuilder.forAddress(host, port).usePlaintext());
    }

    public RpcClient(ManagedChannelBuilder<?> channelBuilder) {
        channel = channelBuilder.build();
        this.asyncStub = JobServiceGrpc.newStub(channel);
    }

    public void submit(JobRequest jobRequest, ResultProcessor resultProcessor) throws InterruptedException {
        CountDownLatch finishLatch = new CountDownLatch(1);
        asyncStub.submit(jobRequest, new JobResponseObserver(resultProcessor, finishLatch));
        if (!finishLatch.await(1, TimeUnit.MINUTES)) {
            String errorMessage = "job cannot finish in 1 minutes";
            logger.log(Level.WARNING, errorMessage);
            throw new RuntimeException(errorMessage);
        }
    }

    public void shutdown() throws InterruptedException {
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }

    private static class JobResponseObserver implements StreamObserver<JobResponse> {
        private final ResultProcessor resultProcessor;
        private CountDownLatch finishLatch;

        public JobResponseObserver(ResultProcessor resultProcessor, CountDownLatch finishLatch) {
            this.resultProcessor = resultProcessor;
            this.finishLatch = finishLatch;
        }

        @Override
        public void onNext(JobResponse jobResponse) {
            if (jobResponse.hasErr()) {
                logger.info("job failed: " + jobResponse.getErr().getErrMsg());
                return;
            }
            resultProcessor.process(jobResponse);
        }

        @Override
        public void onError(Throwable throwable) {
            Status status = Status.fromThrowable(throwable);
            resultProcessor.error(status);
            logger.log(Level.WARNING, "process job response error: {0}", status);
            finishLatch.countDown();
        }

        @Override
        public void onCompleted() {
            resultProcessor.finish();
            logger.info("finish process job response");
            finishLatch.countDown();
        }
    }
}
