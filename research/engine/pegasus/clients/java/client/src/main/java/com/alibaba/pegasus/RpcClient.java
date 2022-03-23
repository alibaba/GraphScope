/**
 * Copyright 2020 Alibaba Group Holding Limited.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.pegasus;

import com.alibaba.pegasus.common.StreamIterator;
import com.alibaba.pegasus.intf.CloseableIterator;
import com.alibaba.pegasus.service.protocol.JobServiceGrpc;
import com.alibaba.pegasus.service.protocol.JobServiceGrpc.JobServiceStub;
import com.alibaba.pegasus.service.protocol.PegasusClient.JobResponse;
import com.alibaba.pegasus.service.protocol.PegasusClient.JobRequest;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RpcClient {
    private static final Logger logger = LoggerFactory.getLogger(RpcClient.class);

    private List<RpcChannel> channels;

    public RpcClient(List<RpcChannel> channels) {
        this.channels = channels;
    }

    public CloseableIterator<JobResponse> submit(JobRequest jobRequest) throws InterruptedException {
        StreamIterator<JobResponse> responseIterator = new StreamIterator<>();
        AtomicInteger counter = new AtomicInteger(this.channels.size());
        AtomicBoolean finished = new AtomicBoolean(false);
        for (RpcChannel rpcChannel : channels) {
            JobServiceStub asyncStub = JobServiceGrpc.newStub(rpcChannel.getChannel());
            // todo: make timeout configurable
            asyncStub.withDeadlineAfter(600000, TimeUnit.MILLISECONDS).submit(jobRequest, new JobResponseObserver(responseIterator, finished, counter));
        }
        return responseIterator;
    }

    public void shutdown() throws InterruptedException {
        for (RpcChannel rpcChannel : channels) {
            rpcChannel.shutdown();
        }
    }

    private static class JobResponseObserver implements StreamObserver<JobResponse> {
        private final StreamIterator<JobResponse> iterator;
        private final AtomicBoolean finished;
        private final AtomicInteger counter;

        public JobResponseObserver(StreamIterator<JobResponse> iterator, AtomicBoolean finished, AtomicInteger counter) {
            this.iterator = iterator;
            this.finished = finished;
            this.counter = counter;
        }

        @Override
        public void onNext(JobResponse jobResponse) {
            if (finished.get()) {
                return;
            }
            try {
                this.iterator.putData(jobResponse);
            } catch (InterruptedException e) {
                onError(e);
            }
        }

        @Override
        public void onError(Throwable throwable) {
            if (finished.getAndSet(true)) {
                return;
            }
            Status status = Status.fromThrowable(throwable);
            logger.error("get job response error: {}", status);
            this.iterator.fail(throwable);
        }

        @Override
        public void onCompleted() {
            logger.info("finish get job response from one server");
            if (counter.decrementAndGet() == 0) {
                logger.info("finish get job response from all servers");
                try {
                   this.iterator.finish();
                } catch (InterruptedException e) {
                    onError(e);
                }
            }
        }
    }
}
