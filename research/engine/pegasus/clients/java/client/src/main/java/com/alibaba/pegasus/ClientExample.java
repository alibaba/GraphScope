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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.alibaba.pegasus.builder.JobBuilder;
import com.alibaba.pegasus.intf.CloseableIterator;
import com.alibaba.pegasus.service.protocol.PegasusClient.JobConfig;
import com.alibaba.pegasus.service.protocol.PegasusClient.JobRequest;
import com.alibaba.pegasus.service.protocol.PegasusClient.JobResponse;
import com.google.protobuf.ByteString;
import io.grpc.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClientExample {
    private static final Logger logger = LoggerFactory.getLogger(ClientExample.class);

    private static void process(JobResponse response) {
        ByteString data = response.getData();
        ArrayList<Long> res = toLongArray(data.toByteArray(), data.size());
        logger.info("got one response: job id {}, array size {}, job data {}",
                response.getJobId(),
                res.size(),
                res.toString());
    }

    private static void finish() {
        logger.info("finish process");
    }

    private static void error(Status status) {
        logger.error("on error {}", status.toString());
    }

    private static ArrayList<Long> toLongArray(byte[] bytes, int size) {
        ArrayList<Long> res = new ArrayList<Long>();
        for (int i = 0; i < size; i = i + 8) {
            long l = fromByteArray(Arrays.copyOfRange(bytes, i, i + 8));
            res.add(l);
        }
        return res;
    }

    private static byte[] toByteArray(long value) {
        byte[] result = new byte[8];

        for (int i = 0; i < 8; ++i) {
            result[i] = (byte) ((int) (value & 255L));
            value >>= 8;
        }

        return result;
    }

    private static long fromByteArray(byte[] bytes) {
        return ((long) bytes[7] & 255L) << 56 |
                ((long) bytes[6] & 255L) << 48 |
                ((long) bytes[5] & 255L) << 40 |
                ((long) bytes[4] & 255L) << 32 |
                ((long) bytes[3] & 255L) << 24 |
                ((long) bytes[2] & 255L) << 16 |
                ((long) bytes[1] & 255L) << 8 |
                (long) bytes[0] & 255L;
    }


    private static ByteString getSeed(long a) {
        return ByteString.copyFrom(toByteArray(a));
    }

    private static ByteString getRoute() {
        return ByteString.EMPTY;
    }

    private static ByteString add(long a) {
        return ByteString.copyFrom(toByteArray(a));
    }

    private static ByteString copy(long a) {
        return ByteString.copyFrom(toByteArray(a));
    }

    private static ByteString getSink() {
        return ByteString.EMPTY;
    }

    public static void main(String[] args) throws Exception {

        RpcChannel rpcChannel0 = new RpcChannel("localhost", 1234);
        RpcChannel rpcChannel1 = new RpcChannel("localhost", 1235);
        List<RpcChannel> channels = new ArrayList<>();
        channels.add(rpcChannel0);
        channels.add(rpcChannel1);
        RpcClient rpcClient = new RpcClient(channels);

        logger.info("Will try to send request");
        JobConfig confPb = JobConfig
                .newBuilder()
                .setJobId(2)
                .setJobName("ping_pong_example")
                .setWorkers(2)
                .addServers(0)
                .addServers(1)
                .build();
        // for job build
        JobBuilder jobBuilder = new JobBuilder(confPb);
        // for nested task
        JobBuilder start = new JobBuilder();

        // construct job
        jobBuilder.addSource(getSeed(0))
                .repeat(3,
                        start.exchange(getRoute())
                                .map(add(1))
                                .flatMap(copy(8))
                )
                .sink(getSink());

        JobRequest req = jobBuilder.build();
        CloseableIterator<JobResponse> iterator = rpcClient.submit(req);
        // process response
        try {
            while (iterator.hasNext()) {
                JobResponse response = iterator.next();
                process(response);
            }
        } catch (Exception e) {
            if (iterator != null) {
                try {
                    iterator.close();
                } catch (IOException ioe) {
                    // Ignore
                }
            }
            error(Status.fromThrowable(e));
            throw e;
        }
        finish();

        rpcClient.shutdown();
    }
}
