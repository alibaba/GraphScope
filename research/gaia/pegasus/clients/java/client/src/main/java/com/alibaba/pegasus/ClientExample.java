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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.logging.Logger;

import com.alibaba.pegasus.builder.JobBuilder;
import com.alibaba.pegasus.intf.ResultProcessor;
import com.alibaba.pegasus.service.proto.PegasusClient.JobConfig;
import com.alibaba.pegasus.service.proto.PegasusClient.JobRequest;
import com.alibaba.pegasus.service.proto.PegasusClient.JobResponse;
import com.google.protobuf.ByteString;
import io.grpc.Status;

public class ClientExample {
    private static final Logger logger = Logger.getLogger(ClientExample.class.getName());

    private static class TestProcessor implements ResultProcessor {

        @Override
        public void process(JobResponse response) {
            ByteString data = response.getData();
            ArrayList<Long> res = toLongArray(data.toByteArray(), data.size());
            System.out.printf("got one response: job id[%d], array size[%d], job data[%s]",
                    response.getJobId(),
                    res.size(),
                    res.toString());
        }

        @Override
        public void finish() {
            System.out.println("finish process");
        }

        @Override
        public void error(Status status) {
            System.out.println("error");
        }
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

        RpcClient rpcClient = new RpcClient("localhost", 1234);
        TestProcessor testProcessor = new TestProcessor();

        logger.info("Will try to send request");
        JobConfig confPb = JobConfig.newBuilder().setJobId(1).setJobName("ping_pong_example").setWorkers(2).build();
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
        rpcClient.submit(req, testProcessor);
        rpcClient.shutdown();
    }
}
