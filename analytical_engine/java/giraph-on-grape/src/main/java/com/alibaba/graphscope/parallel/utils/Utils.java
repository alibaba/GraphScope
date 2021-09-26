/*
 * Copyright 2021 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.graphscope.parallel.utils;

import com.alibaba.graphscope.communication.FFICommunicator;
import com.alibaba.graphscope.serialization.FFIByteVectorInputStream;
import com.alibaba.graphscope.serialization.FFIByteVectorOutputStream;
import com.alibaba.graphscope.stdcxx.FFIByteVector;
import com.alibaba.graphscope.stdcxx.FFIByteVectorFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Objects;

public class Utils {

    private static Logger logger = LoggerFactory.getLogger(Utils.class);

    public static String[] getAllHostNames(
            int workerId, int workerNum, FFICommunicator communicator) {
        if (Objects.isNull(communicator) || communicator.getAddress() <= 0) {
            throw new IllegalStateException("Invalid communicator");
        }
        String selfIp = getHostIp();
        if (workerId == 0) {
            return actAsCoordinator(selfIp, workerId, workerNum, communicator);
        } else {
            return actAsWorker(selfIp, workerId, workerNum, communicator);
        }
    }

    private static String[] actAsCoordinator(
            String selfIp, int workerId, int workerNum, FFICommunicator communicator) {
        String[] res = new String[workerNum];
        res[0] = selfIp;
        FFIByteVector vec = (FFIByteVector) FFIByteVectorFactory.INSTANCE.create();
        FFIByteVectorInputStream inputStream = new FFIByteVectorInputStream();
        FFIByteVectorOutputStream outputStream = new FFIByteVectorOutputStream();
        outputStream.writeLong(0, 0);
        for (int srcWorkerId = 1; srcWorkerId < workerNum; ++srcWorkerId) {
            info(workerId, "receiving msg from " + srcWorkerId);
            communicator.receiveFrom(srcWorkerId, vec);
            inputStream.digestVector(vec);
            info(
                    workerId,
                    "received msg from "
                            + srcWorkerId
                            + " data size: "
                            + vec.size()
                            + " digested: "
                            + inputStream.longAvailable());
        }

        try {
            outputStream.writeUTF(res[0]);
            for (int i = 1; i < workerNum; ++i) {
                if (inputStream.longAvailable() <= 0) {
                    throw new IllegalStateException("reach bottom when try to read msg from " + i);
                }
                res[i] = inputStream.readUTF();
                outputStream.writeUTF(res[i]);
                info(workerId, "from worker: " + i + ": " + res[i]);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        outputStream.writeLong(0, outputStream.bytesWriten() - 8);
        outputStream.finishSetting();

        // Distribute to others;
        for (int dstWorkerId = 1; dstWorkerId < workerNum; ++dstWorkerId) {
            FFIByteVector tempVec = (FFIByteVector) FFIByteVectorFactory.INSTANCE.create();
            tempVec.appendVector(0, outputStream.getVector());
            info(workerId, " sending to worker: [" + dstWorkerId + "] " + tempVec.size());
            communicator.sendTo(dstWorkerId, tempVec);
            info(workerId, " Successfully send to worker: [" + dstWorkerId + "] " + tempVec.size());
        }
        return res;
    }

    private static String[] actAsWorker(
            String selfIp, int workerId, int workerNum, FFICommunicator communicator) {
        FFIByteVectorOutputStream outputStream = new FFIByteVectorOutputStream();
        outputStream.writeLong(0, 0);
        try {
            outputStream.writeUTF(selfIp);
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
        outputStream.writeLong(0, outputStream.bytesWriten() - 8);
        outputStream.finishSetting();
        info(workerId, "now send to coordinator: " + selfIp);
        communicator.sendTo(0, outputStream.getVector());
        info(workerId, "finish sending " + selfIp);

        // Now receive from coordinator
        FFIByteVectorInputStream inputStream = new FFIByteVectorInputStream();
        info(workerId, "waiting msg arriving");
        FFIByteVector vec = (FFIByteVector) FFIByteVectorFactory.INSTANCE.create();
        communicator.receiveFrom(0, vec);
        info(workerId, "msg arrived:" + vec.size());
        // Let readable limit be updated.
        inputStream.digestVector(vec);
        info(
                workerId,
                "Received msg: "
                        + inputStream.longAvailable()
                        + ", "
                        + inputStream.getVector().size());
        // Expected workerNum string
        String[] res = new String[workerNum];
        try {
            for (int i = 0; i < workerNum; ++i) {
                if (inputStream.longAvailable() <= 0) {
                    error(
                            workerId,
                            "Reaching bottom of input stream when trying to read" + i + "th data");
                    return res;
                }
                res[i] = inputStream.readUTF();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return res;
    }

    private static void info(int workerId, String msg) {
        logger.info("Worker [" + workerId + "] " + msg);
    }

    private static void error(int workerId, String msg) {
        logger.info("Worker [" + workerId + "] " + msg);
    }

    private static String getHostIp() {
        try {
            return InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            e.printStackTrace();
            throw new IllegalStateException("Failed to get master host address");
        }
    }
}
