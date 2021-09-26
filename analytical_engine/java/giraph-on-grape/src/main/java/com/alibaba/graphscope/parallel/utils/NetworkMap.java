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

/**
 * Contains global worker's host and port info.
 */
public class NetworkMap {

    private int workerNum;
    private int workerId;

    /**
     * length should be workerNum - 1
     */
    private String[] allIpOrHostNames;

    private int[] allPorts;

    public NetworkMap(int workerId, int workerNum, int initPort, String[] ipOrHostNames) {
        this.workerId = workerId;
        this.workerNum = workerNum;
        this.allIpOrHostNames = ipOrHostNames;
        this.allPorts = new int[workerNum];
        for (int i = 0; i < workerNum; ++i) {
            allPorts[i] = initPort + i;
        }
    }

    public int getWorkerNum() {
        return workerNum;
    }

    public int getSelfWorkerId() {
        return workerId;
    }

    public int getSelfPort() {
        return allPorts[workerId];
    }

    public String getSelfHostNameOrIp() {
        return allIpOrHostNames[workerId];
    }

    public String getAddress() {
        return getSelfHostNameOrIp() + ":" + getSelfPort();
    }

    public String getHostNameForWorker(int dstWorkerId) {
        if (dstWorkerId >= workerNum) {
            throw new IllegalArgumentException(
                    "Expected worker id less than: " + workerNum + " but received: " + dstWorkerId);
        }
        return allIpOrHostNames[dstWorkerId];
    }

    public int getPortForWorker(int dstWorkerId) {
        if (dstWorkerId >= workerNum) {
            throw new IllegalArgumentException(
                    "Expected worker id less than: " + workerNum + " but received: " + dstWorkerId);
        }
        return allPorts[dstWorkerId];
    }
}
