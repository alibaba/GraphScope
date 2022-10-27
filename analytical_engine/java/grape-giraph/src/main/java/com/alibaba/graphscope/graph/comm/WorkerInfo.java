/*
 * Copyright 2022 Alibaba Group Holding Limited.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *   	http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.alibaba.graphscope.graph.comm;

/**
 * Contains information uniquely identify one worker(process) from others.
 */
public class WorkerInfo {

    private int workerId;
    private int workerNum;
    private String dstHostNameOrIp;
    private int initPort;
    private String[] dstHostNameOrIps;

    public WorkerInfo(
            int workerId, int workerNum, String hostNameOrIp, int port, String[] dstHostNameOrIps) {
        this.workerId = workerId;
        this.workerNum = workerNum;
        this.dstHostNameOrIp = hostNameOrIp;
        this.initPort = port;
        this.dstHostNameOrIps = dstHostNameOrIps;
    }

    public int getWorkerId() {
        return workerId;
    }

    public void setWorkerId(int workerId) {
        this.workerId = workerId;
    }

    public int getWorkerNum() {
        return workerNum;
    }

    public void setWorkerNum(int workerNum) {
        this.workerNum = workerNum;
    }

    /**
     * Return the ip/hostname to which the client should connect.
     *
     * @return hostname or ip
     */
    public String getHost() {
        return dstHostNameOrIp;
    }

    /**
     * Return the port client should connect to.
     *
     * @return the port
     */
    public int getInitPort() {
        return initPort;
    }

    public String[] getDstHostNameOrIps() {
        return dstHostNameOrIps;
    }
}
