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
package com.alibaba.maxgraph.coordinator.manager.runtime;

import com.alibaba.maxgraph.proto.RuntimeHBReq;
import com.google.common.base.MoreObjects;

/**
 * @Author: peaker.lgf
 * @Email: peaker.lgf@alibaba-inc.com
 * @create: 2018-12-11 18:01
 **/
public class NodeInfo {
    String ip;
    int port;
    int storePort;
    int serverId;    // partition id, not the timely_worker id in timely group
    RuntimeHBReq.RuntimeStatus status;

    public NodeInfo(String ip, int port, int storePort, RuntimeHBReq.RuntimeStatus status, int serverId) {
        this.ip = ip;
        this.port = port;
        this.storePort = storePort;
        this.status = status;
        this.serverId = serverId;
    }

    public NodeInfo(String ip, int port, int storePort, RuntimeHBReq.RuntimeStatus status) {
        this.ip = ip;
        this.port = port;
        this.storePort = storePort;
        this.status = status;
    }

    public boolean containsServerId(int serverId) {
        return this.serverId == serverId;
    }

    public void resetInfo() {
        this.ip = "";
        this.port = 0;
        this.storePort = 0;
        this.status = RuntimeHBReq.RuntimeStatus.DOWN;
    }

    public void updateInfo(String ip, int port, int storePort, RuntimeHBReq.RuntimeStatus status) {
        this.ip = ip;
        this.port = port;
        this.storePort = storePort;
        this.status = status;
    }

    public void updateStatus(RuntimeHBReq.RuntimeStatus status) {
        this.status = status;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("ip", ip)
                .add("port", port)
                .add("storePort", storePort)
                .add("serverId", serverId)
                .add("status", status)
                .toString();
    }
}
