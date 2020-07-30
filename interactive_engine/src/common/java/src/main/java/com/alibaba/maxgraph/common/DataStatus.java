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
package com.alibaba.maxgraph.common;

import com.alibaba.maxgraph.proto.*;

import com.alibaba.maxgraph.sdkcommon.client.Endpoint;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * @author lvshuang.xjs@alibaba-inc.com
 * @create 2018-06-07 下午4:05
 **/

public class DataStatus {

    private static final Logger LOG = LoggerFactory.getLogger(DataStatus.class);
    public final int serverId;
    public final Endpoint endpoint;
    // TODO: modify logDir final and set in heartbeat
    public String logDir = "";
    public long reportTimeStamp;
    private StoreStatus storeStatus;

    @JsonIgnore
    public RuntimeHBReq runtimeHBReq;

    public StoreStatus getStoreStatus() {
        return storeStatus;
    }

    @JsonCreator
    public DataStatus(int serverId, Endpoint endpoint, StoreStatus storeStatus) {
        this.serverId = serverId;
        this.endpoint = endpoint;
        this.reportTimeStamp = System.currentTimeMillis();
        this.storeStatus = storeStatus;
    }

    public DataStatus(int serverId, Endpoint endpoint, StoreStatus storeStatus,
                      RuntimeHBReq runtimeReq) {
        this(serverId, endpoint, storeStatus);
        this.runtimeHBReq = runtimeReq;
    }

    @JsonIgnore
    public RuntimeHBReq getRuntimeHBReq() {
        return runtimeHBReq;
    }

    @Override
    public String toString() {
        return "DataStatus{" +
                "serverId=" + serverId +
                ", endpoint=" + endpoint +
                ", reportTimeStamp=" + reportTimeStamp +
                ", storeStatus" + storeStatus.name() +
                ", runtimeReq=" + runtimeHBReq +
                '}';
    }

    public static DataStatus fromProto(ServerHBReq serverHBReq) {
        int serverId = serverHBReq.getId();
        Endpoint endpoint = Endpoint.fromProto(serverHBReq.getEndpoint());
        RuntimeHBReq runtimeHBReq = serverHBReq.getRuntimeReq();
        StoreStatus storeStatus = serverHBReq.getStatus();

        return new DataStatus(serverId, endpoint, storeStatus, runtimeHBReq);
    }

    public ServerHBReq toProto() {
        ServerHBReq.Builder builder = ServerHBReq.newBuilder();
        builder.setId(serverId);
        builder.setEndpoint(endpoint.toProto());
        builder.setRuntimeReq(runtimeHBReq);
        builder.setStatus(storeStatus);

        return builder.build();
    }

}
