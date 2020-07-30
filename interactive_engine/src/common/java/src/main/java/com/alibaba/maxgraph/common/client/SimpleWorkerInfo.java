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
package com.alibaba.maxgraph.common.client;

import com.alibaba.maxgraph.sdkcommon.client.Endpoint;
import com.alibaba.maxgraph.proto.RoleType;
import com.alibaba.maxgraph.proto.WorkerStatus;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.google.common.base.MoreObjects;

public class SimpleWorkerInfo {
    public final int id;
    public final Endpoint endpoint;
    public final RoleType roleType;
    public final WorkerStatus workerStatus;
    public final String lastReportTime;
    public final String logDir;

    @JsonCreator
    public SimpleWorkerInfo(int id, Endpoint endpoint, RoleType roleType, WorkerStatus workerStatus, String lastReportTime, String logDir) {
        this.id = id;
        this.endpoint = endpoint;
        this.roleType = roleType;
        this.workerStatus = workerStatus;
        this.lastReportTime = lastReportTime;
        this.logDir = logDir;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("id", id)
                .add("endpoint", endpoint)
                .add("roleType", roleType)
                .add("workerStatus", workerStatus)
                .add("lastReportTime", lastReportTime)
                .add("logDir", logDir)
                .toString();
    }
}
