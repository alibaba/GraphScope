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

import com.alibaba.maxgraph.common.DataStatus;
import com.alibaba.maxgraph.sdkcommon.client.Endpoint;
import com.alibaba.maxgraph.sdkcommon.Protoable;
import com.alibaba.maxgraph.proto.WorkerInfoProto;
import com.alibaba.maxgraph.proto.WorkerStatus;
import com.alibaba.maxgraph.proto.RoleType;
import com.google.common.base.MoreObjects;
import com.google.protobuf.InvalidProtocolBufferException;

import java.util.Objects;

public class WorkerInfo implements Protoable<WorkerInfoProto>, Comparable<WorkerInfo> {
    public final int id;
    public final Endpoint endpoint;
    public final RoleType roleType;
    public final WorkerStatus workerStatus;
    public final Long lastReportTime;
    public final String logDir;
    public DataStatus dataStatus;

    public WorkerInfo(DataStatus dataStatus) {
        this(dataStatus.serverId, dataStatus.endpoint, RoleType.EXECUTOR, WorkerStatus.RUNNING, dataStatus, dataStatus.logDir, dataStatus.reportTimeStamp);
    }

    public WorkerInfo(DataStatus dataStatus, WorkerStatus workerStatus) {
        this(dataStatus.serverId, dataStatus.endpoint, RoleType.EXECUTOR, workerStatus, dataStatus, dataStatus.logDir, dataStatus.reportTimeStamp);
    }

    public WorkerInfo(int id, Endpoint endpoint, RoleType roleType, WorkerStatus workerStatus, String logDir, Long lastReportTime) {
        this(id, endpoint, roleType, workerStatus, null, logDir, lastReportTime);
    }

    public WorkerInfo(int id, Endpoint endpoint, RoleType roleType, WorkerStatus workerStatus, DataStatus dataStatus, String logDir, Long lastReportTime) {
        this.id = id;
        this.endpoint = endpoint;
        this.roleType = roleType;
        this.workerStatus = workerStatus;
        this.dataStatus = dataStatus;
        this.logDir = logDir;
        this.lastReportTime = lastReportTime;
    }

    public static WorkerInfo fromProto(WorkerInfoProto workerInfoProto) {
        return new WorkerInfo(workerInfoProto.getId(),
                Endpoint.fromProto(workerInfoProto.getAddress()),
                workerInfoProto.getRoleType(),
                workerInfoProto.getWorkerStatus(),
                DataStatus.fromProto(workerInfoProto.getServerHBReq()), workerInfoProto.getLogDir(), workerInfoProto.getLastReportTime());
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("id", id)
                .add("endpoint", endpoint)
                .add("roleType", roleType)
                .add("workerStatus", workerStatus)
                .add("dataStatus", dataStatus)
                .toString();
    }

    @Override
    public void fromProto(byte[] data) throws InvalidProtocolBufferException {
        throw new UnsupportedOperationException();
    }

    @Override
    public WorkerInfoProto toProto() {
        WorkerInfoProto.Builder builder = WorkerInfoProto.newBuilder();
        builder.setId(id);
        builder.setAddress(endpoint.toProto());
        builder.setRoleType(roleType);
        builder.setWorkerStatus(workerStatus);
        builder.setLogDir(logDir);
        builder.setLastReportTime(lastReportTime);
        builder.setWorkerStatus(workerStatus);

        if (dataStatus != null) {
            builder.setServerHBReq(dataStatus.toProto());
        }

        return builder.build();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof WorkerInfo)) return false;
        WorkerInfo that = (WorkerInfo) o;
        return id == that.id &&
                roleType == that.roleType;
    }

    @Override
    public int hashCode() {

        return Objects.hash(id, roleType);
    }

    @Override
    public int compareTo(WorkerInfo o) {
        if (id > o.id) {
            return 1;
        } else if (id < o.id) {
            return -1;
        }

        return 0;
    }
}
