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
package com.alibaba.maxgraph.logging;

/**
 * @author xiafei.qiuxf
 * @date 2018-12-12
 */
public class LogEvents {

    public enum ScheduleEvent {
        // am recover info form zk
        AM_RECOVER,
        // rm start appmaster
        AM_START,
        // am exit with exception or killed by rm
        AM_EXIT,
        // am stop worker before restart worker
        AM_KILL_WORKER,
        // am let illegal worker suicide
        AM_KILL_ILLEGAL_WORKER,
        // resource allocated for some worker
        WORKER_RESOURCE_ACQUIRED,
        // start command sent to nodemanager success
        WORKER_LAUNCHED,
        // heartbeat timeout
        WORKER_HB_TIMEOUT,
        // long time not register
        WORKER_REG_TIMEOUT,
        // worker container exist by self or killed by nodemanager
        WORKER_CONTAINER_EXIT,
        // worker status changed from lost/restarting to running
        WORKER_RESUME_RUNNING,
        // worker original host not available
        WORKER_ORIGINAL_RESOURCE_UNAVAILABLE,
        // no available resource
        NO_AVAILABLE_RESOURCE_FOR_WORKER,
        // send resource request to rm
        SEND_RESOURCE_REQUEST_FOR_WORKER,
        // need update resource
        NEED_UPDATE_WORKER_RESOURCE,
        // reuse original resource
        WORKER_RECOVER_BY_LOCAL_RESOURCE,
        // waiting for physical resource timeout
        CONTAINER_REQUEST_WAITING_TIMEOUT
    }

    public enum StoreEventType {
        JOB,
        DDL,
        SNAPSHOT
    }

    public enum StoreEvent {
        DDL_START(StoreEventType.DDL),
        DDL_SUCCESS(StoreEventType.DDL),
        DDL_ERROR(StoreEventType.DDL),
        JOB_REQUEST(StoreEventType.JOB),
        JOB_ONLINE(StoreEventType.JOB),
        JOB_ERROR(StoreEventType.JOB),
        JOB_CANCEL(StoreEventType.JOB),
        JOB_SERVING(StoreEventType.JOB),
        SNAPSHOT_COOR_ONLINE(StoreEventType.SNAPSHOT),
        SNAPSHOT_COOR_SERVING(StoreEventType.SNAPSHOT),
        SNAPSHOT_COOR_OFFLINE(StoreEventType.SNAPSHOT),
        SNAPSHOT_COOR_DATA_NORMAL(StoreEventType.SNAPSHOT),
        SNAPSHOT_COOR_DATA_ABNORMAL(StoreEventType.SNAPSHOT),
        SNAPSHOT_STORE_ONLINE(StoreEventType.SNAPSHOT),
        SNAPSHOT_STORE_SERVING(StoreEventType.SNAPSHOT),
        SNAPSHOT_STORE_OFFLINE(StoreEventType.SNAPSHOT);

        StoreEventType type;

        StoreEvent(StoreEventType type) {
            this.type = type;
        }
    }

    public enum QueryType {
        EXECUTE,
        PREPARE,
        QUERY,
    }

    public enum QueryEvent {
        FRONT_RECEIVED,
        PLAN_GENERATED,
        EXECUTOR_RECEIVED,
        EXECUTOR_FINISH,
        FRONT_FINISH,
    }

    public enum RuntimeEvent {
        GROUP_RESTORE,
        GROUP_STARTING,
        GROUP_READY,
        GROUP_RUNNING,
        SERVER_STARTING,
        SERVER_RUNNING,
        SERVER_DOWN,
    }

}
