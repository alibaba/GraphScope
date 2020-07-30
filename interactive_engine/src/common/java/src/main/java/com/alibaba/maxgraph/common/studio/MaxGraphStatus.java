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
package com.alibaba.maxgraph.common.studio;

/**
 * @author goohqy
 * @date 2018/7/3.
 */
public interface MaxGraphStatus {

    interface InstanceStatus {
        Integer STARTING = 1;
        Integer RESTARTING = 2;
        Integer STOPING = 3;
        Integer STARTED = 4;
        Integer STOPED = 5;
        Integer FAILED = 6;
    }

    interface InstanceOp {
        Integer START = 1;
        Integer RESTART = 2;
        Integer STOP = 3;
        Integer DROP = 4;
    }

    interface InstanceOpStatus {
        Integer WAITING = 0;
        Integer SUCCESS = 1;
        Integer FAILED = 2;
    }

    interface InstanceOpStep {
        Integer STOPING = 0;
        Integer SUBMIT = 1;
        Integer WAIT_AM_READY = 2;
        Integer WAIT_WORKER_READY = 3;
        Integer WAIT_DATA_READY = 4;
        Integer SERVING = 5;
    }

    interface InstanceStopOpStep {
        Integer STOP_YARN = 0;
        Integer STOPED = 1;
    }

    interface ImportState {
        Integer CREATING = 1;
        Integer SUBMITING = 2;
        Integer BUILDING = 3;
        Integer ONLINEING = 4;
        Integer IMPORTED = 5;
    }

    interface ImportStatus {
        Integer RUNNING = 1;
        Integer FINISH = 2;
        Integer FAILED = 3;
        Integer WAITING = 4;
        Integer UNKNOWN = 5;
    }
}
