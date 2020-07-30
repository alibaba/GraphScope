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

/**
 * @Author: peaker.lgf
 * @Email: peaker.lgf@alibaba-inc.com
 * @create: 2018-12-11 18:04
 **/
public enum GroupStatus {
    /**
     *  Group is starting
     */
    STARTING,

    /**
     * Group has all server's ip and port, and all server is STARTING status
     */
    READY,

    /**
     *  all server is running
     */
    RUNNING,

    /**
     *  Group info is recovered from zk
     */
    RESTORE,
}
