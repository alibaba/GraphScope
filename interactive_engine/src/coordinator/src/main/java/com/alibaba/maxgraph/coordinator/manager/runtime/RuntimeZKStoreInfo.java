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

import com.alibaba.maxgraph.sdkcommon.util.JSON;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * @Author: peaker.lgf
 * @Email: peaker.lgf@alibaba-inc.com
 * @create: 2018-12-11 18:11
 **/
public class RuntimeZKStoreInfo {
    private static final Logger LOG = LoggerFactory.getLogger(RuntimeZKStoreInfo.class);
    private long globalVersion;
    private HashMap<Integer, Long> groupState;  // group_id -> group_version

    public RuntimeZKStoreInfo(long globalVersion, HashMap<Integer, Long> groupState) {
        this.globalVersion = globalVersion;
        this.groupState = groupState;
    }

    public RuntimeZKStoreInfo() {}

    /**
     *
     * @param runtimeZkStoreInfo serialize the RuntimeZKStoreInfo class
     * @return  json string
     */
    public static String toString(RuntimeZKStoreInfo runtimeZkStoreInfo) {
        return JSON.toJson(runtimeZkStoreInfo);
    }

    /**
     *  recover group info from zk
     * @param json read string from zk
     * @return
     */
    public static RuntimeZKStoreInfo fromString(String json) {
        return JSON.fromJson(json, RuntimeZKStoreInfo.class);
    }


    public Map<Integer, Long> getGroupState() {
        return this.groupState;
    }

    public long getGlobalVersion() {
        return this.globalVersion;
    }
}
