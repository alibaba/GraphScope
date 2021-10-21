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

import com.alibaba.maxgraph.common.DataStatus;
import com.alibaba.maxgraph.proto.ServerHBResp;

import java.util.Map;

/**
 * @Author: peaker.lgf
 * @Date: 2019-12-24 16:09
 **/
public interface RuntimeManager {
    void init() throws Exception;

    void close();

    /// build runtime response to executor
    void initRuntimeResp(ServerHBResp.Builder builder, DataStatus runtimeStatus);

    /// Get status of each group
    Map<Integer, GroupStatus> getGroupsStatus();
}
