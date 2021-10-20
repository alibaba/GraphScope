/**
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.maxgraph.servers;

import com.alibaba.maxgraph.common.config.CommonConfig;
import com.alibaba.maxgraph.common.config.Configs;
import com.alibaba.maxgraph.common.RoleType;

import java.io.Closeable;

public abstract class NodeBase implements Closeable {

    protected RoleType roleType;
    protected int idx;

    public NodeBase() {
        this.roleType = RoleType.UNKNOWN;
        this.idx = 0;
    }

    public NodeBase(Configs configs) {
        this.roleType = RoleType.fromName(CommonConfig.ROLE_NAME.get(configs));
        this.idx = CommonConfig.NODE_IDX.get(configs);
    }

    public NodeBase(Configs configs, RoleType roleType) {
        this.roleType = roleType;
        this.idx = CommonConfig.NODE_IDX.get(configs);
    }

    protected Configs reConfig(Configs configs) {
        int storeCount = CommonConfig.STORE_NODE_COUNT.get(configs);
        int ingestorCount = CommonConfig.INGESTOR_NODE_COUNT.get(configs);
        return Configs.newBuilder(configs)
                .put(
                        String.format(
                                CommonConfig.NODE_COUNT_FORMAT, RoleType.EXECUTOR_ENGINE.getName()),
                        String.valueOf(storeCount))
                .put(
                        String.format(
                                CommonConfig.NODE_COUNT_FORMAT, RoleType.EXECUTOR_GRAPH.getName()),
                        String.valueOf(storeCount))
                .put(
                        String.format(
                                CommonConfig.NODE_COUNT_FORMAT, RoleType.EXECUTOR_MANAGE.getName()),
                        String.valueOf(storeCount))
                .put(
                        String.format(
                                CommonConfig.NODE_COUNT_FORMAT, RoleType.EXECUTOR_QUERY.getName()),
                        String.valueOf(storeCount))
                .put(
                        String.format(CommonConfig.NODE_COUNT_FORMAT, RoleType.GAIA_RPC.getName()),
                        String.valueOf(storeCount))
                .put(
                        String.format(
                                CommonConfig.NODE_COUNT_FORMAT, RoleType.GAIA_ENGINE.getName()),
                        String.valueOf(storeCount))
                .put(CommonConfig.INGESTOR_QUEUE_COUNT.getKey(), String.valueOf(ingestorCount))
                .put(CommonConfig.ROLE_NAME.getKey(), this.roleType.getName())
                .build();
    }

    public abstract void start();

    public String getName() {
        return roleType + "#" + idx;
    }
}
