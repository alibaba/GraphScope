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
package com.alibaba.graphscope.groot.discovery;

import com.alibaba.graphscope.common.RoleType;
import com.alibaba.graphscope.groot.common.config.CommonConfig;
import com.alibaba.graphscope.groot.common.config.Configs;
import com.alibaba.graphscope.compiler.api.exception.GrootException;

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

public class LocalNodeProvider implements Function<Integer, MaxGraphNode> {

    private Configs configs;
    private RoleType roleType;
    private AtomicReference<MaxGraphNode> localNodeRef = new AtomicReference<>();

    public LocalNodeProvider(Configs configs) {
        this(RoleType.fromName(CommonConfig.ROLE_NAME.get(configs)), configs);
    }

    public LocalNodeProvider(RoleType roleType, Configs configs) {
        this.roleType = roleType;
        this.configs = configs;
    }

    @Override
    public MaxGraphNode apply(Integer port) {
        boolean suc =
                localNodeRef.compareAndSet(
                        null, MaxGraphNode.createGraphNode(roleType, configs, port));
        if (!suc) {
            if (!CommonConfig.DISCOVERY_MODE.get(this.configs).equalsIgnoreCase("file")) {
                throw new GrootException("localNode can only be set once");
            }
        }
        return localNodeRef.get();
    }

    public MaxGraphNode get() {
        return localNodeRef.get();
    }
}
