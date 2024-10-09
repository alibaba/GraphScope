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

import com.alibaba.graphscope.groot.CuratorUtils;
import com.alibaba.graphscope.groot.common.config.CommonConfig;
import com.alibaba.graphscope.groot.common.config.Configs;
import com.alibaba.graphscope.groot.common.exception.InvalidArgumentException;

import org.apache.curator.framework.CuratorFramework;

public class DiscoveryFactory {

    private final Configs configs;

    public DiscoveryFactory(Configs configs) {
        this.configs = configs;
    }

    private CuratorFramework curator = null;

    private CuratorFramework getCurator() {
        if (this.curator == null) {
            this.curator = CuratorUtils.makeCurator(this.configs);
            this.curator.start();
        }
        return this.curator;
    }

    public NodeDiscovery makeDiscovery(LocalNodeProvider localNodeProvider) {
        String discoveryMode = CommonConfig.DISCOVERY_MODE.get(this.configs).toUpperCase();
        switch (discoveryMode) {
            case "FILE":
                return new FileDiscovery(this.configs);
            case "ZOOKEEPER":
                return new ZkDiscovery(this.configs, localNodeProvider, getCurator());
            default:
                throw new InvalidArgumentException(
                        "invalid discovery mode [" + discoveryMode + "]");
        }
    }
}
