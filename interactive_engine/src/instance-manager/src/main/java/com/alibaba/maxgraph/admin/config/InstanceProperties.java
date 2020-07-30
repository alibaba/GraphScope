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
package com.alibaba.maxgraph.admin.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(
        prefix = "instance"
)
public class InstanceProperties {
    private String createScript;
    private String closeScript;
    private ZookeeperProperties zookeeper;

    public void setCreateScript(String createScript) {
        this.createScript = createScript;
    }

    public void setCloseScript(String closeScript) {
        this.closeScript = closeScript;
    }

    public void setZookeeper(ZookeeperProperties zookeeper) {
        this.zookeeper = zookeeper;
    }

    public String getCreateScript() {
        return createScript;
    }

    public String getCloseScript() {
        return closeScript;
    }

    public ZookeeperProperties getZookeeper() {
        return zookeeper;
    }

    public static class ZookeeperProperties {
        private String hosts;
        private String sessionTimeoutMill;
        private String connectTimeoutMill;
        private String namespace;

        public void setHosts(String hosts) {
            this.hosts = hosts;
        }

        public void setSessionTimeoutMill(String sessionTimeoutMill) {
            this.sessionTimeoutMill = sessionTimeoutMill;
        }

        public void setConnectTimeoutMill(String connectTimeoutMill) {
            this.connectTimeoutMill = connectTimeoutMill;
        }

        public void setNamespace(String namespace) {
            this.namespace = namespace;
        }

        public String getHosts() {
            return hosts;
        }

        public int getSessionTimeoutMill() {
            return sessionTimeoutMill == null ? 5000 : Integer.parseInt(sessionTimeoutMill);
        }

        public int getConnectTimeoutMill() {
            return connectTimeoutMill == null ? 5000 : Integer.parseInt(connectTimeoutMill);
        }

        public String getNamespace() {
            return this.namespace;
        }
    }
}
