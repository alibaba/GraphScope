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

import com.alibaba.maxgraph.common.RoleType;
import com.alibaba.maxgraph.common.config.CommonConfig;
import com.alibaba.maxgraph.common.config.Configs;
import com.alibaba.maxgraph.compiler.api.exception.MaxGraphException;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonRootName;

import java.net.InetAddress;
import java.net.UnknownHostException;

@JsonRootName("details")
public class MaxGraphNode {

    private String roleName;
    private int idx;
    private String host;
    private int port;

    @JsonCreator
    public MaxGraphNode(
            @JsonProperty("roleName") String roleName,
            @JsonProperty("idx") int idx,
            @JsonProperty("host") String host,
            @JsonProperty("port") int port) {
        this.roleName = roleName;
        this.idx = idx;
        this.host = host;
        this.port = port;
    }

    public static MaxGraphNode createLocalNode(Configs configs, int port) {
        RoleType role = RoleType.fromName(CommonConfig.ROLE_NAME.get(configs));
        return createGraphNode(role, configs, port);
    }

    public static MaxGraphNode createGraphNode(RoleType role, Configs configs, int port) {
        int idx = CommonConfig.NODE_IDX.get(configs);
        String host = CommonConfig.RPC_HOST.get(configs);
        if (host.isEmpty()) {
            try {
                host = InetAddress.getLocalHost().getHostAddress();
            } catch (UnknownHostException e) {
                throw new MaxGraphException(e);
            }
        }
        return new MaxGraphNode(role.getName(), idx, host, port);
    }

    @JsonIgnore
    public String getRoleIdx() {
        return roleName + "_" + idx;
    }

    public String getRoleName() {
        return roleName;
    }

    public int getIdx() {
        return idx;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    @Override
    public String toString() {
        return "MaxGraphNode{"
                + "roleName='"
                + roleName
                + '\''
                + ", idx="
                + idx
                + ", host='"
                + host
                + '\''
                + ", port="
                + port
                + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MaxGraphNode that = (MaxGraphNode) o;

        if (idx != that.idx) return false;
        if (port != that.port) return false;
        if (roleName != null ? !roleName.equals(that.roleName) : that.roleName != null)
            return false;
        return host != null ? host.equals(that.host) : that.host == null;
    }
}
