package com.alibaba.maxgraph.v2.common.discovery;

import com.alibaba.maxgraph.v2.common.config.CommonConfig;
import com.alibaba.maxgraph.v2.common.config.Configs;
import com.alibaba.maxgraph.v2.common.exception.MaxGraphException;
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
    public MaxGraphNode(@JsonProperty("roleName") String roleName, @JsonProperty("idx") int idx,
                        @JsonProperty("host") String host, @JsonProperty("port") int port) {
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
        return "MaxGraphNode{" +
                "roleName='" + roleName + '\'' +
                ", idx=" + idx +
                ", host='" + host + '\'' +
                ", port=" + port +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MaxGraphNode that = (MaxGraphNode) o;

        if (idx != that.idx) return false;
        if (port != that.port) return false;
        if (roleName != null ? !roleName.equals(that.roleName) : that.roleName != null) return false;
        return host != null ? host.equals(that.host) : that.host == null;
    }
}
