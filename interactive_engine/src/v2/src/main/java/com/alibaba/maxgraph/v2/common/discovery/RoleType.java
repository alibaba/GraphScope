package com.alibaba.maxgraph.v2.common.discovery;

import java.util.HashMap;
import java.util.Map;

public enum RoleType {

    UNKNOWN("unknown"),
    FRONTEND("frontend"),
    INGESTOR("ingestor"),
    STORE("store"),
    COORDINATOR("coordinator"),
    EXECUTOR_GRAPH("executor_graph"),
    EXECUTOR_QUERY("executor_query"),
    EXECUTOR_MANAGE("executor_manage"),
    EXECUTOR_ENGINE("executor_engine");

    private final String name;

    RoleType(String name) {
        this.name = name;
    }

    public String getName() {
        return this.name;
    }

    private static final Map<String, RoleType> lookup = new HashMap<>();

    static {
        for (RoleType role : RoleType.values()) {
            lookup.put(role.getName(), role);
        }
    }

    public static RoleType fromName(String roleName) {
        RoleType roleType = lookup.get(roleName);
        if (roleType == null || roleType == RoleType.UNKNOWN) {
            throw new IllegalArgumentException("Unknown RoleType: [" + roleName + "]");
        }
        return roleType;
    }
}
