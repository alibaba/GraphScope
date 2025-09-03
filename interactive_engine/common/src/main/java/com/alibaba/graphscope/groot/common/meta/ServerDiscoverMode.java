package com.alibaba.graphscope.groot.common.meta;

public enum ServerDiscoverMode {

    // use k8s dns as service discover
    SERVICE("service"),

    FILE("file");

    private String mode;

    ServerDiscoverMode(String mode) {
        this.mode = mode;
    }

    public String getMode() {
        return mode;
    }

    public static ServerDiscoverMode fromMode(String modeStr) {
        for (ServerDiscoverMode mode : ServerDiscoverMode.values()) {
            if (mode.getMode().equals(modeStr)) {
                return mode;
            }
        }
        return null;
    }

}
