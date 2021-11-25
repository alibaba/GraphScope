package com.alibaba.graphscope.gaia.plan.strategy.global.property.cache;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

public final class ToFetchProperties {
    private boolean isAll;
    private List<String> properties;

    public ToFetchProperties(boolean isAll, List<String> properties) {
        this.isAll = isAll;
        this.properties = properties;
    }

    public boolean isAll() {
        return isAll;
    }

    public List<String> getProperties() {
        return properties;
    }

    public void dedupProperties() {
        this.properties = new ArrayList(new HashSet(this.properties));
    }
}
