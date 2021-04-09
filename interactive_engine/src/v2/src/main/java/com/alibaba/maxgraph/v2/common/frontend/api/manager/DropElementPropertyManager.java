package com.alibaba.maxgraph.v2.common.frontend.api.manager;

import java.util.List;

public interface DropElementPropertyManager<M extends DropElementPropertyManager> {
    /**
     * Drop property with given property name in the type
     *
     * @param propertyName The given property name
     * @return The schema type manager
     */
    M dropProperty(String propertyName);

    /**
     * Get drop property names
     *
     * @return The property names
     */
    List<String> getDropPropertyNames();
}
