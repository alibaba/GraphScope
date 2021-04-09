package com.alibaba.maxgraph.v2.common.frontend.api.schema;

import com.alibaba.maxgraph.v2.common.frontend.api.exception.GraphPropertyNotFoundException;

import java.util.List;

/**
 * Element in graph schema, such as vertex/edge
 */
public interface SchemaElement {
    /**
     * Get element label
     *
     * @return The element label
     */
    String getLabel();

    /**
     * Get element label id
     *
     * @return The element label id
     */
    int getLabelId();

    /**
     * Get element property list
     *
     * @return The property list
     */
    List<GraphProperty> getPropertyList();

    /**
     * Get property by property id
     *
     * @param propId The property id
     * @return The graph property
     * @throws GraphPropertyNotFoundException The thrown exception
     */
    GraphProperty getProperty(int propId) throws GraphPropertyNotFoundException;

    /**
     * Get property by property name
     * @param propName The property name
     * @return The graph property
     * @throws GraphPropertyNotFoundException The thrown exception
     */
    GraphProperty getProperty(String propName) throws GraphPropertyNotFoundException;

    int getVersionId();

    /**
     * Get primary key constraint for the given vertex type
     *
     * @return The primary key constraint
     */
    PrimaryKeyConstraint getPrimaryKeyConstraint();
}
