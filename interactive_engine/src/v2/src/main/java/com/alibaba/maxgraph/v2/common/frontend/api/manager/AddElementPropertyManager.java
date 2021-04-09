package com.alibaba.maxgraph.v2.common.frontend.api.manager;

import com.alibaba.maxgraph.v2.common.frontend.api.schema.GraphProperty;

import java.util.List;

public interface AddElementPropertyManager<M extends AddElementPropertyManager> {
    /**
     * Add a property definition to the given element type
     *
     * @param propertyName The property name
     * @param dataType     The data type such as "int"/"long"/"string"/"float"/"double"
     * @return The create type manager
     */
    M addProperty(String propertyName, String dataType);

    /**
     * Add a property definition to the given element type
     *
     * @param propertyName The property name
     * @param dataType     The data type such as "int"/"long"/"string"/"float"/"double"
     * @param comment      The comment of the property
     * @return The create type manager
     */
    M addProperty(String propertyName, String dataType, String comment);

    /**
     * Add a property definition to the given element type
     *
     * @param propertyName The property name
     * @param dataType     The data type such as "int"/"long"/"string"/"float"/"double"
     * @param comment      The comment of the property
     * @param defaultValue The default value
     * @return The create type manager
     */
    M addProperty(String propertyName, String dataType, String comment, Object defaultValue);

    /**
     * Add the comment of this type
     *
     * @param comment The given comment
     * @return The create type manager
     */
    M comment(String comment);

    /**
     * Get element type name
     *
     * @return The label name
     */
    String getLabel();

    /**
     * Get property definitions
     *
     * @return The property definitions
     */
    List<GraphProperty> getPropertyDefinitions();
}
