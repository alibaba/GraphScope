package com.alibaba.maxgraph.v2.common.frontend.api.schema;

import com.alibaba.maxgraph.v2.common.schema.DataType;

/**
 * Property definition in graph schema
 */
public interface GraphProperty {
    /**
     * Global id for property
     *
     * @return The global property id
     */
    int getId();

    /**
     * Name for property
     *
     * @return The property name
     */
    String getName();

    /**
     * Data type for property
     *
     * @return The datatype
     */
    DataType getDataType();

    /**
     * The comment of the property
     *
     * @return The comment
     */
    String getComment();

    /**
     * The property has default value
     *
     * @return true if the property has default value
     */
    boolean hasDefaultValue();

    /**
     * Return the default value
     *
     * @return The default value of the property
     */
    Object getDefaultValue();
}
