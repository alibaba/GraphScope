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
package com.alibaba.maxgraph.compiler.api.schema;

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
