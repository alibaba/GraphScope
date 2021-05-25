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
package com.alibaba.maxgraph.v2.frontend.graph.schema;

import com.alibaba.maxgraph.v2.common.frontend.api.exception.GraphCreateSchemaException;
import com.alibaba.maxgraph.v2.common.frontend.api.manager.DropVertexTypeManager;
import org.apache.commons.lang.StringUtils;

/**
 * Drop vertex type manager
 */
public class MaxGraphDropVertexTypeManager implements DropVertexTypeManager {
    private String label;

    public MaxGraphDropVertexTypeManager(String label) {
        this.label = label;
    }

    @Override
    public String getLabel() {
        if (StringUtils.isEmpty(label)) {
            throw new GraphCreateSchemaException("drop vertex type cant be empty");
        }
        return label;
    }
}
