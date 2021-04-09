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
