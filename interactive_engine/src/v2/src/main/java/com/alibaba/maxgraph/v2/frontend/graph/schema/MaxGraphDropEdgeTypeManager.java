package com.alibaba.maxgraph.v2.frontend.graph.schema;

import com.alibaba.maxgraph.v2.common.frontend.api.exception.GraphCreateSchemaException;
import com.alibaba.maxgraph.v2.common.frontend.api.manager.DropEdgeTypeManager;
import org.apache.commons.lang.StringUtils;

/**
 * Drop edge type manager
 */
public class MaxGraphDropEdgeTypeManager implements DropEdgeTypeManager {
    private String label;

    public MaxGraphDropEdgeTypeManager(String label) {
        this.label = label;
    }

    @Override
    public String getLabel() {
        if (StringUtils.isEmpty(label)) {
            throw new GraphCreateSchemaException("drop edge type cant be empty");
        }
        return label;
    }
}
