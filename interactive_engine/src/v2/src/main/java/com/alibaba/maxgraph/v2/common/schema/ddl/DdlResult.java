package com.alibaba.maxgraph.v2.common.schema.ddl;

import com.alibaba.maxgraph.v2.common.operation.Operation;
import com.alibaba.maxgraph.v2.common.schema.GraphDef;

import java.util.List;

public class DdlResult {

    private GraphDef graphDef;

    private List<Operation> ddlOperations;

    public DdlResult(GraphDef graphDef, List<Operation> ddlOperation) {
        this.graphDef = graphDef;
        this.ddlOperations = ddlOperation;
    }

    public GraphDef getGraphDef() {
        return graphDef;
    }

    public List<Operation> getDdlOperations() {
        return ddlOperations;
    }
}
