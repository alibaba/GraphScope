package com.alibaba.graphscope.groot.schema;

import com.alibaba.maxgraph.compiler.api.schema.EdgeRelation;
import com.alibaba.maxgraph.compiler.api.schema.GraphVertex;
import java.util.Objects;

public class EdgeRelationImpl implements EdgeRelation {

    private long tableId;
    private GraphVertex srcVertex;
    private GraphVertex dstVertex;

    public EdgeRelationImpl(long tableId, GraphVertex srcVertex, GraphVertex dstVertex) {
        this.tableId = tableId;
        this.srcVertex = srcVertex;
        this.dstVertex = dstVertex;
    }

    @Override
    public GraphVertex getSource() {
        return this.srcVertex;
    }

    @Override
    public GraphVertex getTarget() {
        return this.dstVertex;
    }

    @Override
    public long getTableId() {
        return this.tableId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        EdgeRelationImpl that = (EdgeRelationImpl) o;
        return tableId == that.tableId
                && Objects.equals(srcVertex, that.srcVertex)
                && Objects.equals(dstVertex, that.dstVertex);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableId, srcVertex, dstVertex);
    }
}
