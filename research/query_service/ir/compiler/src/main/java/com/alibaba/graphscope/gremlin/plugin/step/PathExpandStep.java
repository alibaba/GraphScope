package com.alibaba.graphscope.gremlin.plugin.step;

import com.alibaba.graphscope.gremlin.exception.ExtendGremlinStepException;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.RangeGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.Parameters;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Iterator;

public class PathExpandStep extends VertexStep<Vertex> {
    private String[] edgeLabels;
    private Direction direction;
    private Traversal rangeTraversal;

    public PathExpandStep(final Traversal.Admin traversal, final Direction direction,
                          final Traversal rangeTraversal, final String... edgeLabels) {
        super(traversal, Vertex.class, direction, edgeLabels);
        this.direction = direction;
        this.rangeTraversal = rangeTraversal;
        this.edgeLabels = edgeLabels;
    }

    public Direction getDirection() {
        return this.direction;
    }

    public String[] getEdgeLabels() {
        return this.edgeLabels;
    }

    public int getLower() {
        Traversal.Admin admin = rangeTraversal.asAdmin();
        if (admin.getSteps().size() == 1 && admin.getStartStep() instanceof RangeGlobalStep) {
            RangeGlobalStep range = (RangeGlobalStep) admin.getStartStep();
            return (int) range.getLowRange();
        } else {
            throw new ExtendGremlinStepException("rangeTraversal should only have one RangeGlobalStep");
        }
    }

    public int getUpper() {
        Traversal.Admin admin = rangeTraversal.asAdmin();
        if (admin.getSteps().size() == 1 && admin.getStartStep() instanceof RangeGlobalStep) {
            RangeGlobalStep range = (RangeGlobalStep) admin.getStartStep();
            return (int) range.getHighRange();
        } else {
            throw new ExtendGremlinStepException("rangeTraversal should only have one RangeGlobalStep");
        }
    }

    @Override
    public void close() {
        throw new ExtendGremlinStepException("unsupported");
    }

    @Override
    protected Iterator<Vertex> flatMap(Traverser.Admin<Vertex> traverser) {
        throw new ExtendGremlinStepException("unsupported");
    }

    @Override
    public void configure(Object... keyValues) {
        throw new ExtendGremlinStepException("unsupported");
    }

    @Override
    public Parameters getParameters() {
        throw new ExtendGremlinStepException("unsupported");
    }
}
