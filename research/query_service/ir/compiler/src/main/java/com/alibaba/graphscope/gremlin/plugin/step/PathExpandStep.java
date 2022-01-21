package com.alibaba.graphscope.gremlin.plugin.step;

import com.alibaba.graphscope.gremlin.exception.ExtendGremlinStepException;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.Configuring;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.RangeGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.FlatMapStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.Parameters;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Iterator;

public class PathExpandStep<E extends Element> extends FlatMapStep<Vertex, E> implements AutoCloseable, Configuring {
    private String[] edgeLabels;
    private Direction direction;
    private Traversal rangeTraversal;

    public PathExpandStep(final Traversal.Admin traversal, final Direction direction,
                          final Traversal rangeTraversal, final String... edgeLabels) {
        super(traversal);
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

    public boolean returnsVertex() {
        return true;
    }

    @Override
    public void close() {
        throw new ExtendGremlinStepException("unsupported");
    }

    @Override
    protected Iterator<E> flatMap(Traverser.Admin<Vertex> traverser) {
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
