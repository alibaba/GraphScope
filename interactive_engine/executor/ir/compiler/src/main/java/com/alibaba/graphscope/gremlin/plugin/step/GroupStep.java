package com.alibaba.graphscope.gremlin.plugin.step;

import com.google.common.base.Objects;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal.Admin;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.*;
import org.apache.tinkerpop.gremlin.process.traversal.step.ByModulating;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.LambdaMapStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ReducingBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.*;

public final class GroupStep<S, K, V> extends ReducingBarrierStep<S, Map<K, V>>
        implements TraversalParent, ByModulating, MultiByModulating {
    private char state = 'k';
    private List<Admin<S, K>> keyTraversalList;
    private List<Admin<S, K>> valueTraversalList;

    public GroupStep(final Admin traversal) {
        super(traversal);
        this.keyTraversalList = new ArrayList<>();
        this.valueTraversalList = new ArrayList<>();
        this.valueTraversalList.add(this.integrateChild(__.fold().asAdmin()));
    }

    @Override
    public void modulateBy(final Admin<?, ?> kvTraversal) {
        if ('k' == this.state) {
            this.keyTraversalList = Collections.singletonList(this.integrateChild(kvTraversal));
            this.state = 'v';
        } else {
            if ('v' != this.state) {
                throw new IllegalStateException(
                        "The key and value traversals for group()-step have already been set: "
                                + this);
            }

            this.valueTraversalList =
                    Collections.singletonList(
                            this.integrateChild(convertValueTraversal(kvTraversal)));
            this.state = 'x';
        }
    }

    @Override
    public void modulateBy(final List<Admin<?, ?>> kvTraversals) {
        if ('k' == this.state) {
            kvTraversals.forEach(k -> this.keyTraversalList.add(this.integrateChild(k)));
            this.state = 'v';
        } else {
            if ('v' != this.state) {
                throw new IllegalStateException(
                        "The key and value traversals for group()-step have already been set: "
                                + this);
            }
            this.valueTraversalList.clear();
            kvTraversals.forEach(
                    k ->
                            this.valueTraversalList.add(
                                    this.integrateChild(convertValueTraversal(k))));
            this.state = 'x';
        }
    }

    private Admin<?, ?> convertValueTraversal(final Admin<?, ?> valueTraversal) {
        return !(valueTraversal instanceof ValueTraversal)
                        && !(valueTraversal instanceof TokenTraversal)
                        && !(valueTraversal instanceof IdentityTraversal)
                        && !(valueTraversal instanceof ColumnTraversal)
                        && (!(valueTraversal.getStartStep() instanceof LambdaMapStep)
                                || !(((LambdaMapStep) valueTraversal.getStartStep())
                                                .getMapFunction()
                                        instanceof FunctionTraverser))
                ? valueTraversal
                : (Admin) __.map(valueTraversal).fold();
    }

    @Override
    public List<Admin<?, ?>> getLocalChildren() {
        List<Admin<?, ?>> children = new ArrayList();
        if (null != this.keyTraversalList) {
            children.addAll(this.keyTraversalList);
        }

        children.addAll(this.valueTraversalList);
        return children;
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return this.getSelfAndChildRequirements(
                new TraverserRequirement[] {
                    TraverserRequirement.OBJECT, TraverserRequirement.BULK
                });
    }

    public List<Admin<S, K>> getKeyTraversalList() {
        return keyTraversalList;
    }

    public List<Admin<S, K>> getValueTraversalList() {
        return valueTraversalList;
    }

    @Override
    public String toString() {
        return StringFactory.stepString(
                this, new Object[] {this.keyTraversalList, this.valueTraversalList});
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        GroupStep<?, ?, ?> groupStep = (GroupStep<?, ?, ?>) o;
        return Objects.equal(keyTraversalList, groupStep.keyTraversalList)
                && Objects.equal(valueTraversalList, groupStep.valueTraversalList);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(super.hashCode(), keyTraversalList, valueTraversalList);
    }

    @Override
    public Map<K, V> projectTraverser(Traverser.Admin<S> traverser) {
        throw new UnsupportedOperationException(
                "project traverser shoule be implemented in runtime but unnecessary in compiler");
    }
}
