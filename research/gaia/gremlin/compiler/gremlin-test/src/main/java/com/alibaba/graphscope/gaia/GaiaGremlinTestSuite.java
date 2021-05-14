package com.alibaba.graphscope.gaia;

import org.apache.tinkerpop.gremlin.AbstractGremlinSuite;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalEngine;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.RepeatTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.UnionTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.*;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.*;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GroupCountTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GroupTest;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.RunnerBuilder;

public class GaiaGremlinTestSuite extends AbstractGremlinSuite {

    private static final Class<?>[] allTests = new Class<?>[]{
            // branch
            RepeatTest.Traversals.class,
            UnionTest.Traversals.class,

            // filter
            CyclicPathTest.Traversals.class,
            DedupTest.Traversals.class,
            FilterTest.Traversals.class,
            HasTest.Traversals.class,
            IsTest.Traversals.class,
            RangeTest.Traversals.class,
            SimplePathTest.Traversals.class,
            WhereTest.Traversals.class,

            // map
            org.apache.tinkerpop.gremlin.process.traversal.step.map.CountTest.Traversals.class,
            GraphTest.Traversals.class,
            OrderTest.Traversals.class,
            PathTest.Traversals.class,
            PropertiesTest.Traversals.class,
            SelectTest.Traversals.class,
            VertexTest.Traversals.class,
            UnfoldTest.Traversals.class,
            ValueMapTest.Traversals.class,

            // sideEffect
            GroupTest.Traversals.class,
            GroupCountTest.Traversals.class,
//             SmallCountTest.Traversals.class
    };

    private static final Class<?>[] testsToEnforce = new Class<?>[]{
            // branch
            RepeatTest.Traversals.class,
            UnionTest.Traversals.class,

            // filter
            CyclicPathTest.Traversals.class,
            DedupTest.Traversals.class,
            FilterTest.Traversals.class,
            HasTest.Traversals.class,
            IsTest.Traversals.class,
            RangeTest.Traversals.class,
            SimplePathTest.Traversals.class,
            WhereTest.Traversals.class,

            // map
            org.apache.tinkerpop.gremlin.process.traversal.step.map.CountTest.Traversals.class,
            GraphTest.Traversals.class,
            OrderTest.Traversals.class,
            PathTest.Traversals.class,
            PropertiesTest.Traversals.class,
            SelectTest.Traversals.class,
            VertexTest.Traversals.class,
            UnfoldTest.Traversals.class,
            // ValueMapTest.Traversals.class,

            // sideEffect
            GroupTest.Traversals.class,
            GroupCountTest.Traversals.class,
//             SmallCountTest.Traversals.class
    };

    public GaiaGremlinTestSuite(final Class<?> klass, final RunnerBuilder builder) throws InitializationError {
        super(klass, builder, allTests, testsToEnforce, false, TraversalEngine.Type.STANDARD);
    }

    public GaiaGremlinTestSuite(final Class<?> klass, final RunnerBuilder builder, final Class<?>[] testsToExecute) throws InitializationError {
        super(klass, builder, testsToExecute, testsToEnforce, true, TraversalEngine.Type.STANDARD);
    }
}
