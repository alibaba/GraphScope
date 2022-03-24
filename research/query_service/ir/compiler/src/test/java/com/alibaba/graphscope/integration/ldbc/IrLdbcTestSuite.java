package com.alibaba.graphscope.integration.ldbc;

import org.apache.tinkerpop.gremlin.AbstractGremlinSuite;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalEngine;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.RunnerBuilder;

public class IrLdbcTestSuite extends AbstractGremlinSuite {

    private static final Class<?>[] allTests =
            new Class<?>[] {
                LdbcQueryTest.Traversals.class,
            };

    private static final Class<?>[] testsToEnforce =
            new Class<?>[] {
                LdbcQueryTest.Traversals.class,
            };

    public IrLdbcTestSuite(final Class<?> klass, final RunnerBuilder builder)
            throws InitializationError {
        super(klass, builder, allTests, testsToEnforce, false, TraversalEngine.Type.STANDARD);
    }

    public IrLdbcTestSuite(
            final Class<?> klass, final RunnerBuilder builder, final Class<?>[] testsToExecute)
            throws InitializationError {
        super(klass, builder, testsToExecute, testsToEnforce, true, TraversalEngine.Type.STANDARD);
    }
}
