/**
 * This file is referred and derived from project apache/tinkerpop
 *
 *    https://github.com/apache/tinkerpop/blob/master/gremlin-test/src/main/java/org/apache/tinkerpop/gremlin/process/ProcessStandardSuite.java
 *
 * which has the following license:
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.alibaba.maxgraph.tests.gremlin;

import org.apache.tinkerpop.gremlin.AbstractGremlinSuite;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalEngine;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.RepeatTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.UnionTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.*;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.*;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.RunnerBuilder;

public class GremlinStandardTestSuite extends AbstractGremlinSuite {
    /**
     * This list of tests in the suite that will be executed as part of this suite.
     */
    private static final Class<?>[] allTests = new Class<?>[]{
            // Branch
            RepeatTest.Traversals.class,
            UnionTest.Traversals.class,

            // filter
            AndTest.Traversals.class,
            CyclicPathTest.Traversals.class,
            DedupTest.Traversals.class,
            HasTest.Traversals.class,
            IsTest.Traversals.class,
            RangeTest.Traversals.class,
            SimplePathTest.Traversals.class,
            WhereTest.Traversals.class,

            // map
            ConstantTest.Traversals.class,
            CountTest.Traversals.class,
            FlatMapTest.Traversals.class,
            FoldTest.Traversals.class,
            MaxTest.Traversals.class,
            MinTest.Traversals.class,
            SumTest.Traversals.class,
            OrderTest.Traversals.class,
            PathTest.Traversals.class,
            PropertiesTest.Traversals.class,
            SelectTest.Traversals.class,
            VertexTest.Traversals.class,
            ValueMapTest.Traversals.class,
    };

    /**
     * A list of the minimum set of base tests that Gremlin flavors should implement to be compliant with Gremlin.
     */
    private static final Class<?>[] testsToEnforce = new Class<?>[]{
            // Repeat
            RepeatTest.class,
            UnionTest.class,

            // filter
            AndTest.class,
            CyclicPathTest.class,
            DedupTest.class,
            HasTest.class,
            IsTest.class,
            RangeTest.class,
            SimplePathTest.class,
            WhereTest.class,

            // map
            ConstantTest.class,
            CountTest.class,
            FlatMapTest.class,
            FoldTest.class,
            MaxTest.class,
            MinTest.class,
            SumTest.class,
            OrderTest.class,
            PathTest.class,
            PropertiesTest.class,
            SelectTest.class,
            VertexTest.class,
            ValueMapTest.class,
    };

    /**
     * This constructor is used by JUnit and will run this suite with its concrete implementations of the
     * {@code testsToEnforce}.
     */
    public GremlinStandardTestSuite(final Class<?> klass, final RunnerBuilder builder) throws InitializationError {
        super(klass, builder, allTests, testsToEnforce, false, TraversalEngine.Type.STANDARD);
    }

    /**
     * This constructor is used by Gremlin flavor implementers who supply their own implementations of the
     * {@code testsToEnforce}.
     */
    public GremlinStandardTestSuite(final Class<?> klass, final RunnerBuilder builder, final Class<?>[] testsToExecute) throws InitializationError {
        super(klass, builder, testsToExecute, testsToEnforce, true, TraversalEngine.Type.STANDARD);
    }
}
