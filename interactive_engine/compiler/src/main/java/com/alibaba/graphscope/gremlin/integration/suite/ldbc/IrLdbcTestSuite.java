/*
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.graphscope.gremlin.integration.suite.ldbc;

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
