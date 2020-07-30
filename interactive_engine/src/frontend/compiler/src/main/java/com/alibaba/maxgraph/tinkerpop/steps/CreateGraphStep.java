/**
 * Copyright 2020 Alibaba Group Holding Limited.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.maxgraph.tinkerpop.steps;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang3.StringUtils;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.Configuring;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.AbstractStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.Parameters;

import java.util.NoSuchElementException;

public class CreateGraphStep<S, E> extends AbstractStep<S, E> implements TraversalParent, Configuring {
    private static final long serialVersionUID = 2261984133446083977L;

    private String graphName;
    private Configuration configuration = new BaseConfiguration();

    public CreateGraphStep(Traversal.Admin traversal, String graphName) {
        super(traversal);
        this.graphName = graphName;
    }

    @Override
    protected Traverser.Admin<E> processNextStart() throws NoSuchElementException {
        throw new UnsupportedOperationException();
    }

    public String getGraphName() {
        return this.graphName;
    }

    @Override
    public void configure(Object... keyValues) {
        if (null == keyValues || keyValues.length <= 1 || keyValues.length % 2 != 0) {
            throw new IllegalArgumentException("Invalid key values configuration" + StringUtils.join(keyValues, ","));
        }
        for (int i = 0; i < keyValues.length - 1; i += 2) {
            Object key = keyValues[i];
            Object value = keyValues[i + 1];
            if (null == key) {
                throw new IllegalArgumentException("key cant be null");
            }
            configuration.addProperty(key.toString(), value);
        }
    }

    @Override
    public Parameters getParameters() {
        throw new IllegalArgumentException();
    }

    public Configuration getConfiguration() {
        return this.configuration;
    }
}
