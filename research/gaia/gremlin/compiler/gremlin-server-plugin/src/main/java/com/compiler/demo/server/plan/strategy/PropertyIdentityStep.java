/**
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
package com.compiler.demo.server.plan.strategy;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.AbstractStep;
import org.apache.tinkerpop.gremlin.structure.Element;

import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;

public class PropertyIdentityStep<S, E extends Element> extends AbstractStep<S, E> {
    private List<String> attachProperties;
    private boolean needAll;

    public PropertyIdentityStep(Traversal.Admin traversal, List<String> properties, boolean needAll) {
        super(traversal);
        this.attachProperties = properties;
        this.needAll = needAll;
    }

    public static PropertyIdentityStep createDefault(Step step) {
        return new PropertyIdentityStep(step.getTraversal(), Collections.EMPTY_LIST, true);
    }

    @Override
    protected Traverser.Admin processNextStart() throws NoSuchElementException {
        throw new UnsupportedOperationException();
    }

    public List<String> getAttachProperties() {
        return attachProperties;
    }

    public boolean isNeedAll() {
        return needAll;
    }
}
