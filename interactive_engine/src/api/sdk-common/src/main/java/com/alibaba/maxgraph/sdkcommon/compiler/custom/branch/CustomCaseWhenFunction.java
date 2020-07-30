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
package com.alibaba.maxgraph.sdkcommon.compiler.custom.branch;

import com.google.common.collect.Lists;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;

import java.util.List;
import java.util.function.Function;

public class CustomCaseWhenFunction<T, R> implements Function<Traverser<T>, R> {
    private Traversal<?, ?> caseTraversal;
    private List<CustomWhenThenFunction> whenThenFunctionList;
    private Traversal<?, ?> elseEndTraversal;

    public CustomCaseWhenFunction() {
        this.caseTraversal = null;
        this.whenThenFunctionList = Lists.newArrayList();
        this.elseEndTraversal = null;
    }

    public void setCaseTraversal(Traversal<?, ?> caseTraversal) {
        this.caseTraversal = caseTraversal;
    }

    public void addWhenThenFunction(CustomWhenThenFunction whenThenFunction) {
        this.whenThenFunctionList.add(whenThenFunction);
    }

    public void setElseEndTraversal(Traversal<?, ?> elseEndTraversal) {
        this.elseEndTraversal = elseEndTraversal;
    }

    public Traversal<?, ?> getCaseTraversal() {
        return this.caseTraversal;
    }

    public List<CustomWhenThenFunction> getWhenThenFunctionList() {
        return this.whenThenFunctionList;
    }

    public Traversal<?, ?> getElseEndTraversal() {
        return this.elseEndTraversal;
    }

    @Override
    public R apply(Traverser<T> tTraverser) {
        throw new IllegalArgumentException();
    }
}
