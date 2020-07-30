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

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;

public class CustomThenFunction {
    private CustomCaseWhenFunction customCaseWhenFunction;
    private Traversal<?, ?> whenPredicate;

    public void setCaseWhenFunction(CustomCaseWhenFunction customCaseWhenFunction) {
        this.customCaseWhenFunction = customCaseWhenFunction;
    }

    public void setWhenPredicate(Traversal<?, ?> whenPredicate) {
        this.whenPredicate = whenPredicate;
    }

    public CustomWhenFunction then(Traversal<?, ?> thenTraversal) {
        CustomWhenThenFunction whenThenFunction = new CustomWhenThenFunction();
        whenThenFunction.setWhenPredicate(whenPredicate);
        whenThenFunction.setThenTraversal(thenTraversal);
        customCaseWhenFunction.addWhenThenFunction(whenThenFunction);

        CustomWhenFunction customWhenFunction = new CustomWhenFunction();
        customWhenFunction.setCustomCaseWhenFunction(customCaseWhenFunction);
        return customWhenFunction;
    }

    public CustomCaseWhenFunction end() {
        return this.customCaseWhenFunction;
    }
}
