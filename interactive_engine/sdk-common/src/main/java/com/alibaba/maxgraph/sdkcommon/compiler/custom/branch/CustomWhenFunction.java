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

public class CustomWhenFunction {
    private CustomCaseWhenFunction customCaseWhenFunction;

    public void setCustomCaseWhenFunction(CustomCaseWhenFunction customCaseWhenFunction) {
        this.customCaseWhenFunction = customCaseWhenFunction;
    }

    public CustomThenFunction when(Traversal<?, ?> whenPredicate) {
        CustomThenFunction customThenFunction = new CustomThenFunction();
        customThenFunction.setCaseWhenFunction(this.customCaseWhenFunction);
        customThenFunction.setWhenPredicate(whenPredicate);
        return customThenFunction;
    }

    public CustomCaseWhenFunction elseEnd(Traversal<?, ?> elseEndTraversal) {
        this.customCaseWhenFunction.setElseEndTraversal(elseEndTraversal);
        return this.customCaseWhenFunction;
    }
}
