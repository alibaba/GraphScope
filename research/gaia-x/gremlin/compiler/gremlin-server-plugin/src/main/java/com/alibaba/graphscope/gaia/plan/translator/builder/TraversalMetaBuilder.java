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
package com.alibaba.graphscope.gaia.plan.translator.builder;

import com.alibaba.graphscope.gaia.plan.meta.object.TraversalId;
import com.alibaba.graphscope.gaia.plan.meta.object.TraverserElement;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;

public class TraversalMetaBuilder extends ConfigBuilder<TraversalMetaBuilder> {
    private Traversal.Admin admin;
    private TraverserElement head;
    private TraversalId metaId;

    public TraversalMetaBuilder(Traversal.Admin admin, TraverserElement head, TraversalId metaId) {
        this.admin = admin;
        this.head = head;
        this.metaId = metaId;
    }

    public Traversal.Admin getAdmin() {
        return admin;
    }

    public TraverserElement getHead() {
        return head;
    }

    public TraversalId getMetaId() {
        return metaId;
    }
}
