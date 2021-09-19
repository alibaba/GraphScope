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
package com.alibaba.graphscope.gaia.plan.predicate;

import com.alibaba.graphscope.common.proto.Gremlin;
import com.alibaba.graphscope.gaia.plan.extractor.TagKeyExtractorFactory;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalRing;

public class WherePredicateP implements PredicateContainer {
    private P predicate;
    private TraversalRing traversalRing;
    private boolean hasNext = true;

    public WherePredicateP(P predicate, TraversalRing traversalRing) {
        this.predicate = predicate;
        this.traversalRing = traversalRing;
    }

    @Override
    public Gremlin.FilterExp generateSimpleP(P predicate) {
        return HasContainerP.generateFilter(
                TagKeyExtractorFactory.WherePredicate.extractFrom(traversalRing.next()).getByKey().getKey(),
                predicate, true);
    }

    @Override
    public boolean hasNext() {
        boolean oldState = this.hasNext;
        this.hasNext = false;
        return oldState;
    }

    @Override
    public P next() {
        return this.predicate;
    }
}
