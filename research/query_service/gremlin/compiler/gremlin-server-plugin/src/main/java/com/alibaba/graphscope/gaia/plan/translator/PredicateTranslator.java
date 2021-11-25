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
package com.alibaba.graphscope.gaia.plan.translator;

import com.alibaba.graphscope.common.proto.Gremlin;
import com.alibaba.graphscope.gaia.plan.predicate.PredicateContainer;
import com.alibaba.graphscope.gaia.FilterChainHelper;
import com.alibaba.graphscope.gaia.FilterHelper;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.util.AndP;
import org.apache.tinkerpop.gremlin.process.traversal.util.ConnectiveP;
import org.apache.tinkerpop.gremlin.process.traversal.util.OrP;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Function;

public class PredicateTranslator extends AttributeTranslator<PredicateContainer, Gremlin.FilterChain> {
    private static Logger logger = LoggerFactory.getLogger(PredicateTranslator.class);

    public PredicateTranslator(PredicateContainer pContainer) {
        super(pContainer);
    }

    @Override
    protected Function<PredicateContainer, Gremlin.FilterChain> getApplyFunc() {
        return (PredicateContainer pContainer) -> {
            Gremlin.FilterChain.Builder result = FilterChainHelper.createFilterChain();
            while (pContainer.hasNext()) {
                FilterChainHelper.and(result, recursive(pContainer.next(), pContainer));
            }
            // printPredicate(result.build());
            return result.build();
        };
    }

    protected Gremlin.FilterChain recursive(P predicate, PredicateContainer container) {
        if (predicate instanceof ConnectiveP) {
            Gremlin.FilterChain.Builder result = FilterChainHelper.createFilterChain();
            ((ConnectiveP) predicate).getPredicates().forEach(p -> {
                if (predicate instanceof AndP) {
                    FilterChainHelper.and(result, recursive((P) p, container));
                } else if (predicate instanceof OrP) {
                    FilterChainHelper.or(result, recursive((P) p, container));
                } else {
                    logger.error("Predicate Type {} is invalid", p.getClass());
                }
            });
            return result.build();
        } else {
            Gremlin.FilterExp simple = container.generateSimpleP(predicate);
            return FilterHelper.INSTANCE.asChain(simple);
        }
    }

    protected void printPredicate(Gremlin.FilterChain chain) {
        try {
            for (Gremlin.FilterNode node : chain.getNodeList()) {
                if (node.getInnerCase() == Gremlin.FilterNode.InnerCase.SINGLE) {
                    logger.info(node.getSingle().toString());
                } else {
                    printPredicate(Gremlin.FilterChain.parseFrom(node.getChain()));
                }
                logger.info(node.getNext().toString());
            }
        } catch (Exception e) {
            logger.error("exception is " + e);
        }
    }
}
