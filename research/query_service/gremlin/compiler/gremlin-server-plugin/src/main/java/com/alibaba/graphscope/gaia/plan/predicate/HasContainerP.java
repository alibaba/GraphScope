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

import com.alibaba.graphscope.common.proto.Common;
import com.alibaba.graphscope.common.proto.Gremlin;
import com.alibaba.graphscope.gaia.FilterHelper;

import com.alibaba.graphscope.gaia.plan.extractor.TagKeyExtractorFactory;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.step.HasContainerHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.stream.Collectors;

public class HasContainerP implements PredicateContainer {
    private static final Logger logger = LoggerFactory.getLogger(HasContainerP.class);
    private List<HasContainer> containerList;
    private int containerIdx = -1;

    public HasContainerP(HasContainerHolder holder) {
        this.containerList = holder.getHasContainers();
    }

    @Override
    public Gremlin.FilterExp generateSimpleP(P predicate) {
        return generateFilter(
                TagKeyExtractorFactory.HasContainer.extractFrom(containerList.get(containerIdx).getKey()).getByKey().getKey(),
                predicate, false);
    }

    @Override
    public boolean hasNext() {
        return containerIdx + 1 < containerList.size();
    }

    @Override
    public P next() {
        return containerList.get(++containerIdx).getPredicate();
    }

    public static Gremlin.FilterExp generateFilter(Common.Key key, P predicate, boolean ignoreValue) {
        if (key.getItemCase() == Common.Key.ItemCase.LABEL) {
            if (ignoreValue) {
                return FilterHelper.INSTANCE.labelPredicate((Number) null, predicate.getBiPredicate());
            } else {
                if (predicate.getValue() instanceof String) {
                    return FilterHelper.INSTANCE.labelPredicate(Integer.valueOf((String) predicate.getValue()), predicate.getBiPredicate());
                } else if (predicate.getValue() instanceof List && ((List) predicate.getValue()).get(0) instanceof String) {
                    List<String> values = (List<String>) predicate.getValue();
                    return FilterHelper.INSTANCE.labelPredicate(values.stream().map(k -> Integer.valueOf(k)).collect(Collectors.toList()),
                            predicate.getBiPredicate());
                } else {
                    throw new UnsupportedOperationException("hasLabel value type not support " + predicate.getValue().getClass());
                }
            }
        } else if (key.getItemCase() == Common.Key.ItemCase.ID) {
            if (ignoreValue) {
                return FilterHelper.INSTANCE.idPredicate((Number) null, predicate.getBiPredicate());
            } else {
                if (predicate.getValue() instanceof List && ((List) predicate.getValue()).get(0) instanceof Number) {
                    return FilterHelper.INSTANCE.idPredicate((List<Number>) predicate.getValue(), predicate.getBiPredicate());
                } else if (predicate.getValue() instanceof Number) {
                    return FilterHelper.INSTANCE.idPredicate((Number) predicate.getValue(), predicate.getBiPredicate());
                } else {
                    throw new UnsupportedOperationException("hasId value type not support " + predicate.getValue().getClass());
                }
            }
        } else if (key.getItemCase() == Common.Key.ItemCase.NAME) {
            if (ignoreValue) {
                return FilterHelper.INSTANCE.propertyPredicate(key.getName(), (Number) null, predicate.getBiPredicate());
            } else {
                if (predicate.getValue() instanceof Number) {
                    return FilterHelper.INSTANCE.propertyPredicate(key.getName(), (Number) predicate.getValue(), predicate.getBiPredicate());
                } else if (predicate.getValue() instanceof String) {
                    return FilterHelper.INSTANCE.propertyPredicate(key.getName(), (String) predicate.getValue(), predicate.getBiPredicate());
                } else if (predicate.getValue() instanceof List) {
                    return FilterHelper.INSTANCE.propertyPredicate(key.getName(), (List) predicate.getValue(), predicate.getBiPredicate());
                } else {
                    throw new UnsupportedOperationException("property value type not support " + predicate.getValue().getClass()
                            + " for " + predicate.getBiPredicate());
                }
            }
        } else if (key.getItemCase() == Common.Key.ItemCase.NAME_ID) {
            if (ignoreValue) {
                return FilterHelper.INSTANCE.propertyPredicate(key.getNameId(), (Number) null, predicate.getBiPredicate());
            } else {
                if (predicate.getValue() instanceof Number) {
                    return FilterHelper.INSTANCE.propertyPredicate(key.getNameId(), (Number) predicate.getValue(), predicate.getBiPredicate());
                } else if (predicate.getValue() instanceof String) {
                    return FilterHelper.INSTANCE.propertyPredicate(key.getNameId(), (String) predicate.getValue(), predicate.getBiPredicate());
                } else if (predicate.getValue() instanceof List) {
                    return FilterHelper.INSTANCE.propertyPredicate(key.getNameId(), (List) predicate.getValue(), predicate.getBiPredicate());
                } else {
                    throw new UnsupportedOperationException("property value type not support " + predicate.getValue().getClass()
                            + " for " + predicate.getBiPredicate());
                }
            }
        } else {
            throw new UnsupportedOperationException("not support " + Common.Key.ItemCase.NAME_ID);
        }
    }
}
