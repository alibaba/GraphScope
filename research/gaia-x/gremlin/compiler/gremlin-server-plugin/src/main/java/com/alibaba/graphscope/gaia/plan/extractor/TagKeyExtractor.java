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
package com.alibaba.graphscope.gaia.plan.extractor;

import com.alibaba.graphscope.common.proto.Common;
import com.alibaba.graphscope.common.proto.Gremlin;
import com.alibaba.graphscope.gaia.plan.PlanUtils;
import com.alibaba.graphscope.gaia.plan.strategy.PreBySubTraversal;
import com.alibaba.graphscope.gaia.plan.strategy.global.property.cache.ToFetchProperties;
import org.apache.commons.lang.StringUtils;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.ColumnTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.ElementValueTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.IdentityTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.TokenTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PropertiesStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PropertyMapStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.TraversalMapStep;
import org.apache.tinkerpop.gremlin.process.traversal.util.DefaultTraversal;
import org.apache.tinkerpop.gremlin.structure.Column;
import org.apache.tinkerpop.gremlin.structure.T;

import java.util.Arrays;

public interface TagKeyExtractor {
    default Gremlin.ByKey modulateBy(Traversal.Admin value) {
        Gremlin.ByKey.Builder builder = Gremlin.ByKey.newBuilder();
        if (value == null || value instanceof IdentityTraversal) {
            return Gremlin.ByKey.newBuilder().build();
        } else if (value instanceof ElementValueTraversal) {
            String propertyKey = ((ElementValueTraversal) value).getPropertyKey();
            if (StringUtils.isNumeric(propertyKey)) {
                builder.setKey(Common.Key.newBuilder().setNameId(Integer.valueOf(propertyKey)));
            } else {
                builder.setKey(Common.Key.newBuilder().setName(propertyKey));
            }
        } else if (value instanceof TokenTraversal) {
            // resultKeys.add(((TokenTraversal) value).getToken().getAccessor());
            T token = ((TokenTraversal) value).getToken();
            if (token == T.label) {
                builder.setKey(Common.Key.newBuilder().setLabel(Common.LabelKey.newBuilder()));
            } else if (token == T.id) {
                builder.setKey(Common.Key.newBuilder().setId(Common.IdKey.newBuilder()));
            } else {
                throw new UnsupportedOperationException("cannot support other T type " + token.name());
            }
        } else if (value instanceof ColumnTraversal) {
            Column column = ((ColumnTraversal) value).getColumn();
            if (column == Column.keys) {
                builder.setMapKeys(Gremlin.MapKey.newBuilder());
            } else {
                builder.setMapValues(Gremlin.MapValue.newBuilder());
            }
        } else if (value != null && value.getSteps().size() == 1 && value.getStartStep() instanceof PropertyMapStep) {
            PropertyMapStep propertyMapStep = (PropertyMapStep) value.getStartStep();
            String[] propertyKeys = propertyMapStep.getPropertyKeys();
            boolean needAllProps = (propertyKeys == null || propertyKeys.length == 0) ? true : false;
            builder.setPropKeys(PlanUtils.convertFrom(new ToFetchProperties(needAllProps, Arrays.asList(propertyKeys))));
        } else if (value != null && value.getSteps().size() == 1 && value.getStartStep() instanceof PropertiesStep) {
            PropertiesStep propertiesStep = (PropertiesStep) value.getStartStep();
            // always add first from values(p1,p2)
            // todo: if value() -> fetch first from all properties (support by runtime)
            if (propertiesStep.getPropertyKeys() != null && propertiesStep.getPropertyKeys().length > 0) {
                String propertyKey = (propertiesStep.getPropertyKeys())[0];
                if (StringUtils.isNumeric(propertyKey)) {
                    builder.setKey(Common.Key.newBuilder().setNameId(Integer.valueOf(propertyKey)));
                } else {
                    builder.setKey(Common.Key.newBuilder().setName(propertyKey));
                }
            }
        } else if (value != null && value instanceof PreBySubTraversal) {
            builder.setComputed(Gremlin.SubValue.newBuilder());
        } else if (value.getSteps().size() == 1 && value.getStartStep() instanceof TraversalMapStep) {
            TraversalMapStep startStep = (TraversalMapStep) value.getStartStep();
            Traversal.Admin mapTraversal = (Traversal.Admin) startStep.getLocalChildren().get(0);
            if (!(mapTraversal instanceof ColumnTraversal)) {
                throw new UnsupportedOperationException();
            } else {
                return modulateBy(mapTraversal);
            }
        } else if (value.getSteps().size() == 2 && (value.getStartStep() instanceof TraversalMapStep && value.getEndStep() instanceof PropertiesStep)) {
            TraversalMapStep startStep = (TraversalMapStep) value.getStartStep();
            Traversal.Admin mapTraversal = (Traversal.Admin) startStep.getLocalChildren().get(0);
            if (!(mapTraversal instanceof ColumnTraversal)) {
                throw new UnsupportedOperationException();
            } else {
                Gremlin.ByKey selectMap = modulateBy(mapTraversal);
                PropertiesStep valuePropertiesStep = (PropertiesStep) value.getEndStep();
                Traversal.Admin tmp = new DefaultTraversal();
                tmp.addStep(new PropertiesStep(tmp, valuePropertiesStep.getReturnType(), valuePropertiesStep.getPropertyKeys()));
                // select(keys).values("xxx")
                Gremlin.ByKey selectMapWithValue = modulateBy(tmp);
                if (selectMap.getItemCase() == Gremlin.ByKey.ItemCase.MAP_KEYS) {
                    return Gremlin.ByKey.newBuilder().setMapKeys(Gremlin.MapKey.newBuilder().setKey(selectMapWithValue.getKey())).build();
                } else {
                    return Gremlin.ByKey.newBuilder().setMapValues(Gremlin.MapValue.newBuilder().setKey(selectMapWithValue.getKey())).build();
                }
            }
        } else {
            throw new UnsupportedOperationException("cannot support other value traversal " + value);
        }
        return builder.build();
    }

    /**
     * @param value keys/values
     *              id/label
     *              property
     *              values()
     *              valueMap()
     *              traversal is pre-by-sub-traversal
     * @return
     */
    default boolean isSimpleValue(Traversal.Admin value) {
        return value == null || value instanceof IdentityTraversal || value instanceof ElementValueTraversal
                || value instanceof TokenTraversal || value instanceof ColumnTraversal || value instanceof PreBySubTraversal
                || value.getSteps().size() == 1 && (value.getStartStep() instanceof PropertyMapStep || value.getStartStep() instanceof PropertiesStep)
                || value.getSteps().size() == 2 && (value.getStartStep() instanceof TraversalMapStep && value.getEndStep() instanceof PropertiesStep)
                || value.getSteps().size() == 1 && value.getStartStep() instanceof TraversalMapStep;
    }

    Gremlin.TagKey extractFrom(Object... args);
}

