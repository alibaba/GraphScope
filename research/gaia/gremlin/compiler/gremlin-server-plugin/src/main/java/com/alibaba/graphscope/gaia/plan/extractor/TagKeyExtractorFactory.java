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
import com.alibaba.graphscope.gaia.idmaker.IdMaker;
import com.alibaba.graphscope.gaia.plan.PlanUtils;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.StringUtils;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.ColumnTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.*;
import org.apache.tinkerpop.gremlin.structure.Column;
import org.apache.tinkerpop.gremlin.structure.T;

import java.util.Map;

public enum TagKeyExtractorFactory implements TagKeyExtractor {
    Select {
        @Override
        public Gremlin.TagKey extractFrom(Object... args) {
            String tag = (String) args[0];
            Traversal.Admin modulateNext = (Traversal.Admin) args[1];
            boolean ignoreTag = true;
            if (args.length > 2) {
                ignoreTag = (boolean) args[2];
            }
            Gremlin.TagKey.Builder tagBuilder = Gremlin.TagKey.newBuilder();
            if (ignoreTag) {
                // set random non-zero value
                tagBuilder.setTag(Gremlin.StepTag.newBuilder().setTag(1));
            } else {
                IdMaker tagIdMaker = PlanUtils.getTagIdMaker((Configuration) args[3]);
                tagBuilder.setTag(Gremlin.StepTag.newBuilder().setTag((int) tagIdMaker.getId(tag)));
            }

            Gremlin.ByKey key = modulateBy(modulateNext);
            if (key.getItemCase() != Gremlin.ByKey.ItemCase.ITEM_NOT_SET) {
                tagBuilder.setByKey(key);
            }
            return tagBuilder.build();
        }
    },
    /**
     * order().by(select(keys/values))
     * order().by(select(tag)) must be value
     * order().by(select(tag).by("name"))
     * order().by("name")
     * order().by(T.id/T.label)
     * order().by(values("name","age"))
     */
    OrderBY {
        @Override
        public Gremlin.TagKey extractFrom(Object... args) {
            Traversal.Admin orderBy = (Traversal.Admin) args[0];
            // default value
            boolean ignoreTag = true;
            if (args.length > 1) {
                ignoreTag = (boolean) args[1];
            }
            if (orderBy != null && orderBy.getSteps().size() == 1 && orderBy.getStartStep() instanceof SelectOneStep) {
                Map.Entry<String, Traversal.Admin> firstTagTraversal = PlanUtils.getFirstEntry(
                        PlanUtils.getSelectTraversalMap(orderBy.getStartStep()));
                Gremlin.TagKey selectResult;
                if (ignoreTag) {
                    selectResult = Select.extractFrom(firstTagTraversal.getKey(), firstTagTraversal.getValue());
                } else {
                    selectResult = Select.extractFrom(firstTagTraversal.getKey(), firstTagTraversal.getValue(), false, args[2]);
                }
                if (selectResult.getByKey().getItemCase() == Gremlin.ByKey.ItemCase.PROP_KEYS) {
                    throw new UnsupportedOperationException("value map not support in order by step " + selectResult.getByKey().getPropKeys());
                }
                return selectResult;
            } else if (super.isSimpleValue(orderBy)) {
                Gremlin.TagKey.Builder tagKeyBuilder = Gremlin.TagKey.newBuilder();
                Gremlin.ByKey byKey = modulateBy(orderBy);
                if (byKey.getItemCase() == Gremlin.ByKey.ItemCase.PROP_KEYS) {
                    throw new UnsupportedOperationException("value map not support in order by step " + byKey.getPropKeys());
                }
                if (byKey.getItemCase() != Gremlin.ByKey.ItemCase.ITEM_NOT_SET) {
                    tagKeyBuilder.setByKey(byKey);
                }
                return tagKeyBuilder.build();
            } else {
                throw new UnsupportedOperationException("cannot support other order by traversal " + orderBy);
            }
        }

        @Override
        public boolean isSimpleValue(Traversal.Admin value) {
            return (value != null && value.getSteps().size() == 1 && value.getStartStep() instanceof SelectOneStep)
                    || super.isSimpleValue(value);
        }
    },
    GroupKeyBy {
        @Override
        public Gremlin.TagKey extractFrom(Object... args) {
            Traversal.Admin keyTraversal = (Traversal.Admin) args[0];
            // default value
            boolean ignoreTag = true;
            if (args.length > 1) {
                ignoreTag = (boolean) args[1];
            }
            if (keyTraversal != null && keyTraversal.getSteps().size() == 1 && (keyTraversal.getStartStep() instanceof SelectOneStep)) {
                Map.Entry<String, Traversal.Admin> firstTagTraversal = PlanUtils.getFirstEntry(
                        PlanUtils.getSelectTraversalMap(keyTraversal.getStartStep()));
                if (ignoreTag) {
                    return Select.extractFrom(firstTagTraversal.getKey(), firstTagTraversal.getValue());
                } else {
                    return Select.extractFrom(firstTagTraversal.getKey(), firstTagTraversal.getValue(), false, args[2]);
                }
            } else if (super.isSimpleValue(keyTraversal)) {
                Gremlin.TagKey.Builder tagKeyBuilder = Gremlin.TagKey.newBuilder();
                Gremlin.ByKey byKey = modulateBy(keyTraversal);
                if (byKey.getItemCase() != Gremlin.ByKey.ItemCase.ITEM_NOT_SET) {
                    tagKeyBuilder.setByKey(byKey);
                }
                return tagKeyBuilder.build();
            } else {
                throw new UnsupportedOperationException("cannot support other group by traversal " + keyTraversal);
            }
        }

        @Override
        public boolean isSimpleValue(Traversal.Admin value) {
            return value != null && value.getSteps().size() == 1 && value.getStartStep() instanceof SelectOneStep
                    || super.isSimpleValue(value);
        }
    },
    GroupValueBy {
        @Override
        public Gremlin.TagKey extractFrom(Object... args) {
            throw new UnsupportedOperationException("group value traversal not implemented");
        }

        @Override
        public boolean isSimpleValue(Traversal.Admin value) {
            return value == null || value.getSteps().isEmpty()
                    || value.getSteps().size() == 1 && value.getStartStep() instanceof FoldStep
                    || value.getSteps().size() == 2 && PlanUtils.isIdentityTraversalMap(value.getStartStep()) && value.getEndStep() instanceof FoldStep
                    || value.getSteps().size() == 1 && value.getStartStep() instanceof CountGlobalStep;
        }
    },
    WherePredicate {
        @Override
        public Gremlin.TagKey extractFrom(Object... args) {
            // modulateNext must not be null
            Traversal.Admin modulateNext = (Traversal.Admin) args[0];
            return Gremlin.TagKey.newBuilder().setByKey(modulateBy(modulateNext)).build();
        }
    },
    HasContainer {
        @Override
        public Gremlin.TagKey extractFrom(Object... args) {
            String key = (String) args[0];
            Common.Key.Builder builder = Common.Key.newBuilder();
            if (key.equals(T.label.getAccessor())) {
                builder.setLabel(Common.LabelKey.newBuilder());
            } else if (key.equals(T.id.getAccessor())) {
                builder.setId(Common.IdKey.newBuilder());
            } else {
                if (StringUtils.isNumeric(key)) {
                    builder.setNameId(Integer.valueOf(key));
                } else {
                    builder.setName(key);
                }
            }
            return Gremlin.TagKey.newBuilder().setByKey(Gremlin.ByKey.newBuilder().setKey(builder)).build();
        }
    },
    TraversalMap {
        @Override
        public Gremlin.TagKey extractFrom(Object... args) {
            Traversal.Admin mapTraversal = (Traversal.Admin) args[0];
            if (mapTraversal instanceof ColumnTraversal) {
                Column column = ((ColumnTraversal) mapTraversal).getColumn();
                Gremlin.ByKey.Builder byKeyBuilder = Gremlin.ByKey.newBuilder();
                if (column == Column.keys) {
                    byKeyBuilder.setMapKeys(Gremlin.MapKey.newBuilder());
                } else {
                    byKeyBuilder.setMapValues(Gremlin.MapValue.newBuilder());
                }
                return Gremlin.TagKey.newBuilder().setByKey(byKeyBuilder).build();
            } else {
                throw new UnsupportedOperationException("cannot support other map traversal " + mapTraversal);
            }
        }
    }
}
