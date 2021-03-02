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
package com.compiler.demo.server.plan.strategy.shuffle;

import com.alibaba.graphscope.common.proto.Common;
import com.alibaba.graphscope.common.proto.Gremlin;
import com.compiler.demo.server.plan.extractor.TagKeyExtractorFactory;
import com.compiler.demo.server.plan.PlanUtils;
import com.compiler.demo.server.plan.strategy.PropertyIdentityStep;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GroupCountStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GroupStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.SelectOneStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.SelectStep;

public class GroupByProperty extends PropertyShuffler {
    private Traversal.Admin keyTraversal;

    public GroupByProperty(Step step) {
        super(step);
        if (step instanceof GroupCountStep || step instanceof GroupStep) {
            keyTraversal = PlanUtils.getKeyTraversal(step);
        } else {
            throw new UnsupportedOperationException("cannot support other step in group property " + step.getClass());
        }
    }

    @Override
    protected boolean match() {
        Step previousOut = getPreviousOut();
        // guarantee no select between out and group().by()
        if (previousOut != null) {
            Step p = this.step;
            p = p.getPreviousStep();
            while (p != previousOut) {
                if (p instanceof SelectStep || p instanceof SelectOneStep) return false;
                p = p.getPreviousStep();
            }
        }
        return isGroupByPropertyPattern(this.keyTraversal);
    }

    // group().by("p1")/by(valueMap())
    public static boolean isGroupByPropertyPattern(Traversal.Admin keyTraversal) {
        if (keyTraversal != null && TagKeyExtractorFactory.GroupBy.isSimpleValue(keyTraversal)) {
            Gremlin.TagKey tagKey = TagKeyExtractorFactory.GroupBy.extractFrom(keyTraversal);
            if (tagKey.getTag().isEmpty() && (tagKey.getByKey().getItemCase() == Gremlin.ByKey.ItemCase.KEY
                    && tagKey.getByKey().getKey().getItemCase() == Common.Key.ItemCase.NAME
                    || tagKey.getByKey().getItemCase() == Gremlin.ByKey.ItemCase.NAME)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public int transform() {
        if (!match()) return stepIdx + 1;
        Traversal.Admin traversal = step.getTraversal();
        traversal.addStep(stepIdx, PropertyIdentityStep.createDefault(step));
        return stepIdx + 2;
    }
}
