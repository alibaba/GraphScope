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
package com.alibaba.graphscope.gaia.plan.meta;

import com.alibaba.graphscope.gaia.plan.meta.object.StepId;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;

import java.util.HashSet;
import java.util.Set;

public class TraverserRequirementMeta {
    private StepId pathStepId;
    private Set<String> removeTags = new HashSet<>();
    private StepId labelPathStepId;

    public TraverserRequirement getTraverserRequirement() {
        if (pathStepId != null) {
            return TraverserRequirement.PATH;
        } else if (labelPathStepId != null) {
            return TraverserRequirement.LABELED_PATH;
        } else {
            return TraverserRequirement.OBJECT;
        }
    }

    public void setPathStepId(StepId stepId) {
        this.pathStepId = stepId;
    }

    public void setLabelPathStepId(StepId stepId) {
        this.labelPathStepId = stepId;
    }

    public void addAllRemoveTags(Set<String> tags) {
        if (getTraverserRequirement() == TraverserRequirement.PATH) {
            this.removeTags.addAll(tags);
        }
    }

    public Set<String> getRemoveTags() {
        return this.removeTags;
    }

    public StepId getPathStepId() {
        return pathStepId;
    }

    public StepId getLabelPathStepId() {
        return labelPathStepId;
    }
}
