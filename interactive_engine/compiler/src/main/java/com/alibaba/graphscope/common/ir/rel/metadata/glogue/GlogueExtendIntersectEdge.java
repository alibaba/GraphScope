/*
 * Copyright 2024 Alibaba Group Holding Limited.
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

package com.alibaba.graphscope.common.ir.rel.metadata.glogue;

import com.alibaba.graphscope.common.ir.rel.metadata.glogue.pattern.Pattern;

import java.util.Map;

public class GlogueExtendIntersectEdge extends GlogueEdge {
    /// a mapping from src pattern vertex order to target pattern vertex order
    private Map<Integer, Integer> srcToTargetOrderMapping;
    /// a extend step records how to extend the src pattern to the target pattern
    private ExtendStep extendStep;
    /// the src pattern
    private Pattern srcPattern;
    /// the target pattern
    private Pattern dstPattern;

    public GlogueExtendIntersectEdge(
            Pattern srcPattern,
            Pattern dstPattern,
            ExtendStep extendStep,
            Map<Integer, Integer> srcToTargetOrderMapping) {
        this.extendStep = extendStep;
        this.srcPattern = srcPattern;
        this.dstPattern = dstPattern;
        this.srcToTargetOrderMapping = srcToTargetOrderMapping;
    }

    public Map<Integer, Integer> getSrcToTargetOrderMapping() {
        return srcToTargetOrderMapping;
    }

    @Override
    public Pattern getSrcPattern() {
        return srcPattern;
    }

    @Override
    public Pattern getDstPattern() {
        return dstPattern;
    }

    public ExtendStep getExtendStep() {
        return extendStep;
    }

    @Override
    public String toString() {
        return "ExtendIntersectEdge{"
                + "srcPattern="
                + srcPattern.getPatternId()
                + ", dstPattern="
                + dstPattern.getPatternId()
                + ", extendStep="
                + extendStep
                + ", srcToTargetIdMapping="
                + srcToTargetOrderMapping
                + '}';
    }
}
