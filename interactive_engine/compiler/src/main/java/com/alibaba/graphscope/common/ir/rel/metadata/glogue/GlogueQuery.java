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

import com.alibaba.graphscope.common.ir.rel.metadata.glogue.pattern.*;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Set;

public class GlogueQuery {
    private Glogue glogue;

    public GlogueQuery(Glogue glogue) {
        this.glogue = glogue;
    }

    /**
     * Get in edges for the given pattern
     * @param pattern
     * @return
     */
    public Set<GlogueEdge> getInEdges(Pattern pattern) {
        return glogue.getInEdges(pattern);
    }

    /**
     * Get out edges for the given pattern
     * @param pattern
     * @return
     */
    public Set<GlogueEdge> getOutEdges(Pattern pattern) {
        return glogue.getOutEdges(pattern);
    }

    /**
     * get pattern count
     * @param pattern
     * @param allowsNull if the flag is set to true, return null when the pattern is not found, otherwise return 1.0d
     * @return
     */
    public @Nullable Double getRowCount(Pattern pattern, boolean allowsNull) {
        return glogue.getRowCount(pattern, allowsNull);
    }

    /**
     * get the max size of the preserved pattern
     * @return
     */
    public int getMaxPatternSize() {
        return glogue.getMaxPatternSize();
    }

    public Double getLabelConstraintsDeltaCost(PatternEdge edge, PatternVertex target) {
        return glogue.schema.getLabelConstraintsDeltaCost(edge, target);
    }

    @Override
    public String toString() {
        return glogue.toString();
    }
}
