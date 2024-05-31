/*
 *
 *  * Copyright 2020 Alibaba Group Holding Limited.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package com.alibaba.graphscope.common.ir.rex;

import org.apache.calcite.rex.RexNode;

import java.util.Collections;
import java.util.List;

public class ClassifiedFilter {
    private final List<RexNode> labelFilters;
    private final List<Comparable> labelValues;
    private final List<RexNode> uniqueKeyFilters;
    private final List<RexNode> extraFilters;

    public ClassifiedFilter(
            List<RexNode> labelFilters,
            List<Comparable> labelValues,
            List<RexNode> uniqueKeyFilters,
            List<RexNode> extraFilters) {
        this.labelFilters = labelFilters;
        this.labelValues = labelValues;
        this.uniqueKeyFilters = uniqueKeyFilters;
        this.extraFilters = extraFilters;
    }

    public List<RexNode> getLabelFilters() {
        return Collections.unmodifiableList(labelFilters);
    }

    public List<Comparable> getLabelValues() {
        return Collections.unmodifiableList(labelValues);
    }

    public List<RexNode> getUniqueKeyFilters() {
        return Collections.unmodifiableList(uniqueKeyFilters);
    }

    public List<RexNode> getExtraFilters() {
        return Collections.unmodifiableList(extraFilters);
    }
}
