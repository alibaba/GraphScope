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

package com.alibaba.graphscope.common.calcite.tools.config;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Query given labels of a vertex or an edge
 */
public class LabelConfig {
    public static final LabelConfig DEFAULT = new LabelConfig(true);

    private List<String> labels;
    private boolean isAll;

    public LabelConfig(boolean isAll) {
        this.isAll = isAll;
        this.labels = new ArrayList<>();
    }

    public LabelConfig addLabel(String label) {
        this.labels.add(label);
        return this;
    }

    public List<String> getLabels() {
        return Collections.unmodifiableList(labels);
    }

    public boolean isAll() {
        return isAll;
    }
}
