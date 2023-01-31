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

package com.alibaba.graphscope.common.calcite.rel;

import com.alibaba.graphscope.common.calcite.tools.config.ScanOpt;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.commons.lang3.ObjectUtils;

import java.util.List;

public class LogicalSource extends AbstractBindableTableScan {
    private ScanOpt scanOpt;

    protected LogicalSource(RelOptCluster cluster, List<RelHint> hints, TableConfig tableConfig) {
        super(cluster, hints, tableConfig);
        this.scanOpt = getScanOpt();
    }

    public static LogicalSource create(
            RelOptCluster cluster, List<RelHint> hints, TableConfig tableConfig) {
        return new LogicalSource(cluster, hints, tableConfig);
    }

    private ScanOpt getScanOpt() {
        ObjectUtils.requireNonEmpty(hints);
        RelHint optHint = hints.get(0);
        ObjectUtils.requireNonEmpty(optHint.listOptions);
        return ScanOpt.valueOf(optHint.listOptions.get(0));
    }
}
