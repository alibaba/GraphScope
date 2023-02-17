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

package com.alibaba.graphscope.common.ir.planner;

import org.apache.calcite.plan.*;
import org.apache.calcite.rel.RelNode;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;
import java.util.Objects;

/**
 *  A heuristic implementation of the {@link RelOptPlanner} to optimize graph relation
 */
public class GraphHepPlanner extends AbstractRelOptPlanner {
    public static GraphHepPlanner DEFAULT = new GraphHepPlanner(RelOptCostImpl.FACTORY, null);

    private RelNode root;

    public GraphHepPlanner(RelOptCostFactory costFactory, @Nullable Context context) {
        super(costFactory, context);
    }

    @Override
    public void setRoot(RelNode relNode) {
        Objects.requireNonNull(relNode);
        this.root = relNode;
    }

    @Override
    public @Nullable RelNode getRoot() {
        return this.root;
    }

    /**
     * entrypoint to execute optimization rules
     * @return
     */
    @Override
    public RelNode findBestExp() {
        return null;
    }

    @Override
    public boolean addRule(RelOptRule rule) {
        return false;
    }

    @Override
    public boolean removeRule(RelOptRule rule) {
        return false;
    }

    @Override
    public List<RelOptRule> getRules() {
        return null;
    }

    @Override
    public RelNode changeTraits(RelNode relNode, RelTraitSet relTraitSet) {
        return relNode;
    }

    @Override
    public RelNode register(RelNode relNode, @Nullable RelNode relNode1) {
        return relNode;
    }

    @Override
    public RelNode ensureRegistered(RelNode relNode, @Nullable RelNode relNode1) {
        return relNode;
    }

    @Override
    public boolean isRegistered(RelNode relNode) {
        return true;
    }
}
