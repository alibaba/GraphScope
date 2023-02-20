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

package com.alibaba.graphscope.common.ir.schema;

import static com.alibaba.graphscope.common.ir.util.Static.RESOURCE;

import com.alibaba.graphscope.common.ir.tools.config.GraphOpt;
import com.alibaba.graphscope.compiler.api.exception.GraphElementNotFoundException;
import com.alibaba.graphscope.compiler.api.schema.GraphElement;
import com.alibaba.graphscope.compiler.api.schema.GraphSchema;
import com.google.common.collect.ImmutableList;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.Statistic;
import org.apache.commons.lang3.ObjectUtils;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Maintain a set of {@link RelOptTable} objects per query in compilation phase
 */
public class GraphOptSchema implements RelOptSchema {
    private RelOptCluster optCluster;
    private StatisticSchema rootSchema;
    private Map<List<String>, RelOptTable> tableMap;

    public GraphOptSchema(@Nullable RelOptCluster optCluster, StatisticSchema rootSchema) {
        this.optCluster = optCluster;
        this.rootSchema = Objects.requireNonNull(rootSchema);
        this.tableMap = new ConcurrentHashMap<>();
    }

    /**
     * get all table names for a specific {@code opt} to handle fuzzy conditions, i.e. g.V()
     * @param opt
     * @return
     */
    public List<List<String>> getTableNames(GraphOpt.Source opt) {
        switch (opt) {
            case VERTEX:
                return rootSchema.getVertexList().stream()
                        .map(k -> ImmutableList.of(k.getLabel()))
                        .collect(Collectors.toList());
            case EDGE:
            default:
                return rootSchema.getEdgeList().stream()
                        .map(k -> ImmutableList.of(k.getLabel()))
                        .collect(Collectors.toList());
        }
    }

    /**
     * @param tableName name of an entity or a relation,
     *                  i.e. the name of an entity can be denoted by ["person"]
     *                  and the name of a relation can be denoted by ["knows"]
     * @return
     * @throws Exception - if the given table is not found
     */
    @Override
    public RelOptTable getTableForMember(List<String> tableName) {
        ObjectUtils.requireNonEmpty(tableName);
        String labelName = tableName.get(0);
        try {
            GraphElement element = rootSchema.getElement(labelName);
            return createRelOptTable(tableName, element, rootSchema.getStatistic(tableName));
        } catch (GraphElementNotFoundException e) {
            throw RESOURCE.tableNotFound(labelName).ex();
        }
    }

    private RelOptTable createRelOptTable(
            List<String> tableName, GraphElement element, Statistic statistic) {
        return new GraphOptTable(this, tableName, element, statistic);
    }

    /**
     * {@link RelDataTypeFactory} provides interfaces to create {@link org.apache.calcite.rel.type.RelDataType} in Calcite
     *
     * @return
     */
    @Override
    public RelDataTypeFactory getTypeFactory() {
        return this.optCluster.getTypeFactory();
    }

    public GraphSchema getRootSchema() {
        return this.rootSchema;
    }

    // used in optimizer

    @Override
    public void registerRules(RelOptPlanner relOptPlanner) {
        throw RESOURCE.functionWillImplement(this.getClass()).ex();
    }
}
