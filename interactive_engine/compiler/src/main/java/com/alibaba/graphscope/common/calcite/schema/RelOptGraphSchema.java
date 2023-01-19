package com.alibaba.graphscope.common.calcite.schema;

import com.alibaba.maxgraph.compiler.api.exception.GraphElementNotFoundException;
import com.alibaba.maxgraph.compiler.api.schema.GraphElement;
import com.alibaba.maxgraph.compiler.api.schema.GraphSchema;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.Statistic;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.ObjectUtils;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * maintain a set of {@link RelOptTable} objects per query in compilation phase
 */
public class RelOptGraphSchema implements RelOptSchema {
    private RelOptCluster optCluster;
    private StatisticSchema rootSchema;
    private Map<List<String>, RelOptTable> tableMap;

    public RelOptGraphSchema(RelOptCluster optCluster, StatisticSchema rootSchema) {
        this.optCluster = optCluster;
        this.rootSchema = rootSchema;
        this.tableMap = new ConcurrentHashMap<>();
    }

    /**
     * @param tableName name of an entity or a relation,
     *                  i.e. the name of an entity can be denoted by ["person"]
     *                  and the name of a relation can be denoted by ["knows"]
     * @return return null if the given table not exist
     */
    @Override
    public @Nullable RelOptTable getTableForMember(List<String> tableName) {
        ObjectUtils.requireNonEmpty(tableName);
        String labelName = tableName.get(0);
        try {
            GraphElement element = rootSchema.getElement(labelName);
            return createRelOptTable(tableName, element, rootSchema.getStatistic(tableName));
        } catch (GraphElementNotFoundException e) {
            return null;
        }
    }

    private RelOptTable createRelOptTable(
            List<String> tableName, GraphElement element, Statistic statistic) {
        return new RelOptGraphTable(this, tableName, element, statistic);
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

    @Override
    public void registerRules(RelOptPlanner relOptPlanner) throws Exception {
        throw new NotImplementedException("");
    }

    public GraphSchema getRootSchema() {
        return this.rootSchema;
    }
}
