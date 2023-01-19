package com.alibaba.graphscope.common.calcite.rel.builder;

import static org.apache.calcite.util.Static.RESOURCE;

import com.alibaba.graphscope.common.calcite.rel.*;
import com.alibaba.graphscope.common.calcite.rel.builder.config.*;
import com.alibaba.graphscope.common.calcite.rex.RexGraphVariable;
import com.alibaba.graphscope.common.calcite.schema.type.GraphSchemaType;
import com.google.common.collect.ImmutableList;

import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.ObjectUtils;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.List;

/**
 * integrate interfaces to build algebra structures,
 * including {@link RexNode} for expressions and {@link RelNode} for operators
 */
public class GraphBuilder extends RelBuilder {
    /**
     * @param context      not used currently
     * @param cluster      get {@link org.apache.calcite.rex.RexBuilder} (to build literal)
     *                     and other global resources (not used currently) from it
     * @param relOptSchema get graph schema from it
     */
    protected GraphBuilder(
            @Nullable Context context, RelOptCluster cluster, RelOptSchema relOptSchema) {
        super(context, cluster, relOptSchema);
    }

    /**
     * @param config integrate all configs the IrBuilder needs in this structure
     * @return
     */
    public static GraphBuilder create(FrameworkConfig config) {
        return Frameworks.withPrepare(
                config,
                (cluster, relOptSchema, rootSchema, statement) -> {
                    return new GraphBuilder(config.getContext(), cluster, relOptSchema);
                });
    }

    /**
     * validate and build an algebra structure of {@link LogicalSource},
     * how to validate:
     * validate the existence of the given labels in config,
     * if exist then derive the {@link GraphSchemaType} of the given labels and keep the type in {@link RelNode#getRowType()}
     *
     * @param config
     * @return
     */
    public GraphBuilder source(SourceConfig config) {
        LabelConfig labelConfig = config.getLabels();
        switch (labelConfig.getType()) {
            case SINGLE:
            case PARTIAL:
                ObjectUtils.requireNonEmpty(labelConfig.getLabels());
                List<RelOptTable> tables = new ArrayList<>();
                for (String label : labelConfig.getLabels()) {
                    RelOptTable table = relOptSchema.getTableForMember(ImmutableList.of(label));
                    if (table == null) {
                        throw RESOURCE.tableNotFound(label).ex();
                    }
                    tables.add(table);
                }
                RelNode source = LogicalSource.create(cluster, getHints(config), tables);
                push(source);
                return this;
            default:
                throw new NotImplementedException("");
        }
    }

    private List<RelHint> getHints(SourceConfig config) {
        return null;
    }

    private List<RelHint> getHints(ExpandConfig config) {
        return null;
    }

    private List<RelHint> getHints(GetVConfig config) {
        return null;
    }

    /**
     * validate and build an algebra structure of {@link LogicalExpand}
     *
     * @param config
     * @return
     */
    public GraphBuilder expand(ExpandConfig config) {
        return null;
    }

    /**
     * validate and build an algebra structure of {@link LogicalGetV}
     *
     * @param config
     * @return
     */
    public GraphBuilder getV(GetVConfig config) {
        return null;
    }

    /**
     * build an algebra structure of {@link LogicalPathExpand}
     *
     * @param expandV expand with getV
     * @param offset
     * @param fetch
     * @return
     */
    public GraphBuilder pathExpand(RelNode expandV, int offset, int fetch) {
        return null;
    }

    /**
     * validate and build an algebra structure of {@link LogicalMatch},
     * make sure there exists {@code num} sentences in current context.
     * how to validate:
     * check the graph pattern (lookup from the graph schema and check whether the links are all valid) denoted by each sentence one by one.
     *
     * @param sentences
     * @param opts
     * @return
     */
    public GraphBuilder match(Iterable<? extends RelNode> sentences, Iterable<MatchOpt> opts) {
        return null;
    }

    //    public void test() {
    //        IrBuilder builder = new IrBuilder();
    //        RelNode sentence1 = builder.source().expand().getV().build();
    //        RelNode sentence2 = builder.source().expand().getV().build();
    //        builder.match(Arrays.asList(sentence1, sentence2),
    //                Arrays.asList(MatchOpt.INNER, MatchOpt.OPTIONAL)); // sentence1 -RIGHT_JOIN->
    // sentence2
    //    }

    /**
     * validate and build {@link RexLiteral} from a given constant
     *
     * @param value
     * @return
     */
    @Override
    public RexLiteral literal(@Nullable Object value) {
        return null;
    }

    /**
     * validate and build {@link RexGraphVariable} from a given alias (i.e. "a")
     *
     * @param alias null to denote `head` in gremlin
     * @return
     */
    public RexGraphVariable variable(@Nullable String alias) {
        return null;
    }

    /**
     * validate and build {@link RexGraphVariable} from a given variable containing fieldName (i.e. "a.name" or "name")
     *
     * @param alias
     * @param fieldName
     * @return
     */
    public RexGraphVariable variable(@Nullable String alias, String fieldName) {
        return null;
    }

    /**
     * build complex expressions denoted by {@link org.apache.calcite.rex.RexCall} from the given parameters
     *
     * @param operator provides type checker and inference
     * @param operands
     * @return
     */
    @Override
    public RexNode call(SqlOperator operator, RexNode... operands) {
        return call(operator, ImmutableList.copyOf(operands));
    }

    @Override
    public GraphBuilder filter(RexNode... conditions) {
        return null;
    }

    @Override
    public GraphBuilder project(
            Iterable<? extends RexNode> nodes,
            Iterable<? extends @Nullable String> aliases,
            boolean isAppend) {
        return null;
    }

    // build group keys

    /**
     * @param nodes   group keys (variables), complex expressions (i.e. "a.age + 1") should be projected in advance
     * @param aliases
     * @return
     */
    public GroupKey groupKey(
            List<? extends RexNode> nodes, Iterable<? extends @Nullable String> aliases) {
        return null;
    }

    // global key, i.e. g.V().count()
    @Override
    public GroupKey groupKey() {
        return null;
    }

    // build aggregate functions

    /**
     * @param distinct
     * @param alias
     * @param operands keys (variables) to aggregate on, complex expressions (i.e. sum("a.age+1")) should be projected in advance
     * @return
     */
    public AggCall collect(boolean distinct, @Nullable String alias, RexNode... operands) {
        return null;
    }

    @Override
    public AggCall count(boolean distinct, @Nullable String alias, RexNode... operands) {
        return null;
    }

    @Override
    public AggCall sum(boolean distinct, @Nullable String alias, RexNode operand) {
        return null;
    }

    @Override
    public AggCall avg(boolean distinct, @Nullable String alias, RexNode operand) {
        return null;
    }

    @Override
    public AggCall min(@Nullable String alias, RexNode operand) {
        return null;
    }

    @Override
    public AggCall max(@Nullable String alias, RexNode operand) {
        return null;
    }

    @Override
    public GraphBuilder aggregate(GroupKey groupKey, AggCall... aggCalls) {
        return null;
    }

    @Override
    public RexNode desc(RexNode expr) {
        return null;
    }

    /**
     * @param nodes order keys (variables), complex expressions (i.e. "a.age + 1") should be projected in advance
     * @return
     */
    @Override
    public GraphBuilder sort(Iterable<? extends RexNode> nodes) {
        return sortLimit(-1, -1, nodes);
    }

    // to fuse order()+limit()
    @Override
    public GraphBuilder sortLimit(int offset, int fetch, Iterable<? extends RexNode> nodes) {
        return null;
    }

    @Override
    public RelBuilder limit(int offset, int fetch) {
        return null;
    }

    @Override
    public RelNode build() {
        return null;
    }
}
