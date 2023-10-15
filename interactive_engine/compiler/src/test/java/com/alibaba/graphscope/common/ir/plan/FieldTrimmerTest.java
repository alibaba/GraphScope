package com.alibaba.graphscope.common.ir.plan;

import com.alibaba.graphscope.common.ir.Utils;
import com.alibaba.graphscope.common.ir.planner.GraphFieldTrimmer;
import com.alibaba.graphscope.common.ir.tools.GraphBuilder;
import com.alibaba.graphscope.common.ir.tools.GraphStdOperatorTable;
import com.alibaba.graphscope.common.ir.tools.config.ExpandConfig;
import com.alibaba.graphscope.common.ir.tools.config.GraphOpt;
import com.alibaba.graphscope.common.ir.tools.config.LabelConfig;
import com.alibaba.graphscope.common.ir.tools.config.SourceConfig;
import org.apache.calcite.rel.RelNode;
import org.junit.Assert;
import org.junit.Test;

public class FieldTrimmerTest {
    // Match(x:person)-[y:knows]->( ) where x.name <> "Alice"
    @Test
    public void field_trimmer_1_test() {
        GraphBuilder builder = Utils.mockGraphBuilder();
        RelNode sentence =
                builder.source(
                               new SourceConfig(
                                       GraphOpt.Source.VERTEX,
                                       new LabelConfig(false).addLabel("person"),
                                       "x"))
                       .expand(
                               new ExpandConfig(
                                       GraphOpt.Expand.OUT,
                                       new LabelConfig(false).addLabel("knows"),
                                       "y"))
                       .build();
        RelNode filter =
                builder.match(sentence, GraphOpt.Match.INNER)
                       .filter(
                               builder.call(
                                       GraphStdOperatorTable.EQUALS,
                                       builder.variable("x", "name"),
                                       builder.literal("Alice")))
                       .build();
        GraphFieldTrimmer trimmer = new GraphFieldTrimmer(builder);
        RelNode after = trimmer.trim(filter);
        Assert.assertEquals(
                "GraphLogicalSingleMatch(input=[null],"
                        + " sentence=[GraphLogicalExpand(tableConfig=[{isAll=false, tables=[knows]}],"
                        + " alias=[y], fusedFilter=[[=(DEFAULT.weight, 1.0E0)]], opt=[OUT])\n"
                        + "  GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}],"
                        + " alias=[x], opt=[VERTEX])\n"
                        + "], matchOpt=[INNER])",
                after.explain()
                     .trim());
    }
    // Match(x:person)-[y:knows]->( ) with x as p where p.name <> "Alice"
    @Test
    public void field_trimmer_2_test(){

    }


}
