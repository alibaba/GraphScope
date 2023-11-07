package com.alibaba.graphscope.common.ir.rel.metadata;

import com.alibaba.graphscope.common.ir.Utils;
import com.alibaba.graphscope.common.ir.rel.graph.GraphLogicalExpand;
import com.alibaba.graphscope.common.ir.tools.GraphBuilder;
import com.alibaba.graphscope.common.ir.tools.config.ExpandConfig;
import com.alibaba.graphscope.common.ir.tools.config.GraphOpt;
import com.alibaba.graphscope.common.ir.tools.config.LabelConfig;
import com.alibaba.graphscope.common.ir.tools.config.SourceConfig;

public class GlogueTest {

    public static void main(String[] args) {
        // GlogueMetadataQuery glogueMetadataQuery = new GlogueMetadataQuery();
        // g.V().hasLabel("person")

        GraphBuilder builder = Utils.mockGraphBuilder();
        GraphLogicalExpand rel =
                (GraphLogicalExpand)
                        builder.source(
                                        new SourceConfig(
                                                GraphOpt.Source.VERTEX,
                                                new LabelConfig(false).addLabel("person")))
                                .expand(
                                        new ExpandConfig(
                                                GraphOpt.Expand.OUT,
                                                new LabelConfig(false).addLabel("knows")))
                                .build();
        System.out.println(rel.explain().trim());
        // RelMetadataQuery mq = rel.getCluster().getMetadataQuery();
        // System.out.println(mq.getRowCount(rel));
        System.out.println("try to get row count...");

        //        GlogueRowCountArbitraryImpl glogueRowCountArbitrary = new
        // GlogueRowCountArbitraryImpl();
        //        System.out.println(glogueRowCountArbitrary.getRowCount(rel));
    }
}
