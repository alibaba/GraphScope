package com.alibaba.graphscope.gaia;

import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.process.remote.RemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.finalization.ProfileStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.FilterRankingStrategy;
import org.apache.tinkerpop.gremlin.structure.Graph;

@Graph.OptIn("com.alibaba.graphscope.gaia.GaiaGremlinTestSuite")


//@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.branch.RepeatTest",
//        method = "g_V_repeatXoutXknowsXX_untilXrepeatXoutXcreatedXX_emitXhasXname_lopXXX_path_byXnameX",
//        reason = "not support until or emit in repeat")
//
//@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.branch.RepeatTest",
//        method = "g_V_repeatXoutX_timesX2X_emit_path",
//        reason = "not support until or emit in repeat")
//
//@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.branch.RepeatTest",
//        method = "g_V_repeatXout_repeatXoutX_timesX1XX_timesX1X_limitX1X_path_by_name",
//        reason = "not support path by")
//
//@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.branch.RepeatTest",
//        method = "g_VX1X_repeatXoutX_untilXoutE_count_isX0XX_name",
//        reason = "not support until or emit in repeat")
//
//@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.branch.RepeatTest",
//        method = "g_V_repeatXbothX_untilXname_eq_marko_or_loops_gt_1X_groupCount_byXnameX",
//        reason = "not support until in repeat and lambda")
//
//@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.branch.RepeatTest",
//        method = "g_VX1X_repeatXgroupCountXmX_byXloopsX_outX_timesX3X_capXmX",
//        reason = "not support cap")
//
//@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.branch.RepeatTest",
//        method = "g_VX1X_emitXhasXlabel_personXX_repeatXoutX_name",
//        reason = "not support emit")
//
//@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.branch.RepeatTest",
//        method = "g_V_hasXname_markoX_repeatXoutE_inV_simplePathX_untilXhasXname_rippleXX_path_byXnameX_byXlabelX",
//        reason = "not support until")
//
//@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.branch.RepeatTest",
//        method = "g_V_untilXconstantXtrueXX_repeatXrepeatXout_createdXX_untilXhasXname_rippleXXXemit_lang",
//        reason = "not support until")
//
//@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.branch.RepeatTest",
//        method = "g_VX6X_repeatXa_bothXcreatedX_simplePathX_emitXrepeatXb_bothXknowsXX_untilXloopsXbX_asXb_whereXloopsXaX_asXbX_hasXname_vadasXX_dedup_name",
//        reason = "not support emit")
//
//@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.branch.RepeatTest",
//        method = "g_VX3X_repeatXbothX_createdXX_untilXloops_is_40XXemit_repeatXin_knowsXX_emit_loopsXisX1Xdedup_values",
//        reason = "not support emit")
//
//@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.branch.RepeatTest",
//        method = "g_V_hasXloop_name_loopX_repeatXinX_timesX5X_path_by_name",
//        reason = "not support path by")
//
//@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.branch.RepeatTest",
//        method = "g_V_repeatXgroupCountXmX_byXnameX_outX_timesX2X_capXmX",
//        reason = "not support cap")
//
//@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.branch.RepeatTest",
//        method = "g_V_repeatXrepeatXout_createdXX_untilXhasXname_rippleXXXemit_lang",
//        reason = "not support until")
//
//@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.branch.RepeatTest",
//        method = "g_V_emit_repeatXa_outXknows_filterXloops_isX0XX_lang",
//        reason = "not support repeat with loop name")
//
//@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.branch.UnionTest",
//        method = "g_VX1_2X_localXunionXoutE_count__inE_count__outE_weight_sumXX",
//        reason = "not support local")
//
//@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.branch.UnionTest",
//        method = "g_V_chooseXlabel_is_person__unionX__out_lang__out_nameX__in_labelX",
//        reason = "not support choose")
//
//@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.branch.UnionTest",
//        method = "g_VX1_2X_localXunionXcountXX",
//        reason = "not support local")
//
//@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.branch.UnionTest",
//        method = "g_V_chooseXlabel_is_person__unionX__out_lang__out_nameX__in_labelX_groupCount",
//        reason = "not support choose")
//
//@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.branch.UnionTest",
//        method = "g_VX1_2X_unionXoutE_count__inE_count__outE_weight_sumX",
//        reason = "not support sum")
//
//@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.CyclicPathTest",
//        method = "g_VX1X_asXaX_outXcreatedX_asXbX_inXcreatedX_asXcX_cyclicPath_fromXaX_toXbX_path",
//        reason = "not support path from to")
//
//@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.DedupTest",
//        method = "g_V_both_name_order_byXa_bX_dedup_value",
//        reason = "not support lambda")
//
//@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.FilterTest",
//        method = "g_V_filterXfalseX",
//        reason = "not support lambda")
//
//@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.FilterTest",
//        method = "g_VX1X_out_filterXage_gt_30X",
//        reason = "not support lambda")
//
//@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.FilterTest",
//        method = "g_E_filterXfalseX",
//        reason = "not support lambda")
//
//@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.FilterTest",
//        method = "g_VX1X_filterXage_gt_30X",
//        reason = "not support lambda")
//
//@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.FilterTest",
//        method = "g_V_filterXname_startsWith_m_OR_name_startsWith_pX",
//        reason = "not support lambda")
//
//@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.FilterTest",
//        method = "g_V_filterXtrueX",
//        reason = "not support lambda")
//
//@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.FilterTest",
//        method = "g_E_filterXtrueX",
//        reason = "not support lambda")
//
//@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.FilterTest",
//        method = "g_V_filterXlang_eq_javaX",
//        reason = "not support lambda")
//
//@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.FilterTest",
//        method = "g_VX2X_filterXage_gt_30X",
//        reason = "not support lambda")

//@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GroupTest",
//        method = "g_V_outXfollowedByX_group_byXsongTypeX_byXbothE_group_byXlabelX_byXweight_sumXX",
//        reason = "not support sum")
//@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.branch.RepeatTest",
//        method = "g_V_repeatXoutXknowsXX_untilXrepeatXoutXcreatedXX_emitXhasXname_lopXXX_path_byXnameX",
//        reason = "no reason")
public class RemoteTestGraph extends DummyGraph {
    public static final String GRAPH_NAME = "test.graph.name";
    private RemoteGremlinConnection remoteGremlinConnection;
    private Cluster cluster;
    private Client client;

    public RemoteTestGraph(final Configuration configuration) {
        try {
            String gremlinEndpoint = configuration.getString(GRAPH_NAME);
            cluster = RemoteGremlinConnection.createCluster(gremlinEndpoint);
            client = cluster.connect();
            // remoteGremlinConnection = new RemoteGremlinConnection(gremlinEndpoint);
        } catch (Exception e) {
            throw new RuntimeException("initiate remote test graph fail " + e);
        }
    }

    public static RemoteTestGraph open(final Configuration configuration) {
        return new RemoteTestGraph(configuration);
    }

    @Override
    public void close() throws Exception {
        // remoteGremlinConnection.close();
        client.close();
        cluster.close();
    }

    @Override
    public Configuration configuration() {
        return null;
    }

    @Override
    public GraphTraversalSource traversal() {
        GraphTraversalSource graphTraversalSource = new GraphTraversalSource(
                this,
                TraversalStrategies.GlobalCache.getStrategies(this.getClass())).withRemote(DriverRemoteConnection.using(client));
        TraversalStrategies strategies = graphTraversalSource.getStrategies();
        strategies.removeStrategies(
                ProfileStrategy.class,
                FilterRankingStrategy.class);
        return graphTraversalSource;
    }
}
