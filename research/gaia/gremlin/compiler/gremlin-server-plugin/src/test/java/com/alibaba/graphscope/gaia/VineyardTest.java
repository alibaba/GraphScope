package com.alibaba.graphscope.gaia;

import com.alibaba.graphscope.gaia.config.GaiaConfig;
import com.alibaba.graphscope.gaia.config.VineyardGaiaConfig;
import com.alibaba.graphscope.gaia.idmaker.IdMaker;
import com.alibaba.graphscope.gaia.idmaker.IncrementalQueryIdMaker;
import com.alibaba.graphscope.gaia.idmaker.TagIdMaker;
import com.alibaba.graphscope.gaia.plan.PlanUtils;
import com.alibaba.graphscope.gaia.plan.translator.TraversalTranslator;
import com.alibaba.graphscope.gaia.plan.translator.builder.PlanConfig;
import com.alibaba.graphscope.gaia.plan.translator.builder.TraversalBuilder;
import com.alibaba.graphscope.gaia.store.GraphStoreService;
import com.alibaba.graphscope.gaia.store.VineyardGraphStore;
import com.alibaba.maxgraph.common.cluster.InstanceConfig;
import com.alibaba.pegasus.builder.AbstractBuilder;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;

import java.util.Properties;

public class VineyardTest {
    public static void main(String[] args) {
        GaiaConfig config = new VineyardGaiaConfig(new InstanceConfig(new Properties()));
        GraphStoreService storeService = new VineyardGraphStore(null);
        IdMaker queryIdMaker = new IncrementalQueryIdMaker();
        Traversal testTraversal = QueryTest.getTestTraversal(config, storeService);
        long queryId = (long) queryIdMaker.getId(testTraversal.asAdmin());
        AbstractBuilder job = new TraversalTranslator((new TraversalBuilder(testTraversal.asAdmin()))
                .addConfig(PlanConfig.QUERY_ID, queryId)
                .addConfig(PlanConfig.TAG_ID_MAKER, new TagIdMaker(testTraversal.asAdmin()))
                .addConfig(PlanConfig.QUERY_CONFIG, PlanUtils.getDefaultConfig(queryId, config))).translate();
        PlanUtils.print(job);
    }
}
