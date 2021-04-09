package com.alibaba.maxgraph.tests.gremlin;

import com.alibaba.maxgraph.v2.MaxNode;
import com.alibaba.maxgraph.v2.common.NodeBase;
import com.alibaba.maxgraph.v2.common.config.CommonConfig;
import com.alibaba.maxgraph.v2.common.config.Configs;
import com.alibaba.maxgraph.v2.common.config.StoreConfig;
import com.alibaba.maxgraph.v2.common.exception.MaxGraphException;
import com.alibaba.maxgraph.v2.common.frontend.api.io.MaxGraphIORegistry;
import com.alibaba.maxgraph.v2.frontend.config.FrontendConfig;
import com.alibaba.maxgraph.v2.frontend.graph.MaxGraphTraversalSource;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.MessageSerializer;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.driver.ser.GryoMessageSerializerV1d0;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.remote.RemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.finalization.ProfileStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.FilterRankingStrategy;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.Properties;

@Graph.OptIn(Graph.OptIn.SUITE_PROCESS_STANDARD)

@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.branch.BranchTest",
        method = "g_V_branchXlabel_eq_person__a_bX_optionXa__ageX_optionXb__langX_optionXb__nameX",
        reason = "MaxGraph not support lambda")

@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.branch.ChooseTest",
        method = "g_V_chooseXlabel_eqXpersonX__outXknowsX__inXcreatedXX_name",
        reason = "MaxGraph not support lambda")

@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.branch.LocalTest",
        method = "g_V_localXpropertiesXlocationX_order_byXvalueX_limitX2XX_value",
        reason = "MaxGraph not support multiple property with the same name")

@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.branch.RepeatTest",
        method = "g_V_repeatXbothX_untilXname_eq_marko_or_loops_gt_1X_groupCount_byXnameX",
        reason = "MaxGraph not support lambda")

@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.DedupTest",
        method = "g_V_both_name_order_byXa_bX_dedup_value",
        reason = "MaxGraph not support lambda temporarily")

@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.FilterTest",
        method = "*",
        reason = "MaxGraph not support lambda temporarily")

@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasTest",
        method = "g_V_outXcreatedX_hasXname__mapXlengthX_isXgtX3XXX_name",
        reason = "MaxGraph not support labmda temporarily")

@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.DropTest",
        method = "g_V_properties_drop",
        reason = "MaxGraph cant delete vertex primary key properties")

@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasTest",
        method = "g_VX1X_out_hasXid_lt_3X",
        reason = "MaxGraph cant compare system id while tinkerpop compare id value")

@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.MapTest",
        method = "g_VX1X_mapXnameX",
        reason = "MaxGraph not support labmda temporarily")

@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.MapTest",
        method = "g_VX1X_outE_label_mapXlengthX",
        reason = "MaxGraph not support labmda temporarily")

@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.MapTest",
        method = "g_withPath_V_asXaX_out_mapXa_nameX",
        reason = "MaxGraph not support labmda temporarily")

@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.MapTest",
        method = "g_VX1X_out_mapXnameX_mapXlengthX",
        reason = "MaxGraph not support labmda temporarily")

@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.MapTest",
        method = "g_withPath_V_asXaX_out_out_mapXa_name_it_nameX",
        reason = "MaxGraph not support labmda temporarily")

@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.OrderTest",
        method = "g_VX1X_hasXlabel_personX_mapXmapXint_ageXX_orderXlocalX_byXvalues_descX_byXkeys_ascX",
        reason = "MaxGraph not support lambda temporarily")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.OrderTest",
        method = "g_V_name_order_byXa1_b1X_byXb2_a2X",
        reason = "MaxGraph not support lambda temporarily")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.OrderTest",
        method = "g_V_hasLabelXpersonX_order_byXvalueXageX_descX_name",
        reason = "MaxGraph not support lambda temporarily")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.OrderTest",
        method = "g_V_order_byXname_a1_b1X_byXname_b2_a2X_name",
        reason = "MaxGraph not support lambda temporarily")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.OrderTest",
        method = "g_V_both_hasLabelXpersonX_order_byXage_descX_name",
        reason = "Ignore by stand test")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.OrderTest",
        method = "g_V_properties_order_byXkey_descX_key",
        reason = "MaxGraph Properties keys has id property")

@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.UnfoldTest",
        method = "g_V_valueMap_unfold_mapXkeyX",
        reason = "MaxGraph not support lambda temporarily")

@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.UnfoldTest",
        method = "g_VX1X_repeatXboth_simplePathX_untilXhasIdX6XX_path_byXnameX_unfold",
        reason = "MaxGraph not support compare system id to primary key id")

@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GroupTest",
        method = "g_V_group_byXname_substring_1X_byXconstantX1XX",
        reason = "MaxGraph not support lambda temporarily")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GroupTest",
        method = "g_V_groupXaX_byXname_substring_1X_byXconstantX1XX_capXaX",
        reason = "MaxGraph not support lambda temporarily")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GroupTest",
        method = "g_V_hasLabelXsongX_groupXaX_byXnameX_byXproperties_groupCount_byXlabelXX_out_capXaX",
        reason = "MaxGraph properties has primary key id property")

@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.SackTest",
        method = "g_withSackXmap__map_cloneX_V_out_out_sackXmap_a_nameX_sack",
        reason = "MaxGraph not support lambda temporarily")

@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.branch.UnionTest",
        method = "g_VX1_2X_unionXoutE_count__inE_count__outE_weight_sumX",
        reason = "MaxGraph deal count in union will output 0, it should be correct")

@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GroupTest",
        method = "g_V_hasLabelXsongX_group_byXnameX_byXproperties_groupCount_byXlabelXX",
        reason = "MaxGraph properties has primary key id")

@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.ValueMapTest",
        method = "g_VX1X_valueMapXname_locationX_byXunfoldX_by",
        reason = "MaxGraph properties not support multiple value for one property")

@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.InjectTest",
        method = "g_VX1X_out_name_injectXdanielX_asXaX_mapXlengthX_path",
        reason = "MaxGraph not support lambda temporarily")

@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.SubgraphTest",
        method = "*",
        reason = "MaxGraph not support lambda temporarily")

@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.AddVertexTest",
        method = "g_addVXpersonX_propertyXname_stephenX",
        reason = "MaxGraph add vertex will generate primary key property if it is not set")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.AddVertexTest",
        method = "g_V_asXaX_hasXname_markoX_outXcreatedX_asXbX_addVXselectXaX_labelX_propertyXtest_selectXbX_labelX_valueMap_withXtokensX",
        reason = "MaxGraph add vertex will generate primary key property if it is not set")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.AddVertexTest",
        method = "g_addVXpersonX_propertyXsingle_name_stephenX_propertyXsingle_name_stephenmX",
        reason = "MaxGraph add vertex will generate primary key property if it is not set")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.AddVertexTest",
        method = "g_addVXanimalX_propertyXname_mateoX_propertyXname_gateoX_propertyXname_cateoX_propertyXage_5X",
        reason = "MaxGraph add vertex will generate primary key property if it is not set")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.AddVertexTest",
        method = "g_V_addVXanimalX_propertyXname_valuesXnameXX_propertyXname_an_animalX_propertyXvaluesXnameX_labelX",
        reason = "MaxGraph not support add multiple value in one property")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.AddVertexTest",
        method = "g_addVXpersonX_propertyXsingle_name_stephenX_propertyXsingle_name_stephenm_since_2010X",
        reason = "MaxGraph not support add meta info to property")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.AddVertexTest",
        method = "g_V_hasXname_markoX_propertyXfriendWeight_outEXknowsX_weight_sum__acl_privateX",
        reason = "MaxGraph not support add meta info to property")

@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.SideEffectTest",
        method = "*",
        reason = "MaxGraph not support sideEffect with lambda and Supplier")

@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.SelectTest",
        method = "g_V_asXa_bX_out_asXcX_path_selectXkeysX",
        reason = "MaxGraph will return list for path.select(keys) while tinkerpop return set value")

@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.CoreTraversalTest",
        method = "shouldNotAlterTraversalAfterTraversalBecomesLocked",
        reason = "Test case should be ignore in standard cases")

@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.PropertiesTest",
        method = "g_V_hasXageX_properties_hasXid_nameIdX_value",
        reason = "The same property name in MaxGraph has the same propid")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.PropertiesTest",
        method = "g_V_hasXageX_properties_hasXid_nameIdAsStringX_value",
        reason = "The same property name in MaxGraph has the same propid")

@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.AddEdgeTest",
        method = "g_addV_asXfirstX_repeatXaddEXnextX_toXaddVX_inVX_timesX5X_addEXnextX_toXselectXfirstXX",
        reason = "MaxGraph in classic graph has vertex and the count is wrong if more vertex is added")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.AddEdgeTest",
        method = "g_V_aggregateXxX_asXaX_selectXxX_unfold_addEXexistsWithX_toXaX_propertyXtime_nowX",
        reason = "MaxGraph in classic graph has vertex and the count is wrong if more vertex is added")

@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.ElementIdStrategyProcessTest",
        method = "*",
        reason = "MaxGraph not support generate id be given strategy")

@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.ReadTest",
        method = "*",
        reason = "MaxGraph not support read total graph data from file")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.WriteTest",
        method = "*",
        reason = "MaxGraph not support write total graph data from file")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.EarlyLimitStrategyProcessTest",
        method = "*",
        reason = "MaxGraph not support EarlyLimitStrategy yet")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.TraversalInterruptionTest",
        method = "*",
        reason = "MaxGraph not support TraversalInterruption temporarily")

@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.ProfileTest",
        method = "*",
        reason = "MaxGraph not support Profile temporarily")

@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.CoreTraversalTest",
        method = "shouldTraverseIfAutoTxEnabledAndOriginalTxIsClosed",
        reason = "MaxGraph not support transaction temporarily")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.CoreTraversalTest",
        method = "shouldFilterOnIterate",
        reason = "MaxGraph not support temporarily")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.CoreTraversalTest",
        method = "shouldTraverseIfManualTxEnabledAndOriginalTxIsClosed",
        reason = "MaxGraph not support temporarily")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.CoreTraversalTest",
        method = "shouldNeverPropagateANoBulkTraverser",
        reason = "MaxGraph not support temporarily")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.CoreTraversalTest",
        method = "shouldNeverPropagateANullValuedTraverser",
        reason = "MaxGraph not support temporarily")

@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.ValueMapTest",
        method = "g_VX1X_outXcreatedX_valueMap",
        reason = "MaxGraph will have id property in value map result")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.ValueMapTest",
        method = "g_V_valueMap_withXtokensX",
        reason = "MaxGraph will have id property in value map result")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.ValueMapTest",
        method = "g_V_valueMap",
        reason = "MaxGraph will have id property in value map result")

@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.AddEdgeTest",
        method = "g_withSideEffectXb_bX_VXaX_addEXknowsX_toXbX_propertyXweight_0_5X",
        reason = "MaxGraph will return edge's id property even when id property value is not setted")

@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.EventStrategyProcessTest",
        method = "*",
        reason = "MaxGraph not support Transaction temporarily")

@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.ReadOnlyStrategyProcessTest",
        method = "*",
        reason = "MaxGraph not support ReadOnlyStrategy temporarily")

@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.PartitionStrategyProcessTest",
        method = "*",
        reason = "MaxGraph not support PartitionStrategy temporarily")

@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GroupTest",
        method = "g_V_groupXaX_byXnameX_capXaX",
        reason = "MaxGraph not support save remote side effect in gremlin server")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GroupTest",
        method = "g_V_repeatXunionXoutXknowsX_groupXaX_byXageX__outXcreatedX_groupXbX_byXnameX_byXcountXX_groupXaX_byXnameXX_timesX2X_capXa_bX",
        reason = "MaxGraph not support save remote side effect in gremlin server")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GroupTest",
        method = "g_V_groupXmX_byXnameX_byXinXknowsX_nameX_capXmX",
        reason = "MaxGraph not support save remote side effect in gremlin server")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GroupTest",
        method = "g_V_repeatXbothXfollowedByXX_timesX2X_groupXaX_byXsongTypeX_byXcountX_capXaX",
        reason = "MaxGraph not support save remote side effect in gremlin server")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GroupTest",
        method = "g_withSideEffectXa__marko_666_noone_blahX_V_groupXaX_byXnameX_byXoutE_label_foldX_capXaX",
        reason = "MaxGraph not support save remote side effect in gremlin server")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GroupTest",
        method = "g_V_outXfollowedByX_group_byXsongTypeX_byXbothE_group_byXlabelX_byXweight_sumXX",
        reason = "MaxGraph not support save remote side effect in gremlin server")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GroupTest",
        method = "g_V_group_byXlabelX_byXbothE_groupXaX_byXlabelX_byXweight_sumX_weight_sumX",
        reason = "MaxGraph not support save remote side effect in gremlin server")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GroupTest",
        method = "g_V_hasXlangX_groupXaX_byXlangX_byXnameX_out_capXaX",
        reason = "MaxGraph not support save remote side effect in gremlin server")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GroupTest",
        method = "g_V_groupXaX_byXlabelX_byXoutE_weight_sumX_capXaX",
        reason = "MaxGraph not support save remote side effect in gremlin server")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GroupTest",
        method = "g_V_repeatXout_groupXaX_byXnameX_byXcountX_timesX2X_capXaX",
        reason = "MaxGraph not support save remote side effect in gremlin server")

@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GroupCountTest",
        method = "g_V_unionXrepeatXoutX_timesX2X_groupCountXmX_byXlangXX__repeatXinX_timesX2X_groupCountXmX_byXnameXX_capXmX",
        reason = "MaxGraph not support save remote side effect in gremlin server")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GroupCountTest",
        method = "g_V_repeatXout_groupCountXaX_byXnameXX_timesX2X_capXaX",
        reason = "MaxGraph not support save remote side effect in gremlin server")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GroupCountTest",
        method = "g_V_outXcreatedX_groupCountXxX_capXxX",
        reason = "MaxGraph not support save remote side effect in gremlin server")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GroupCountTest",
        method = "g_V_both_groupCountXaX_out_capXaX_selectXkeysX_unfold_both_groupCountXaX_capXaX",
        reason = "MaxGraph not support save remote side effect in gremlin server")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GroupCountTest",
        method = "g_V_outXcreatedX_groupCountXaX_byXnameX_capXaX",
        reason = "MaxGraph not support save remote side effect in gremlin server")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GroupCountTest",
        method = "g_V_outXcreatedX_name_groupCountXaX_capXaX",
        reason = "MaxGraph not support save remote side effect in gremlin server")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GroupCountTest",
        method = "g_V_both_groupCountXaX_byXlabelX_asXbX_barrier_whereXselectXaX_selectXsoftwareX_isXgtX2XXX_selectXbX_name",
        reason = "MaxGraph not support save remote side effect in gremlin server")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GroupCountTest",
        method = "g_V_hasXnoX_groupCountXaX_capXaX",
        reason = "MaxGraph not support save remote side effect in gremlin server")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.ComplexTest",
        method = "classicRecommendation",
        reason = "MaxGraph not support save remote side effect in gremlin server")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.SubgraphStrategyProcessTest",
        method = "*",
        reason = "MaxGraph not support SubgraphStrategy temporarily")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.branch.OptionalTest",
        method = "g_VX1X_optionalXaddVXdogXX_label",
        reason = "MaxGraph only allow vertex data with primary keys")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.AddVertexTest",
        method = "g_VX1X_addVXanimalX_propertyXage_selectXaX_byXageXX_propertyXname_puppyX",
        reason = "MaxGraph only allow vertex data with primary keys")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.AddVertexTest",
        method = "g_withSideEffectXa_nameX_addV_propertyXselectXaX_markoX_name",
        reason = "MaxGraph only allow vertex data with primary keys")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.AddVertexTest",
        method = "g_addVXV_hasXname_markoX_propertiesXnameX_keyX_label",
        reason = "MaxGraph only allow vertex data with primary keys")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.AddVertexTest",
        method = "g_V_addVXanimalX_propertyXage_0X",
        reason = "MaxGraph only allow vertex data with primary keys")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.AddVertexTest",
        method = "g_withSideEffectXa_markoX_addV_propertyXname_selectXaXX_name",
        reason = "MaxGraph only allow vertex data with primary keys")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.branch.RepeatTest",
        method = "g_VX3X_repeatXbothX_createdXX_untilXloops_is_40XXemit_repeatXin_knowsXX_emit_loopsXisX1Xdedup_values",
        reason = "This is a TODO bug")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.LoopsTest",
        method = "g_VX1X_repeatXboth_simplePathX_untilXhasXname_peterX_or_loops_isX2XX_hasXname_peterX_path_byXnameX",
        reason = "Failed in docker env, optout for now")
public class MaxTestGraph implements Graph {
    private static final Logger logger = LoggerFactory.getLogger(MaxTestGraph.class);

    public static MaxTestGraph INSTANCE = null;

    private NodeBase maxNode;
    private RemoteConnection remoteConnection;
    private Cluster cluster;

    public static final String GRAPH_CONFIG_FILE = "graph.config.file";
    public static final String GRAPH_DATA_DIR = "graph.data.dir";

    public MaxTestGraph(Configs configs) {
        try {
            this.maxNode = new MaxNode(configs);
            this.maxNode.start();
            int port = FrontendConfig.GREMLIN_PORT.get(configs);
            this.cluster = createCluster("localhost", port);
            this.remoteConnection = DriverRemoteConnection.using(cluster);
        } catch (Throwable e) {
            this.closeGraph();
            throw new MaxGraphException(e);
        }
    }

    public static MaxTestGraph open(final Configuration conf) throws Exception {
        if (INSTANCE == null) {
            logger.info("open new MaxTestGraph");
            String configFile = conf.getString(GRAPH_CONFIG_FILE);
            Properties properties = new Properties();
            try (InputStream ins = Thread.currentThread().getContextClassLoader().getResourceAsStream(configFile)) {
                properties.load(ins);
            }
            String log4rsPath =
                    Paths.get(Thread.currentThread().getContextClassLoader().getResource("log4rs.yml").toURI()).toString();
            Configs configs = Configs.newBuilder(properties)
                    .put(StoreConfig.STORE_DATA_PATH.getKey(), conf.getString(GRAPH_DATA_DIR))
                    .put(CommonConfig.LOG4RS_CONFIG.getKey(), log4rsPath)
                    .build();
            INSTANCE = new MaxTestGraph(configs);
        }
        return INSTANCE;
    }

    private Cluster createCluster(String ip, int port) {
        GryoMapper.Builder kryo = GryoMapper.build().addRegistry(MaxGraphIORegistry.instance());
        MessageSerializer serializer = new GryoMessageSerializerV1d0(kryo);
        return Cluster.build()
                .maxContentLength(65536000)
                .addContactPoint(ip)
                .port(port)
                .keepAliveInterval(60000)
                .serializer(serializer)
                .create();
    }

    private void dropData() {
        Client client = this.cluster.connect();
        try {
            logger.info("drop schema: " + client.submit("graph.dropSchema()").one().getString());
        } finally {
            client.close();
        }
    }

    public void loadSchema(LoadGraphWith.GraphData graphData) throws URISyntaxException, IOException {
        String schemaResource = graphData.name().toLowerCase() + ".schema";
        byte[] schemaJsonBytes = Files.readAllBytes(Paths.get(
                Thread.currentThread().getContextClassLoader().getResource(schemaResource).toURI()));
        Client client = this.cluster.connect();
        logger.info("load json schema: " +
                client.submit("graph.loadJsonSchema(\"" + Hex.encodeHexString(schemaJsonBytes) + "\")").one().getString());
        client.close();
    }

    @Override
    public GraphTraversalSource traversal() {
        MaxGraphTraversalSource source = AnonymousTraversalSource.traversal(MaxGraphTraversalSource.class)
                .withRemote(remoteConnection);
        source.getStrategies().removeStrategies(ProfileStrategy.class, FilterRankingStrategy.class);
        return source;
    }

    @Override
    public Vertex addVertex(Object... keyValues) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <C extends GraphComputer> C compute(Class<C> graphComputerClass) throws IllegalArgumentException {
        throw new UnsupportedOperationException();
    }

    @Override
    public GraphComputer compute() throws IllegalArgumentException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Iterator<Vertex> vertices(Object... vertexIds) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Iterator<Edge> edges(Object... edgeIds) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Transaction tx() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() {
        dropData();
    }

    private void closeGraph() {
        logger.info("close MaxTestGraph");
        if (this.remoteConnection != null) {
            try {
                this.remoteConnection.close();
            } catch (Exception e) {
                logger.error("close remote connection failed", e);
            }
        }
        if (this.cluster != null) {
            this.cluster.close();
        }
        if (this.maxNode != null) {
            try {
                this.maxNode.close();
            } catch (IOException e) {
                logger.error("close maxNode failed", e);
            }
        }
    }

    @Override
    public Variables variables() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Configuration configuration() {
        return null;
    }
}
