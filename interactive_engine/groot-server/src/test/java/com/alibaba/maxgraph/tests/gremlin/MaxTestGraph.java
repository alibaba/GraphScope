/**
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.maxgraph.tests.gremlin;

import com.alibaba.graphscope.groot.sdk.MaxGraphClient;
import com.alibaba.maxgraph.common.config.CommonConfig;
import com.alibaba.maxgraph.common.config.Configs;
import com.alibaba.maxgraph.common.config.GremlinConfig;
import com.alibaba.maxgraph.compiler.api.exception.MaxGraphException;
import com.alibaba.maxgraph.sdkcommon.io.MaxGraphIORegistry;
import com.alibaba.maxgraph.servers.MaxNode;
import com.alibaba.maxgraph.servers.NodeBase;

import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
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
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

@Graph.OptIn("com.alibaba.maxgraph.tests.gremlin.GremlinStandardTestSuite")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.DedupTest",
        method = "g_V_groupCount_selectXvaluesX_unfold_dedup",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.DedupTest",
        method = "g_V_repeatXdedupX_timesX2X_count",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.DedupTest",
        method = "g_V_out_in_valuesXnameX_fold_dedupXlocalX_unfold",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.DedupTest",
        method = "g_V_group_byXlabelX_byXbothE_weight_dedup_foldX",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasTest",
        method = "g_V_hasIdXwithoutXemptyXX_count",
        reason = "unsupported")
// @Graph.OptOut(
//        test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasTest",
//        method = "g_V_hasNotXageX_name",
//        reason = "unsupported")
// @Graph.OptOut(
//        test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasTest",
//        method = "g_V_hasXname_containingXarkXX",
//        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasTest",
        method = "g_V_hasXname_endingWithXasXX",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasTest",
        method = "g_V_hasXname_startingWithXmarXX",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasTest",
        method = "g_V_hasXlocationX",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasTest",
        method = "g_V_hasXlabel_isXsoftwareXX",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasTest",
        method = "g_VX1AsStringX_out_hasXid_2AsStringX",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasTest",
        method = "g_V_outXcreatedX_hasXname__mapXlengthX_isXgtX3XXX_name",
        reason = "unsupported")
// @Graph.OptOut(
//        test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasTest",
//        method = "g_V_hasXperson_name_containingXoX_andXltXmXXX",
//        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasTest",
        method = "g_V_hasXname_not_startingWithXmarXX",
        reason = "unsupported")
// @Graph.OptOut(
//        test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasTest",
//        method = "g_V_hasXname_not_containingXarkXX",
//        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasTest",
        method = "g_V_bothE_properties_dedup_hasKeyXweightX_hasValueXltX0d3XX_value",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasTest",
        method = "g_VX1X_out_hasXid_2AsString_3AsStringX",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasTest",
        method = "g_V_hasLabelXperson_software_blahX",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasTest",
        method = "g_V_hasXname_not_endingWithXasXX",
        reason = "unsupported")
// @Graph.OptOut(
//        test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasTest",
//        method = "g_V_hasXname_gtXmX_andXcontainingXoXXX",
//        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasTest",
        method = "g_V_both_dedup_properties_hasKeyXageX_hasValueXgtX30XX_value",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasTest",
        method = "g_EX11X_outV_outE_hasXid_10X",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasTest",
        method = "g_V_notXhasIdXwithinXemptyXXX_count",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasTest",
        method = "g_VX1X_out_hasXid_lt_3X",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasTest",
        method =
                "g_V_hasLabelXpersonX_hasXage_notXlteX10X_andXnotXbetweenX11_20XXXX_andXltX29X_orXeqX35XXXX_name",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasTest",
        method = "g_V_both_dedup_properties_hasKeyXageX_value",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasTest",
        method = "g_V_bothE_properties_dedup_hasKeyXweightX_value",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasTest",
        method = "g_V_hasIdXemptyX_count",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasTest",
        method = "g_V_hasIdXwithinXemptyXX_count",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasTest",
        method = "g_E_hasLabelXuses_traversesX",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasTest",
        method = "g_EX11X_outV_outE_hasXid_10AsStringX",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.IsTest",
        method = "g_V_whereXinXcreatedX_count_isX1XX_valuesXnameX",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.IsTest",
        method = "g_V_whereXinXcreatedX_count_isXgte_2XX_valuesXnameX",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.RangeTest",
        method = "g_V_outE_valuesXweightX_fold_orderXlocalX_skipXlocal_2X",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.RangeTest",
        method =
                "g_V_asXaX_in_asXaX_in_asXaX_selectXmixed_aX_byXunfold_valuesXnameX_foldX_limitXlocal_1X",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.RangeTest",
        method =
                "g_V_asXaX_in_asXaX_in_asXaX_selectXmixed_aX_byXunfold_valuesXnameX_foldX_limitXlocal_2X",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.RangeTest",
        method = "g_V_hasLabelXpersonX_order_byXageX_valuesXnameX_skipX1X",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.RangeTest",
        method =
                "g_V_asXaX_out_asXaX_out_asXaX_selectXmixed_aX_byXunfold_valuesXnameX_foldX_rangeXlocal_1_2X",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.RangeTest",
        method =
                "g_V_asXaX_out_asXaX_out_asXaX_selectXmixed_aX_byXunfold_valuesXnameX_foldX_rangeXlocal_1_3X",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.RangeTest",
        method =
                "g_V_asXaX_out_asXaX_out_asXaX_selectXmixed_aX_byXunfold_valuesXnameX_foldX_rangeXlocal_4_5X",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.RangeTest",
        method = "g_VX1X_outXcreatedX_inXcreatedX_rangeX1_3X",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.RangeTest",
        method = "g_V_repeatXbothX_timesX3X_rangeX5_11X",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.RangeTest",
        method = "g_V_asXaX_in_asXbX_in_asXcX_selectXa_b_cX_byXnameX_limitXlocal_1X",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.RangeTest",
        method = "g_V_asXaX_in_asXbX_in_asXcX_selectXa_b_cX_byXnameX_limitXlocal_2X",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.RangeTest",
        method = "g_V_localXoutE_limitX1X_inVX_limitX3X",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.RangeTest",
        method = "g_V_hasLabelXpersonX_order_byXageX_skipX1X_valuesXnameX",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.RangeTest",
        method = "g_VX1X_outXcreatedX_inEXcreatedX_rangeX1_3X_outV",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.RangeTest",
        method = "g_V_asXaX_out_asXbX_out_asXcX_selectXa_b_cX_byXnameX_rangeXlocal_1_2X",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.RangeTest",
        method = "g_V_asXaX_out_asXbX_out_asXcX_selectXa_b_cX_byXnameX_rangeXlocal_1_3X",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.SimplePathTest",
        method = "g_V_asXaX_out_asXbX_out_asXcX_simplePath_byXlabelX_fromXbX_toXcX_path_byXnameX",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.WhereTest",
        method =
                "g_V_asXaX_out_asXbX_whereXandXasXaX_outXknowsX_asXbX__orXasXbX_outXcreatedX_hasXname_rippleX__asXbX_inXknowsX_count_isXnotXeqX0XXXXX_selectXa_bX",
        reason = "unsupported")
// @Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.WhereTest",
//        method = "g_V_hasXageX_asXaX_out_in_hasXageX_asXbX_selectXa_bX_whereXa_eqXbXX",
//        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.WhereTest",
        method =
                "g_withSideEffectXa_josh_peterX_VX1X_outXcreatedX_inXcreatedX_name_whereXwithinXaXX",
        reason = "unsupported")
// @Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.WhereTest",
//        method = "g_V_hasXageX_asXaX_out_in_hasXageX_asXbX_selectXa_bX_whereXa_outXknowsX_bX",
//        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.WhereTest",
        method = "g_V_whereXoutXcreatedX_and_outXknowsX_or_inXknowsXX_valuesXnameX",
        reason = "unsupported")
// @Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.WhereTest",
//        method = "g_V_whereXnotXoutXcreatedXXX_name",
//        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.WhereTest",
        method =
                "g_V_asXaX_out_asXbX_whereXin_count_isXeqX3XX_or_whereXoutXcreatedX_and_hasXlabel_personXXX_selectXa_bX",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.WhereTest",
        method = "g_withSideEffectXa_g_VX2XX_VX1X_out_whereXneqXaXX",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.WhereTest",
        method =
                "g_V_asXaX_outEXcreatedX_asXbX_inV_asXcX_inXcreatedX_asXdX_whereXa_ltXbX_orXgtXcXX_andXneqXdXXX_byXageX_byXweightX_byXinXcreatedX_valuesXageX_minX_selectXa_c_dX",
        reason = "unsupported")
// @Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.WhereTest",
//        method = "g_V_hasXageX_asXaX_out_in_hasXageX_asXbX_selectXa_bX_whereXb_hasXname_markoXX",
//        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.WhereTest",
        method = "g_VX1X_out_aggregateXxX_out_whereXnotXwithinXaXXX",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.WhereTest",
        method =
                "g_V_asXaX_outXcreatedX_asXbX_whereXandXasXbX_in__notXasXaX_outXcreatedX_hasXname_rippleXXX_selectXa_bX",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.WhereTest",
        method = "g_VX1X_repeatXbothEXcreatedX_whereXwithoutXeXX_aggregateXeX_otherVX_emit_path",
        reason = "unsupported")
// @Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.WhereTest",
//        method = "g_V_hasXageX_asXaX_out_in_hasXageX_asXbX_selectXa_bX_whereXa_neqXbXX",
//        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.CountTest",
        method = "g_V_fold_countXlocalX",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.CountTest",
        method = "g_V_repeatXoutX_timesX5X_asXaX_outXwrittenByX_asXbX_selectXa_bX_count",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.CountTest",
        method = "g_V_both_both_count",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.CountTest",
        method = "g_V_whereXinXkknowsX_outXcreatedX_count_is_0XX_name",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphTest",
        method = "g_V_hasLabelXpersonX_asXpX_VXsoftwareX_addInEXuses_pX",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphTest",
        method = "g_V_outXknowsX_V_name",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphTest",
        method =
                "g_V_hasXname_GarciaX_inXsungByX_asXsongX_V_hasXname_Willie_DixonX_inXwrittenByX_whereXeqXsongXX_name",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.OrderTest",
        method = "g_V_group_byXlabelX_byXname_order_byXdescX_foldX",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.OrderTest",
        method = "g_V_hasLabelXsongX_order_byXperformances_descX_byXnameX_rangeX110_120X_name",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.OrderTest",
        method =
                "g_V_asXvX_mapXbothE_weight_foldX_sumXlocalX_asXsX_selectXv_sX_order_byXselectXsX_descX",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.OrderTest",
        method = "g_V_mapXbothE_weight_foldX_order_byXsumXlocalX_descX",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.OrderTest",
        method = "g_V_hasLabelXpersonX_fold_orderXlocalX_byXageX",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.OrderTest",
        method =
                "g_V_hasXsong_name_OHBOYX_outXfollowedByX_outXfollowedByX_order_byXperformancesX_byXsongType_descX",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.OrderTest",
        method = "g_V_hasLabelXpersonX_group_byXnameX_byXoutE_weight_sumX_orderXlocalX_byXvaluesX",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.OrderTest",
        method =
                "g_VX1X_hasXlabel_personX_mapXmapXint_ageXX_orderXlocalX_byXvalues_descX_byXkeys_ascX",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.OrderTest",
        method = "g_V_name_order_byXa1_b1X_byXb2_a2X",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.OrderTest",
        method = "g_V_properties_order_byXkey_descX_key",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.OrderTest",
        method = "g_V_hasLabelXpersonX_order_byXvalueXageX_descX_name",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.OrderTest",
        method = "g_VX1X_elementMap_orderXlocalX_byXkeys_descXunfold",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.OrderTest",
        method =
                "g_V_hasLabelXpersonX_group_byXnameX_byXoutE_weight_sumX_unfold_order_byXvalues_descX",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.OrderTest",
        method = "g_V_both_hasLabelXpersonX_order_byXage_descX_name",
        reason = "unsupported")
// @Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.OrderTest",
//        method = "g_V_order_byXname_incrX_name",
//        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.OrderTest",
        method = "g_V_order_byXoutE_count_descX",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.OrderTest",
        method = "g_V_order_byXname_a1_b1X_byXname_b2_a2X_name",
        reason = "unsupported")
// @Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.OrderTest",
//        method = "g_V_outE_order_byXweight_decrX_weight",
//        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.PropertiesTest",
        method = "g_V_hasXageX_propertiesXage_nameX_value",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.PropertiesTest",
        method = "g_V_hasXageX_properties_hasXid_nameIdX_value",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.PropertiesTest",
        method = "g_injectXg_VX1X_propertiesXnameX_nextX_value",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.PropertiesTest",
        method = "g_V_hasXageX_properties_hasXid_nameIdAsStringX_value",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.PropertiesTest",
        method = "g_V_hasXageX_propertiesXname_ageX_value",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.PropertiesTest",
        method = "g_V_hasXageX_propertiesXnameX",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.SelectTest",
        method = "g_V_hasXperson_name_markoX_barrier_asXaX_outXknows_selectXaX",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.SelectTest",
        method =
                "g_V_hasXperson_name_markoX_elementMapXnameX_asXaX_unionXidentity_identityX_selectXaX_selectXnameX",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.SelectTest",
        method = "g_V_valueMap_selectXall_aX",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.SelectTest",
        method = "g_V_selectXlast_a_bX",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.SelectTest",
        method = "g_V_untilXout_outX_repeatXin_asXaX_in_asXbXX_selectXa_bX_byXnameX",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.SelectTest",
        method = "g_V_valueMap_selectXfirst_aX",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.SelectTest",
        method = "g_V_asXaX_out_aggregateXxX_asXbX_selectXa_bX_byXnameX",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.SelectTest",
        method = "g_V_label_groupCount_asXxX_selectXxX",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.SelectTest",
        method = "g_V_valueMap_selectXfirst_a_bX",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.SelectTest",
        method =
                "g_V_outXcreatedX_unionXasXprojectX_inXcreatedX_hasXname_markoX_selectXprojectX__asXprojectX_inXcreatedX_inXknowsX_hasXname_markoX_selectXprojectXX_groupCount_byXnameX",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.SelectTest",
        method = "g_V_valueMap_selectXlast_a_bX",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.SelectTest",
        method = "g_V_hasLabelXpersonX_asXpX_mapXbothE_label_groupCountX_asXrX_selectXp_rX",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.SelectTest",
        method =
                "g_V_chooseXoutE_count_isX0X__asXaX__asXbXX_chooseXselectXaX__selectXaX__selectXbXX",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.SelectTest",
        method = "g_V_outE_weight_groupCount_selectXkeysX_unfold",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.SelectTest",
        method = "g_V_outE_weight_groupCount_unfold_selectXvaluesX_unfold",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.SelectTest",
        method = "g_V_selectXfirst_aX",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.SelectTest",
        method = "g_V_outE_weight_groupCount_unfold_selectXkeysX_unfold",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.SelectTest",
        method = "g_V_valueMap_selectXlast_aX",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.SelectTest",
        method = "g_V_selectXall_aX",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.SelectTest",
        method = "g_V_hasXperson_name_markoX_path_asXaX_unionXidentity_identityX_selectXaX_unfold",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.SelectTest",
        method = "g_V_asXaX_groupXmX_by_byXbothE_countX_barrier_selectXmX_selectXselectXaXX",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.SelectTest",
        method =
                "g_V_asXaX_groupXmX_by_byXbothE_countX_barrier_selectXmX_selectXselectXaXX_byXmathX_plus_XX",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.SelectTest",
        method = "g_V_asXaX_outXknowsX_asXbX_localXselectXa_bX_byXnameXX",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.SelectTest",
        method = "g_V_selectXa_bX",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.SelectTest",
        method =
                "g_V_hasXname_gremlinX_inEXusesX_order_byXskill_ascX_asXaX_outV_asXbX_selectXa_bX_byXskillX_byXnameX",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.SelectTest",
        method = "g_V_selectXlast_aX",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.SelectTest",
        method = "g_V_untilXout_outX_repeatXin_asXaXX_selectXaX_byXtailXlocalX_nameX",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.SelectTest",
        method = "g_V_asXa_bX_out_asXcX_path_selectXkeysX",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.SelectTest",
        method = "g_VX1X_groupXaX_byXconstantXaXX_byXnameX_selectXaX_selectXaX",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.SelectTest",
        method = "g_V_selectXaX_count",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.SelectTest",
        method = "g_V_asXaX_outXknowsX_asXaX_selectXall_constantXaXX",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.SelectTest",
        method =
                "g_V_outE_weight_groupCount_selectXvaluesX_unfold_groupCount_selectXvaluesX_unfold",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.SelectTest",
        method = "g_V_selectXaX",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.SelectTest",
        method = "g_V_valueMap_selectXall_a_bX",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.SelectTest",
        method = "g_V_valueMap_selectXa_bX",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.SelectTest",
        method =
                "g_V_hasLabelXsoftwareX_asXnameX_asXlanguageX_asXcreatorsX_selectXname_language_creatorsX_byXnameX_byXlangX_byXinXcreatedX_name_fold_orderXlocalXX",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.SelectTest",
        method = "g_V_selectXfirst_a_bX",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.SelectTest",
        method = "g_VX1X_asXaX_repeatXout_asXaXX_timesX2X_selectXfirst_aX",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.SelectTest",
        method = "g_V_selectXall_a_bX",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.SelectTest",
        method = "g_V_hasXperson_name_markoX_count_asXaX_unionXidentity_identityX_selectXaX",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.SelectTest",
        method = "g_V_outE_weight_groupCount_selectXvaluesX_unfold",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.SelectTest",
        method = "g_V_valueMap_selectXaX",
        reason = "unsupported")
// @Graph.OptOut(
//        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexTest",
//        method = "g_VX4X_bothE_hasXweight_lt_1X_otherV",
//        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexTest",
        method = "g_V_hasLabelXloopsX_bothEXselfX",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexTest",
        method = "g_VX1_2_3_4X_name",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexTest",
        method = "g_V_hasLabelXpersonX_V_hasLabelXsoftwareX_name",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexTest",
        method = "g_VX1X_outEXknowsX_bothV_name",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexTest",
        method = "g_V_hasLabelXloopsX_bothXselfX",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.UnfoldTest",
        method = "g_V_valueMap_unfold_mapXkeyX",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.UnfoldTest",
        method = "g_V_localXoutE_foldX_unfold",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.UnfoldTest",
        method = "g_VX1X_repeatXboth_simplePathX_untilXhasIdX6XX_path_byXnameX_unfold",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GroupTest",
        method = "g_V_group_byXnameX",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GroupTest",
        method = "g_V_repeatXbothXfollowedByXX_timesX2X_groupXaX_byXsongTypeX_byXcountX_capXaX",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GroupTest",
        method =
                "g_withSideEffectXa__marko_666_noone_blahX_V_groupXaX_byXnameX_byXoutE_label_foldX_capXaX",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GroupTest",
        method = "g_V_repeatXout_groupXaX_byXnameX_byXcountX_timesX2X_capXaX",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GroupTest",
        method = "g_V_group_byXnameX_by",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GroupTest",
        method = "g_V_group_byXlabelX_byXbothE_groupXaX_byXlabelX_byXweight_sumX_weight_sumX",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GroupTest",
        method =
                "g_V_hasLabelXpersonX_asXpX_outXcreatedX_group_byXnameX_byXselectXpX_valuesXageX_sumX",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GroupTest",
        method =
                "g_V_hasLabelXsongX_groupXaX_byXnameX_byXproperties_groupCount_byXlabelXX_out_capXaX",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GroupTest",
        method = "g_V_hasXlangX_groupXaX_byXlangX_byXnameX_out_capXaX",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GroupTest",
        method = "g_V_group_byXoutE_countX_byXnameX",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GroupTest",
        method = "g_V_groupXaX_byXlabelX_byXoutE_weight_sumX_capXaX",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GroupCountTest",
        method =
                "g_V_unionXoutXknowsX__outXcreatedX_inXcreatedXX_groupCount_selectXvaluesX_unfold_sum",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GroupCountTest",
        method =
                "g_V_unionXrepeatXoutX_timesX2X_groupCountXmX_byXlangXX__repeatXinX_timesX2X_groupCountXmX_byXnameXX_capXmX",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GroupCountTest",
        method = "g_V_groupCount_byXbothE_countX",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GroupCountTest",
        method = "g_V_repeatXout_groupCountXaX_byXnameXX_timesX2X_capXaX",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GroupCountTest",
        method = "g_V_outXcreatedX_groupCount_byXnameX",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GroupCountTest",
        method = "g_V_outXcreatedX_groupCountXxX_capXxX",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GroupCountTest",
        method = "g_V_both_groupCountXaX_out_capXaX_selectXkeysX_unfold_both_groupCountXaX_capXaX",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GroupCountTest",
        method = "g_V_outXcreatedX_name_groupCount",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GroupCountTest",
        method = "g_V_outXcreatedX_groupCountXaX_byXnameX_capXaX",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GroupCountTest",
        method = "g_V_outXcreatedX_name_groupCountXaX_capXaX",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GroupCountTest",
        method =
                "g_V_both_groupCountXaX_byXlabelX_asXbX_barrier_whereXselectXaX_selectXsoftwareX_isXgtX2XXX_selectXbX_name",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GroupCountTest",
        method = "g_V_hasXnoX_groupCountXaX_capXaX",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.branch.RepeatTest",
        method =
                "g_V_repeatXoutXknowsXX_untilXrepeatXoutXcreatedXX_emitXhasXname_lopXXX_path_byXnameX",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.branch.RepeatTest",
        method = "g_V_repeatXoutX_timesX2X_emit_path",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.branch.RepeatTest",
        method = "g_V_repeatXout_repeatXoutX_timesX1XX_timesX1X_limitX1X_path_by_name",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.branch.RepeatTest",
        method = "g_V_emit_repeatXoutX_timesX2X_path",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.branch.RepeatTest",
        method = "g_VX1X_repeatXoutX_untilXoutE_count_isX0XX_name",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.branch.RepeatTest",
        method = "g_V_repeatXbothX_untilXname_eq_marko_or_loops_gt_1X_groupCount_byXnameX",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.branch.RepeatTest",
        method = "g_VX1X_repeatXgroupCountXmX_byXloopsX_outX_timesX3X_capXmX",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.branch.RepeatTest",
        method =
                "g_VX1X_repeatXrepeatXunionXout_uses_out_traversesXX_whereXloops_isX0X_timesX1X_timeX2X_name",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.branch.RepeatTest",
        method = "g_VX1X_emitXhasXlabel_personXX_repeatXoutX_name",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.branch.RepeatTest",
        method =
                "g_V_hasXname_markoX_repeatXoutE_inV_simplePathX_untilXhasXname_rippleXX_path_byXnameX_byXlabelX",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.branch.RepeatTest",
        method =
                "g_V_untilXconstantXtrueXX_repeatXrepeatXout_createdXX_untilXhasXname_rippleXXXemit_lang",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.branch.RepeatTest",
        method =
                "g_VX6X_repeatXa_bothXcreatedX_simplePathX_emitXrepeatXb_bothXknowsXX_untilXloopsXbX_asXb_whereXloopsXaX_asXbX_hasXname_vadasXX_dedup_name",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.branch.RepeatTest",
        method =
                "g_VX3X_repeatXbothX_createdXX_untilXloops_is_40XXemit_repeatXin_knowsXX_emit_loopsXisX1Xdedup_values",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.branch.RepeatTest",
        method = "g_V_hasXloop_name_loopX_repeatXinX_timesX5X_path_by_name",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.branch.RepeatTest",
        method = "g_V_repeatXgroupCountXmX_byXnameX_outX_timesX2X_capXmX",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.branch.RepeatTest",
        method = "g_V_repeatXrepeatXout_createdXX_untilXhasXname_rippleXXXemit_lang",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.branch.RepeatTest",
        method = "g_V_emit_repeatXa_outXknows_filterXloops_isX0XX_lang",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.branch.RepeatTest",
        method = "g_V_repeatXoutX_timesX2X_emit",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.branch.RepeatTest",
        method = "g_V_emit_timesX2X_repeatXoutX_path",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.branch.UnionTest",
        method = "g_VX1_2X_localXunionXoutE_count__inE_count__outE_weight_sumXX",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.branch.UnionTest",
        method = "g_V_chooseXlabel_is_person__unionX__out_lang__out_nameX__in_labelX",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.branch.UnionTest",
        method = "g_VX1_2X_localXunionXcountXX",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.branch.UnionTest",
        method = "g_V_chooseXlabel_is_person__unionX__out_lang__out_nameX__in_labelX_groupCount",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.branch.UnionTest",
        method = "g_VX1_2X_unionXoutE_count__inE_count__outE_weight_sumX",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.branch.UnionTest",
        method =
                "g_V_unionXrepeatXunionXoutXcreatedX__inXcreatedXX_timesX2X__repeatXunionXinXcreatedX__outXcreatedXX_timesX2XX_label_groupCount",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.DedupTest",
        method =
                "g_V_asXaX_repeatXbothX_timesX3X_emit_name_asXbX_group_byXselectXaXX_byXselectXbX_dedup_order_foldX_selectXvaluesX_unfold_dedup",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.DedupTest",
        method = "g_V_both_both_dedup_byXlabelX",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.DedupTest",
        method = "g_V_both_hasXlabel_softwareX_dedup_byXlangX_name",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.DedupTest",
        method = "g_V_out_asXxX_in_asXyX_selectXx_yX_byXnameX_fold_dedupXlocal_x_yX_unfold",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.DedupTest",
        method = "g_V_both_name_order_byXa_bX_dedup_value",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.DedupTest",
        method = "g_V_asXaX_outXcreatedX_asXbX_inXcreatedX_asXcX_dedupXa_bX_path",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.DedupTest",
        method = "g_V_both_both_dedup_byXoutE_countX_name",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.DedupTest",
        method = "g_V_asXaX_both_asXbX_dedupXa_bX_byXlabelX_selectXa_bX",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.DedupTest",
        method =
                "g_V_both_group_by_byXout_dedup_foldX_unfold_selectXvaluesX_unfold_out_order_byXnameX_limitX1X_valuesXnameX",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.FilterTest",
        method = "g_V_filterXfalseX",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.FilterTest",
        method = "g_VX1X_out_filterXage_gt_30X",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.FilterTest",
        method = "g_E_filterXfalseX",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.FilterTest",
        method = "g_VX1X_filterXage_gt_30X",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.FilterTest",
        method = "g_V_filterXname_startsWith_m_OR_name_startsWith_pX",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.FilterTest",
        method = "g_V_filterXtrueX",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.FilterTest",
        method = "g_E_filterXtrueX",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.FilterTest",
        method = "g_V_filterXlang_eq_javaX",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.FilterTest",
        method = "g_VX2X_filterXage_gt_30X",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GroupTest",
        method = "g_V_repeatXbothXfollowedByXX_timesX2X_group_byXsongTypeX_byXcountX",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GroupTest",
        method = "g_V_groupXaX_byXnameX_capXaX",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GroupTest",
        method = "g_V_out_group_byXlabelX_selectXpersonX_unfold_outXcreatedX_name_limitX2X",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GroupTest",
        method =
                "g_V_repeatXunionXoutXknowsX_groupXaX_byXageX__outXcreatedX_groupXbX_byXnameX_byXcountXX_groupXaX_byXnameXX_timesX2X_capXa_bX",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GroupTest",
        method = "g_V_hasLabelXsongX_group_byXnameX_byXproperties_groupCount_byXlabelXX",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GroupTest",
        method = "g_V_group_byXname_substring_1X_byXconstantX1XX",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GroupTest",
        method = "g_V_groupXmX_byXnameX_byXinXknowsX_nameX_capXmX",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GroupTest",
        method = "g_V_groupXaX_byXname_substring_1X_byXconstantX1XX_capXaX",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GroupTest",
        method = "g_V_outXfollowedByX_group_byXsongTypeX_byXbothE_group_byXlabelX_byXweight_sumXX",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.CountTest",
        method = "g_V_hasXnoX_count",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.PathTest",
        method = "g_VX1X_name_path",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GroupTest",
        method = "g_V_group_byXbothE_countX_byXgroup_byXlabelXX",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GroupCountTest",
        method = "g_V_hasXnoX_groupCount",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.CyclicPathTest",
        method = "g_VX1X_asXaX_outXcreatedX_asXbX_inXcreatedX_asXcX_cyclicPath_fromXaX_toXbX_path",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.CountTest",
        method = "g_V_repeatXoutX_timesX3X_count",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.CountTest",
        method = "g_V_repeatXoutX_timesX8X_count",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.PathTest",
        method = "g_V_asXaX_hasXname_markoX_asXbX_hasXage_29X_asXcX_path",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.PathTest",
        method = "g_V_out_out_path_byXnameX_byXageX",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.PathTest",
        method = "g_V_repeatXoutX_timesX2X_path_byXitX_byXnameX_byXlangX",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.PathTest",
        method = "g_VX1X_out_path_byXageX_byXnameX",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.PathTest",
        method = "g_V_asXaX_out_asXbX_out_asXcX_path_fromXbX_toXcX_byXnameX",
        reason = "unsupported")

// todo: return label is integer
// @Graph.OptOut(
//        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexTest",
//        method = "g_VX1X_outE",
//        reason = "returned label is id")
// @Graph.OptOut(
//        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexTest",
//        method = "g_VX4X_bothEXcreatedX",
//        reason = "returned label is id")
// @Graph.OptOut(
//        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexTest",
//        method = "g_VX4X_bothE",
//        reason = "returned label is id")
// @Graph.OptOut(
//        test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasTest",
//        method = "g_E_hasXlabelXknowsX",
//        reason = "returned label is id")
// @Graph.OptOut(
//        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexTest",
//        method = "g_VX2X_inE",
//        reason = "returned label is id")
// @Graph.OptOut(
//        method =
//
// "g_VX1X_outEXknowsX_asXhereX_hasXweight_1X_asXfakeX_inV_hasXname_joshX_selectXhereX",
//        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.SelectTest",
//        reason = "returned label is id")
// @Graph.OptOut(
//        method = "g_VX1X_outEXknowsX_asXhereX_hasXweight_1X_inV_hasXname_joshX_selectXhereX",
//        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.SelectTest",
//        reason = "returned label is id")
// @Graph.OptOut(
//        method = "g_VX1X_outEXknowsX_hasXweight_1X_asXhereX_inV_hasXname_joshX_selectXhereX",
//        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.SelectTest",
//        reason = "returned label is id")
// @Graph.OptOut(
//        method = "g_VX1X_outE_asXhereX_inV_hasXname_vadasX_selectXhereX",
//        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.SelectTest",
//        reason = "returned label is id")

// add more ignored tests which are out of ir range
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.branch.RepeatTest",
        method = "g_V_repeatXoutX_timesX2X",
        reason = "repeat is unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.branch.RepeatTest",
        method = "g_VX1X_timesX2X_repeatXoutX_name",
        reason = "repeat is unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.branch.RepeatTest",
        method = "g_V_repeatXbothX_timesX10X_asXaX_out_asXbX_selectXa_bX",
        reason = "repeat is unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.branch.RepeatTest",
        method = "g_V_repeatXoutX_timesX2X_repeatXinX_timesX2X_name",
        reason = "repeat is unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.branch.UnionTest",
        method = "g_VX1X_unionXrepeatXoutX_timesX2X__outX_name",
        reason = "repeat is unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.CyclicPathTest",
        method = "g_VX1X_outXcreatedX_inXcreatedX_cyclicPath_path",
        reason = "cyclicPath is unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.CyclicPathTest",
        method = "g_VX1X_outXcreatedX_inXcreatedX_cyclicPath",
        reason = "cyclicPath is unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.DedupTest",
        method = "g_V_both_properties_dedup_count",
        reason = "properties is unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.DedupTest",
        method = "g_V_bothE_properties_dedup_count",
        reason = "properties is unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.DedupTest",
        method = "g_V_both_properties_properties_dedup_count",
        reason = "properties is unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasTest",
        method = "g_VXv1X_hasXage_gt_30X",
        reason = "object id is unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasTest",
        method = "g_VXv4X_hasXage_gt_30X",
        reason = "object id is unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasTest",
        method = "g_V_hasXk_withinXcXX_valuesXkX",
        reason = "addV is unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.SimplePathTest",
        method = "g_VX1X_outXcreatedX_inXcreatedX_simplePath",
        reason = "simplePath is unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.SimplePathTest",
        method = "g_V_repeatXboth_simplePathX_timesX3X_path",
        reason = "simplePath is unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphTest",
        method = "g_VX1X_V_valuesXnameX",
        reason = "g.V().V() is unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.SelectTest",
        method = "g_VX1X_asXaX_repeatXout_asXaXX_timesX2X_selectXlast_aX",
        reason = "repeat is unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexTest",
        method = "g_VXv1X_out",
        reason = "object id is unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexTest",
        method = "g_VX1X_to_XOUT_knowsX",
        reason = "to(Direction.OUT,\"knows\") is unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexTest",
        method = "g_VXlistXv1_v2_v3XX_name",
        reason = "object id is unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.ValueMapTest",
        method = "g_VX1X_valueMapXname_locationX_byXunfoldX_by",
        reason = "valueMap().by(...) is unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.ValueMapTest",
        method = "g_V_valueMap_withXtokensX",
        reason = "valueMap(true) is unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.ValueMapTest",
        method = "g_V_valueMapXname_ageX_withXtokensX",
        reason = "valueMap(true) is unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.ValueMapTest",
        method = "g_V_valueMapXname_ageX_withXtokens_idsX_byXunfoldX",
        reason = "valueMap().with(...) is unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.ValueMapTest",
        method = "g_V_hasLabelXpersonX_filterXoutEXcreatedXX_valueMap_withXtokensX",
        reason = "valueMap().with(...) is unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.ValueMapTest",
        method = "g_V_valueMapXname_ageX_withXtokens_labelsX_byXunfoldX",
        reason = "valueMap().with(...) is unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GroupTest",
        method = "g_V_groupXmX_byXlabelX_byXlabel_countX_capXmX",
        reason = "group side effect is unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GroupCountTest",
        method = "g_V_hasXperson_name_markoX_bothXknowsX_groupCount_byXvaluesXnameX_foldX",
        reason = "fold() excluding group().by(...).by(fold()) is unsupported")

// @Graph.OptOut(method="g_V_unionXout__inX_name" ,
// test="org.apache.tinkerpop.gremlin.process.traversal.step.branch.UnionTest", reason = "will be
// supported")
// @Graph.OptOut(method="g_V_outE_asXeX_inV_asXvX_selectXeX_order_byXweight_ascX_selectXvX_valuesXnameX_dedup" , test="org.apache.tinkerpop.gremlin.process.traversal.step.filter.DedupTest", reason = "will be supported")
// @Graph.OptOut(method="g_V_hasXperson_name_markoX_age" ,
// test="org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasTest", reason = "will be
// supported")
@Graph.OptOut(
        method = "g_V_in_hasIdXneqX1XX",
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasTest",
        reason = "will be supported")
@Graph.OptOut(
        method = "g_V_hasXage_isXgt_30XX",
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasTest",
        reason = "will be supported")
@Graph.OptOut(
        method = "g_EX7X_hasXlabelXknowsX",
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasTest",
        reason = "will be supported")
@Graph.OptOut(
        method = "g_VX1X_outE_hasXweight_inside_0_06X_inV",
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasTest",
        reason = "will be supported")
// @Graph.OptOut(method="g_V_valuesXageX_isXgte_29X_isXlt_34X" ,
// test="org.apache.tinkerpop.gremlin.process.traversal.step.filter.IsTest", reason = "will be
// supported")
// @Graph.OptOut(method="g_V_valuesXageX_isXlte_30X" ,
// test="org.apache.tinkerpop.gremlin.process.traversal.step.filter.IsTest", reason = "will be
// supported")
// @Graph.OptOut(method="g_V_valuesXageX_isX32X" ,
// test="org.apache.tinkerpop.gremlin.process.traversal.step.filter.IsTest", reason = "will be
// supported")
// @Graph.OptOut(method="g_VX1X_out_limitX2X" ,
// test="org.apache.tinkerpop.gremlin.process.traversal.step.filter.RangeTest", reason = "will be
// supported")
@Graph.OptOut(
        method = "g_VX1X_outXknowsX_outEXcreatedX_rangeX0_1X_inV",
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.RangeTest",
        reason = "range is unsupported, use limit instead")
@Graph.OptOut(
        method = "g_VX1X_outXknowsX_outXcreatedX_rangeX0_1X",
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.RangeTest",
        reason = "range is unsupported, use limit instead")
// @Graph.OptOut(method="g_VX1X_asXaX_outXcreatedX_inXcreatedX_whereXeqXaXX_name" ,
// test="org.apache.tinkerpop.gremlin.process.traversal.step.filter.WhereTest", reason = "will be
// supported")
// @Graph.OptOut(method="g_V_asXaX_outEXcreatedX_asXbX_inV_asXcX_whereXa_gtXbX_orXeqXbXXX_byXageX_byXweightX_byXweightX_selectXa_cX_byXnameX" , test="org.apache.tinkerpop.gremlin.process.traversal.step.filter.WhereTest", reason = "will be supported")
// @Graph.OptOut(method="g_VX1X_asXaX_outXcreatedX_inXcreatedX_whereXneqXaXX_name" ,
// test="org.apache.tinkerpop.gremlin.process.traversal.step.filter.WhereTest", reason = "will be
// supported")
@Graph.OptOut(
        method =
                "g_VX1X_asXaX_outXcreatedX_inXcreatedX_asXbX_whereXasXbX_outXcreatedX_hasXname_rippleXX_valuesXage_nameX",
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.WhereTest",
        reason = "values('age', 'name') is unsupported")
// @Graph.OptOut(method="g_VX1X_asXaX_outXcreatedX_inXcreatedX_asXbX_whereXa_neqXbXX_name" ,
// test="org.apache.tinkerpop.gremlin.process.traversal.step.filter.WhereTest", reason = "will be
// supported")
// @Graph.OptOut(method="g_V_asXaX_outXcreatedX_whereXasXaX_name_isXjoshXX_inXcreatedX_name" ,
// test="org.apache.tinkerpop.gremlin.process.traversal.step.filter.WhereTest", reason = "will be
// supported")
// @Graph.OptOut(method="g_V_asXaX_outXcreatedX_asXbX_inXcreatedX_asXcX_bothXknowsX_bothXknowsX_asXdX_whereXc__notXeqXaX_orXeqXdXXXX_selectXa_b_c_dX" , test="org.apache.tinkerpop.gremlin.process.traversal.step.filter.WhereTest", reason = "will be supported")
// @Graph.OptOut(method="g_V_asXaX_outXcreatedX_inXcreatedX_asXbX_whereXa_gtXbXX_byXageX_selectXa_bX_byXnameX" , test="org.apache.tinkerpop.gremlin.process.traversal.step.filter.WhereTest", reason = "will be supported")
// @Graph.OptOut(method="g_V_hasLabelXpersonX_order_byXageX" ,
// test="org.apache.tinkerpop.gremlin.process.traversal.step.map.OrderTest", reason = "will be
// supported")
// @Graph.OptOut(method="g_V_asXaX_outXcreatedX_asXbX_order_byXshuffleX_selectXa_bX" ,
// test="org.apache.tinkerpop.gremlin.process.traversal.step.map.OrderTest", reason = "will be
// supported")
// @Graph.OptOut(method="g_V_order_byXname_ascX_name" ,
// test="org.apache.tinkerpop.gremlin.process.traversal.step.map.OrderTest", reason = "will be
// supported")
// @Graph.OptOut(method="g_V_order_byXnameX_name" ,
// test="org.apache.tinkerpop.gremlin.process.traversal.step.map.OrderTest", reason = "will be
// supported")
// @Graph.OptOut(method="g_V_outE_order_byXweight_descX_weight" ,
// test="org.apache.tinkerpop.gremlin.process.traversal.step.map.OrderTest", reason = "will be
// supported")
// @Graph.OptOut(method="g_V_both_hasLabelXpersonX_order_byXage_descX_limitX5X_name" ,
// test="org.apache.tinkerpop.gremlin.process.traversal.step.map.OrderTest", reason = "will be
// supported")
@Graph.OptOut(
        method = "g_VX1X_outEXcreatedX_inV_inE_outV_path",
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.PathTest",
        reason = "will be supported")
@Graph.OptOut(
        method = "g_EX11X_propertiesXweightX_asXaX_selectXaX_byXkeyX",
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.SelectTest",
        reason = "will be supported")
@Graph.OptOut(
        method = "g_V_asXaX_hasXname_markoX_asXbX_asXcX_selectXa_b_cX_by_byXnameX_byXageX",
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.SelectTest",
        reason = "one object can only have one alias")
// @Graph.OptOut(method="g_VX1X_asXaX_outXknowsX_asXbX_selectXa_bX" ,
// test="org.apache.tinkerpop.gremlin.process.traversal.step.map.SelectTest", reason = "will be
// supported")
@Graph.OptOut(
        method = "g_V_hasXname_isXmarkoXX_asXaX_selectXaX",
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.SelectTest",
        reason = "will be supported")
// @Graph.OptOut(method="g_V_asXaX_name_order_asXbX_selectXa_bX_byXnameX_by_XitX" ,
// test="org.apache.tinkerpop.gremlin.process.traversal.step.map.SelectTest", reason = "will be
// supported")
// @Graph.OptOut(method="g_V_asXaX_whereXoutXknowsXX_selectXaX" ,
// test="org.apache.tinkerpop.gremlin.process.traversal.step.map.SelectTest", reason = "will be
// supported")
@Graph.OptOut(
        method = "g_EX11X_propertiesXweightX_asXaX_selectXaX_byXvalueX",
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.SelectTest",
        reason = "will be supported")
// @Graph.OptOut(method="g_VX1X_outE_otherV" ,
// test="org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexTest", reason = "will be
// supported")
@Graph.OptOut(
        method = "g_VX1AsStringX_outXknowsX",
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexTest",
        reason = "will be supported")
// @Graph.OptOut(method="g_V_out_outE_inV_inE_inV_both_name" ,
// test="org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexTest", reason = "will be
// supported")
@Graph.OptOut(
        method = "g_EXe11X",
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexTest",
        reason = "will be supported")
// @Graph.OptOut(method="g_EX11X" ,
// test="org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexTest", reason = "will be
// supported")
// @Graph.OptOut(method="g_V_outE_hasXweight_1X_outV" ,
// test="org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexTest", reason = "will be
// supported")
@Graph.OptOut(
        method = "g_EXlistXe7_e11XX",
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexTest",
        reason = "will be supported")
@Graph.OptOut(
        method = "g_EX11AsStringX",
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexTest",
        reason = "will be supported")
@Graph.OptOut(
        method = "g_EXe7_e11X",
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexTest",
        reason = "will be supported")
// @Graph.OptOut(method="g_V_valueMapXname_ageX" ,
// test="org.apache.tinkerpop.gremlin.process.traversal.step.map.ValueMapTest", reason = "will be
// supported")
@Graph.OptOut(
        method = "g_VX1X_outXcreatedX_valueMap",
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.ValueMapTest",
        reason = "will be supported")
@Graph.OptOut(
        method = "g_V_valueMap",
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.ValueMapTest",
        reason = "will be supported")
@Graph.OptOut(
        method = "g_V_group_byXlabelX_byXlabel_countX",
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GroupTest",
        reason = "will be supported")
// @Graph.OptOut(method="g_V_group_byXageX" ,
// test="org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GroupTest", reason = "will
// be supported")

// @Graph.OptOut(method="g_V_hasXp_neqXvXX" ,
// test="org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasTest", reason = "existence of
// property is unsupported")
// @Graph.OptOut(method="g_V_hasXage_withoutX27X_count" ,
// test="org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasTest", reason = "existence of
// property is unsupported")
// @Graph.OptOut(method="g_VX1X_hasXcircumferenceX" ,
// test="org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasTest", reason = "existence of
// property is unsupported")
// @Graph.OptOut(method="g_V_hasXage_withoutX27_29X_count" ,
// test="org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasTest", reason = "existence of
// property is unsupported")
// @Graph.OptOut(method="g_VX1X_hasXnameX" ,
// test="org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasTest", reason = "existence of
// property is unsupported")
// @Graph.OptOut(method="g_V_hasXblahX" ,
// test="org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasTest", reason = "existence of
// property is unsupported")
// @Graph.OptOut(method="g_V_hasXlangX_group_byXlangX_byXcountX" ,
// test="org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GroupTest", reason =
// "existence of property is unsupported")
// @Graph.OptOut(method="g_VX1X_asXaX_out_hasXageX_whereXgtXaXX_byXageX_name" ,
// test="org.apache.tinkerpop.gremlin.process.traversal.step.filter.WhereTest", reason = "existence
// of property is unsupported")

// complex steps nested in match is unsupported yet, i.e. where(eq)/count/order/match
@Graph.OptOut(
        method =
                "g_V_matchXa_whereXa_neqXcXX__a_created_b__orXa_knows_vadas__a_0knows_and_a_hasXlabel_personXX__b_0created_c__b_0created_count_isXgtX1XXX_selectXa_b_cX_byXidX",
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.MatchTest",
        reason = "unsupported")
@Graph.OptOut(
        method = "g_V_matchXa_created_lop_b__b_0created_29_c__c_whereXrepeatXoutX_timesX2XXX",
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.MatchTest",
        reason = "unsupported")
@Graph.OptOut(
        method =
                "g_V_matchXa_knows_b__andXa_created_c__b_created_c__andXb_created_count_d__a_knows_count_dXXX",
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.MatchTest",
        reason = "unsupported")
@Graph.OptOut(
        method =
                "g_V_matchXa_hasXsong_name_sunshineX__a_mapX0followedBy_weight_meanX_b__a_0followedBy_c__c_filterXweight_whereXgteXbXXX_outV_dX_selectXdX_byXnameX",
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.MatchTest",
        reason = "unsupported")
@Graph.OptOut(
        method = "g_V_matchXa_created_b__a_repeatXoutX_timesX2XX_selectXa_bX",
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.MatchTest",
        reason = "unsupported")
@Graph.OptOut(
        method =
                "g_V_matchXa_knows_b__b_created_lop__b_matchXb_created_d__d_0created_cX_selectXcX_cX_selectXa_b_cX",
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.MatchTest",
        reason = "unsupported")
@Graph.OptOut(
        method =
                "g_V_matchXa_followedBy_count_isXgtX10XX_b__a_0followedBy_count_isXgtX10XX_bX_count",
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.MatchTest",
        reason = "unsupported")
@Graph.OptOut(
        method =
                "g_V_asXaX_out_asXbX_matchXa_out_count_c__orXa_knows_b__b_in_count_c__and__c_isXgtX2XXXX",
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.MatchTest",
        reason = "unsupported")
@Graph.OptOut(
        method =
                "g_V_matchXa_outEXcreatedX_order_byXweight_descX_limitX1X_inV_b__b_hasXlang_javaXX_selectXa_bX_byXnameX",
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.MatchTest",
        reason = "unsupported")
@Graph.OptOut(
        method =
                "g_V_matchXa_created_lop_b__b_0created_29_cX_whereXc_repeatXoutX_timesX2XX_selectXa_b_cX",
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.MatchTest",
        reason = "unsupported")
@Graph.OptOut(
        method = "g_V_asXaX_out_asXbX_matchXa_out_count_c__b_in_count_cX",
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.MatchTest",
        reason = "unsupported")
@Graph.OptOut(
        method =
                "g_V_matchXwhereXandXa_created_b__b_0created_count_isXeqX3XXXX__a_both_b__whereXb_inXX",
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.MatchTest",
        reason = "unsupported")
@Graph.OptOut(
        method = "g_V_matchXa_knows_count_bX_selectXbX",
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.MatchTest",
        reason = "unsupported")
@Graph.OptOut(
        method = "g_V_valueMap_matchXa_selectXnameX_bX",
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.MatchTest",
        reason = "unsupported")

// steps nested in match is unsupported yet, will be supported latter, i.e values('name')
@Graph.OptOut(
        method = "g_V_notXmatchXa_age_b__a_name_cX_whereXb_eqXcXX_selectXaXX_name",
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.MatchTest",
        reason = "unsupported")
@Graph.OptOut(
        method = "g_V_hasLabelXsongsX_matchXa_name_b__a_performances_cX_selectXb_cX_count",
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.MatchTest",
        reason = "unsupported")
@Graph.OptOut(
        method = "g_V_matchXa_outXknowsX_name_bX_identity",
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.MatchTest",
        reason = "unsupported")

// will be supported in the first milestone, i.e dedup('a', 'b')
@Graph.OptOut(
        method = "g_V_matchXa__a_both_b__b_both_cX_dedupXa_bX",
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.MatchTest",
        reason = "unsupported")
@Graph.OptOut(
        method = "g_V_matchXa__a_both_b__b_both_cX_dedupXa_bX",
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.MatchTest",
        reason = "unsupported")
@Graph.OptOut(
        method = "g_V_matchXa_both_b__b_both_cX_dedupXa_bX_byXlabelX",
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.MatchTest",
        reason = "unsupported")
@Graph.OptOut(
        method = "g_V_matchXd_0knows_a__d_hasXname_vadasX__a_knows_b__b_created_cX",
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.MatchTest",
        reason = "unsupported")
@Graph.OptOut(
        method = "g_V_matchXa_out_bX_selectXb_idX",
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.MatchTest",
        reason = "unsupported")
@Graph.OptOut(
        method = "g_V_matchXa_knows_b__b_created_c__a_created_cX_dedupXa_b_cX_selectXaX_byXnameX",
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.MatchTest",
        reason = "unsupported")

// need grateful graph
@Graph.OptOut(
        method = "g_V_matchXa_hasXname_GarciaX__a_0writtenBy_b__a_0sungBy_bX",
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.MatchTest",
        reason = "unsupported")
@Graph.OptOut(
        method =
                "g_V_matchXa_0sungBy_b__a_0sungBy_c__b_writtenBy_d__c_writtenBy_e__d_hasXname_George_HarisonX__e_hasXname_Bob_MarleyXX",
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.MatchTest",
        reason = "unsupported")
@Graph.OptOut(
        method =
                "g_V_matchXa_hasXname_GarciaX__a_0writtenBy_b__b_followedBy_c__c_writtenBy_d__whereXd_neqXaXXX",
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.MatchTest",
        reason = "unsupported")
@Graph.OptOut(
        method =
                "g_V_matchXa_0sungBy_b__a_0writtenBy_c__b_writtenBy_dX_whereXc_sungBy_dX_whereXd_hasXname_GarciaXX",
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.MatchTest",
        reason = "unsupported")
@Graph.OptOut(
        method =
                "g_V_matchXa_0sungBy_b__a_0writtenBy_c__b_writtenBy_d__c_sungBy_d__d_hasXname_GarciaXX",
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.MatchTest",
        reason = "unsupported")

// cases unsupported in groot
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasTest",
        method = "g_VX1X_out_hasXid_2_3X",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasTest",
        method = "g_V_hasXp_neqXvXX",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasTest",
        method = "g_V_hasXblahX",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasTest",
        method = "g_VX1X_hasXcircumferenceX",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexTest",
        method = "g_EX11X",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.ValueMapTest",
        method = "g_V_valueMapXname_ageX",
        reason = "unsupported")
public class MaxTestGraph implements Graph {
    private static final Logger logger = LoggerFactory.getLogger(MaxTestGraph.class);

    public static MaxTestGraph INSTANCE = null;

    private NodeBase maxNode;
    private RemoteConnection remoteConnection;
    private Cluster cluster;

    private MaxGraphClient ddlClient;

    public MaxTestGraph(Configs configs) {
        try {
            this.maxNode = new MaxNode(configs);
            this.maxNode.start();
            // This is to ensure the frontend can communicate some RPC after start, to make the
            // graphdef not null anymore, in snapshotCache.
            Thread.sleep(3000);
            int port = GremlinConfig.GREMLIN_PORT.get(configs);
            this.cluster = createCluster("localhost", port);
            this.ddlClient = MaxGraphClient.newBuilder().addHost("localhost", 55556).build();
            this.remoteConnection = DriverRemoteConnection.using(cluster);
        } catch (Throwable e) {
            this.closeGraph();
            throw new MaxGraphException(e);
        }
    }

    public static MaxTestGraph open(final Configuration conf) throws Exception {
        if (INSTANCE == null) {
            logger.info("open new MaxTestGraph");
            String log4rsPath =
                    Paths.get(
                                    Thread.currentThread()
                                            .getContextClassLoader()
                                            .getResource("log4rs.yml")
                                            .toURI())
                            .toString();
            Configs.Builder builder = Configs.newBuilder();
            conf.getKeys().forEachRemaining((k) -> builder.put(k, conf.getString(k)));
            Configs configs = builder.put(CommonConfig.LOG4RS_CONFIG.getKey(), log4rsPath).build();
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
        this.ddlClient.dropSchema();
    }

    public void loadSchema(LoadGraphWith.GraphData graphData)
            throws URISyntaxException, IOException {
        String schemaResource = graphData.name().toLowerCase() + ".schema";
        Path path =
                Paths.get(
                        Thread.currentThread()
                                .getContextClassLoader()
                                .getResource(schemaResource)
                                .toURI());
        String json = new String(Files.readAllBytes(path), StandardCharsets.UTF_8);
        this.ddlClient.loadJsonSchema(json);
    }

    public void loadData(LoadGraphWith.GraphData graphData) throws InterruptedException {
        Thread.sleep(5000);
        String graphName = graphData.name().toLowerCase();
        if (graphName.equals("modern")) {
            ddlClient.initWriteSession();

            Map<String, String> v1 = new HashMap<>();
            v1.put("id", "1");
            v1.put("name", "marko");
            v1.put("age", "29");
            ddlClient.addVertex("person", v1);

            Map<String, String> v2 = new HashMap<>();
            v2.put("id", "2");
            v2.put("name", "vadas");
            v2.put("age", "27");
            ddlClient.addVertex("person", v2);

            Map<String, String> v4 = new HashMap<>();
            v4.put("id", "4");
            v4.put("name", "josh");
            v4.put("age", "32");
            ddlClient.addVertex("person", v4);

            Map<String, String> v6 = new HashMap<>();
            v6.put("id", "6");
            v6.put("name", "peter");
            v6.put("age", "35");
            ddlClient.addVertex("person", v6);

            Map<String, String> v3 = new HashMap<>();
            v3.put("id", "3");
            v3.put("name", "lop");
            v3.put("lang", "java");
            ddlClient.addVertex("software", v3);

            Map<String, String> v5 = new HashMap<>();
            v5.put("id", "5");
            v5.put("name", "ripple");
            v5.put("lang", "java");
            ddlClient.addVertex("software", v5);

            ddlClient.commit();
            Thread.sleep(5000);

            ddlClient.addEdge(
                    "knows",
                    "person",
                    "person",
                    Collections.singletonMap("id", "1"),
                    Collections.singletonMap("id", "2"),
                    Collections.singletonMap("weight", "0.5"));

            ddlClient.addEdge(
                    "created",
                    "person",
                    "software",
                    Collections.singletonMap("id", "1"),
                    Collections.singletonMap("id", "3"),
                    Collections.singletonMap("weight", "0.4"));

            ddlClient.addEdge(
                    "knows",
                    "person",
                    "person",
                    Collections.singletonMap("id", "1"),
                    Collections.singletonMap("id", "4"),
                    Collections.singletonMap("weight", "1.0"));

            ddlClient.addEdge(
                    "created",
                    "person",
                    "software",
                    Collections.singletonMap("id", "4"),
                    Collections.singletonMap("id", "3"),
                    Collections.singletonMap("weight", "0.4"));

            ddlClient.addEdge(
                    "created",
                    "person",
                    "software",
                    Collections.singletonMap("id", "4"),
                    Collections.singletonMap("id", "5"),
                    Collections.singletonMap("weight", "1.0"));

            ddlClient.addEdge(
                    "created",
                    "person",
                    "software",
                    Collections.singletonMap("id", "6"),
                    Collections.singletonMap("id", "3"),
                    Collections.singletonMap("weight", "0.2"));

            ddlClient.commit();
            Thread.sleep(5000);
        } else {
            throw new UnsupportedOperationException("graph " + graphName + " is unsupported yet");
        }
    }

    @Override
    public GraphTraversalSource traversal() {
        GraphTraversalSource source =
                AnonymousTraversalSource.traversal(GraphTraversalSource.class)
                        .withRemote(remoteConnection);
        source.getStrategies().removeStrategies(ProfileStrategy.class, FilterRankingStrategy.class);
        return source;
    }

    @Override
    public Vertex addVertex(Object... keyValues) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <C extends GraphComputer> C compute(Class<C> graphComputerClass)
            throws IllegalArgumentException {
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

    public MaxNode getMaxNode() {
        return (MaxNode) maxNode;
    }
}
