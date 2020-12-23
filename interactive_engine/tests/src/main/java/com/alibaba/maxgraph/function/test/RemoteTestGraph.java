/**
 * This file is referred and derived from project apache/tinkerpop
 *
 *   https://github.com/apache/tinkerpop/blob/master/gremlin-server/src/test/java/org/apache/tinkerpop/gremlin/driver/remote/GraphBinaryRemoteGraphComputerProvider.java
 *
 * which has the following license:
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.alibaba.maxgraph.function.test;

import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.finalization.ProfileStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.FilterRankingStrategy;
import org.apache.tinkerpop.gremlin.structure.Graph;

//@Graph.OptIn("com.alibaba.maxgraph.function.test.gremlin.OtherGraphTestSuite")
@Graph.OptIn("com.alibaba.maxgraph.function.test.gremlin.GremlinStandardTestSuite")

@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.branch.RepeatTest",
        method = "g_V_repeatXoutXknowsXX_untilXrepeatXoutXcreatedXX_emitXhasXname_lopXXX_path_byXnameX",
        reason = "Not support nest loop")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.branch.RepeatTest",
        method = "g_V_repeatXout_repeatXoutX_timesX1XX_timesX1X_limitX1X_path_by_name",
        reason = "Not support nest loop")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.branch.RepeatTest",
        method = "g_V_repeatXbothX_untilXname_eq_marko_or_loops_gt_1X_groupCount_byXnameX",
        reason = "Not support lambda")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.branch.RepeatTest",
        method = "g_VX1X_repeatXgroupCountXmX_byXloopsX_outX_timesX3X_capXmX",
        reason = "Not support group count side effect")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.branch.RepeatTest",
        method = "g_VX1X_repeatXrepeatXunionXout_uses_out_traversesXX_whereXloops_isX0X_timesX1X_timeX2X_name",
        reason = "Not support nest loop")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.branch.RepeatTest",
        method = "g_V_untilXconstantXtrueXX_repeatXrepeatXout_createdXX_untilXhasXname_rippleXXXemit_lang",
        reason = "Not support nest loop")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.branch.RepeatTest",
        method = "g_VX6X_repeatXa_bothXcreatedX_simplePathX_emitXrepeatXb_bothXknowsXX_untilXloopsXbX_asXb_whereXloopsXaX_asXbX_hasXname_vadasXX_dedup_name",
        reason = "Not support nest loop")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.branch.RepeatTest",
        method = "g_VX3X_repeatXbothX_createdXX_untilXloops_is_40XXemit_repeatXin_knowsXX_emit_loopsXisX1Xdedup_values",
        reason = "Not support nest loop")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.branch.RepeatTest",
        method = "g_V_repeatXgroupCountXmX_byXnameX_outX_timesX2X_capXmX",
        reason = "Not support group count side effect")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.branch.RepeatTest",
        method = "g_V_repeatXrepeatXout_createdXX_untilXhasXname_rippleXXXemit_lang",
        reason = "Not support nest loop")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.branch.RepeatTest",
        method = "g_V_emit_repeatXa_outXknows_filterXloops_isX0XX_lang",
        reason = "Not support filter loops")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.branch.RepeatTest",
        method = "g_V_repeatXoutX_timesX2X_repeatXinX_timesX2X_name",
        reason = "Not support repeat brother")

@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.branch.UnionTest",
        method = "g_VX1_2X_localXunionXoutE_count__inE_count__outE_weight_sumXX",
        reason = "Not support local operator")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.branch.UnionTest",
        method = "g_VX1_2X_localXunionXcountXX",
        reason = "Not support local operator")

@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.CyclicPathTest",
        method = "g_VX1X_asXaX_outXcreatedX_asXbX_inXcreatedX_asXcX_cyclicPath_fromXaX_toXbX_path",
        reason = "Not support path from to")

@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.DedupTest",
        method = "g_V_out_in_valuesXnameX_fold_dedupXlocalX_unfold",
        reason = "Not support dedup local")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.DedupTest",
        method = "g_V_out_asXxX_in_asXyX_selectXx_yX_byXnameX_fold_dedupXlocal_x_yX_unfold",
        reason = "Not support dedup local")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.DedupTest",
        method = "g_V_both_name_order_byXa_bX_dedup_value",
        reason = "Not support lambda")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.DedupTest",
        method = "g_V_asXaX_both_asXbX_dedupXa_bX_byXlabelX_selectXa_bX",
        reason = "Not support dedup(a, b).by(...).by(...)")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.DedupTest",
        method = "g_V_asXaX_outXcreatedX_asXbX_inXcreatedX_asXcX_dedupXa_bX_path",
        reason = "Not support dedup(a, b).by(...).by(...)")

@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasTest",
        method = "g_V_outXcreatedX_hasXname__mapXlengthX_isXgtX3XXX_name",
        reason = "lambda")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasTest",
        method = "g_V_hasXname_containingXarkXX",
        reason = "not support TextP")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasTest",
        method = "g_V_hasXname_startingWithXmarXX",
        reason = "not support TextP")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasTest",
        method = "g_V_hasXname_endingWithXasXX",
        reason = "not support TextP")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasTest",
        method = "g_V_hasXname_not_containingXarkXX",
        reason = "not support TextP")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasTest",
        method = "g_V_hasXname_not_startingWithXmarXX",
        reason = "not support TextP")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasTest",
        method = "g_V_hasXname_not_endingWithXasXX",
        reason = "not support TextP")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasTest",
        method = "g_V_hasXperson_name_containingXoX_andXltXmXXX",
        reason = "not support TextP")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasTest",
        method = "g_V_hasXname_gtXmX_andXcontainingXoXXX",
        reason = "not support TextP")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasTest",
        method = "g_VX1X_out_hasXid_lt_3X",
        reason = "not support compare vertex id")

@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.RangeTest",
        method = "g_V_localXoutE_limitX1X_inVX_limitX3X",
        reason = "not support local operator")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.RangeTest",
        method = "g_V_asXaX_in_asXaX_in_asXaX_selectXmixed_aX_byXunfold_valuesXnameX_foldX_limitXlocal_1X",
        reason = "not support local operator")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.RangeTest",
        method = "g_V_asXaX_in_asXaX_in_asXaX_selectXmixed_aX_byXunfold_valuesXnameX_foldX_limitXlocal_2X",
        reason = "not support local operator")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.RangeTest",
        method = "g_V_asXaX_out_asXaX_out_asXaX_selectXmixed_aX_byXunfold_valuesXnameX_foldX_rangeXlocal_1_2X",
        reason = "not support local operator")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.RangeTest",
        method = "g_V_asXaX_out_asXaX_out_asXaX_selectXmixed_aX_byXunfold_valuesXnameX_foldX_rangeXlocal_1_3X",
        reason = "not support local operator")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.RangeTest",
        method = "g_V_asXaX_out_asXaX_out_asXaX_selectXmixed_aX_byXunfold_valuesXnameX_foldX_rangeXlocal_4_5X",
        reason = "not support local operator")

@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.SimplePathTest",
        method = "g_V_asXaX_out_asXbX_out_asXcX_simplePath_byXlabelX_fromXbX_toXcX_path_byXnameX",
        reason = "not support from to")


@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.WhereTest",
        method = "g_V_hasXageX_asXaX_out_in_hasXageX_asXbX_selectXa_bX_whereXb_hasXname_markoXX",
        reason = "Not support match")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.WhereTest",
        method = "g_V_hasXageX_asXaX_out_in_hasXageX_asXbX_selectXa_bX_whereXa_outXknowsX_bX",
        reason = "Not support match")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.WhereTest",
        method = "g_V_asXaX_outXcreatedX_whereXasXaX_name_isXjoshXX_inXcreatedX_name",
        reason = "Not support match")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.WhereTest",
        method = "g_VX1X_asXaX_outXcreatedX_inXcreatedX_asXbX_whereXasXbX_outXcreatedX_hasXname_rippleXX_valuesXage_nameX",
        reason = "Not support match")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.WhereTest",
        method = "g_V_asXaX_out_asXbX_whereXandXasXaX_outXknowsX_asXbX__orXasXbX_outXcreatedX_hasXname_rippleX__asXbX_inXknowsX_count_isXnotXeqX0XXXXX_selectXa_bX",
        reason = "Not support match")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.WhereTest",
        method = "g_withSideEffectXa_josh_peterX_VX1X_outXcreatedX_inXcreatedX_name_whereXwithinXaXX",
        reason = "Not support match")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.WhereTest",
        method = "g_withSideEffectXa_g_VX2XX_VX1X_out_whereXneqXaXX",
        reason = "Not support side effect")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.WhereTest",
        method = "g_VX1X_out_aggregateXxX_out_whereXnotXwithinXaXXX",
        reason = "Not support aggregate")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.WhereTest",
        method = "g_VX1X_repeatXbothEXcreatedX_whereXwithoutXeXX_aggregateXeX_otherVX_emit_path",
        reason = "Not support aggregate")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.WhereTest",
        method = "g_V_whereXnotXoutXcreatedXXX_name",
        reason = "Not support nest not in where")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.WhereTest",
        method = "g_V_asXaX_outEXcreatedX_asXbX_inV_asXcX_whereXa_gtXbX_orXeqXbXXX_byXageX_byXweightX_byXweightX_selectXa_cX_byXnameX",
        reason = "Not support nest not in where")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.WhereTest",
        method = "g_V_whereXoutXcreatedX_and_outXknowsX_or_inXknowsXX_valuesXnameX",
        reason = "Not support nest not in where")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.WhereTest",
        method = "g_V_asXaX_out_asXbX_whereXin_count_isXeqX3XX_or_whereXoutXcreatedX_and_hasXlabel_personXXX_selectXa_bX",
        reason = "Not support nest not in where")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.WhereTest",
        method = "g_V_asXaX_outEXcreatedX_asXbX_inV_asXcX_inXcreatedX_asXdX_whereXa_ltXbX_orXgtXcXX_andXneqXdXXX_byXageX_byXweightX_byXinXcreatedX_valuesXageX_minX_selectXa_c_dX",
        reason = "Not support nest not in where")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.WhereTest",
        method = "g_V_asXaX_outXcreatedX_asXbX_whereXandXasXbX_in__notXasXaX_outXcreatedX_hasXname_rippleXXX_selectXa_bX",
        reason = "Not support nest not in where")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.WhereTest",
        method = "g_V_asXaX_outXcreatedX_asXbX_inXcreatedX_asXcX_bothXknowsX_bothXknowsX_asXdX_whereXc__notXeqXaX_orXeqXdXXXX_selectXa_b_c_dX",
        reason = "Not support nest not in where")

@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.CountTest",
        method = "g_V_repeatXoutX_timesX5X_asXaX_outXwrittenByX_asXbX_selectXa_bX_count",
        reason = "Not support grateful graph")

@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.FoldTest",
        method = "g_V_age_foldX0_plusX",
        reason = "Not support BinaryOperator")

@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.MaxTest",
        method = "g_V_name_fold_maxXlocalX",
        reason = "Not support max local")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.MaxTest",
        method = "g_V_age_fold_maxXlocalX",
        reason = "Not support max local")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.MaxTest",
        method = "g_V_foo_fold_maxXlocalX",
        reason = "Not support max local")

@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.MinTest",
        method = "g_V_age_fold_minXlocalX",
        reason = "Not support min local")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.MinTest",
        method = "g_V_foo_fold_minXlocalX",
        reason = "Not support min local")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.MinTest",
        method = "g_V_name_fold_minXlocalX",
        reason = "Not support min local")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.MinTest",
        method = "g_V_foo_injectX9999999999X_min",
        reason = "Not support inject operator")

@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.SumTest",
        method = "g_V_age_fold_sumXlocalX",
        reason = "Not support sum local")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.SumTest",
        method = "g_V_foo_fold_sumXlocalX",
        reason = "Not support sum local")

@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.OrderTest",
        method = "g_V_asXvX_mapXbothE_weight_foldX_sumXlocalX_asXsX_selectXv_sX_order_byXselectXsX_descX",
        reason = "Not support map/local/lambda operator")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.OrderTest",
        method = "g_V_mapXbothE_weight_foldX_order_byXsumXlocalX_descX",
        reason = "Not support map/local/lambda operator")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.OrderTest",
        method = "g_VX1X_hasXlabel_personX_mapXmapXint_ageXX_orderXlocalX_byXvalues_descX_byXkeys_ascX",
        reason = "Not support map/local/lambda operator")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.OrderTest",
        method = "g_V_name_order_byXa1_b1X_byXb2_a2X",
        reason = "Not support map/local/lambda operator")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.OrderTest",
        method = "g_V_hasLabelXpersonX_order_byXvalueXageX_descX_name",
        reason = "Not support map/local/lambda operator")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.OrderTest",
        method = "g_V_order_byXname_a1_b1X_byXname_b2_a2X_name",
        reason = "Not support map/local/lambda operator")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.OrderTest",
        method = "g_V_properties_order_byXkey_descX_key",
        reason = "Not support remove id property from graph")

@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.PathTest",
        method = "g_V_asXaX_out_asXbX_out_asXcX_path_fromXbX_toXcX_byXnameX",
        reason = "Not support path from to")

@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.SelectTest",
        method = "g_V_asXaX_out_aggregateXxX_asXbX_selectXa_bX_byXnameX",
        reason = "Not support aggregate operator")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.SelectTest",
        method = "g_V_outXcreatedX_unionXasXprojectX_inXcreatedX_hasXname_markoX_selectXprojectX__asXprojectX_inXcreatedX_inXknowsX_hasXname_markoX_selectXprojectXX_groupCount_byXnameX",
        reason = "Not support match")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.SelectTest",
        method = "g_V_hasLabelXpersonX_asXpX_mapXbothE_label_groupCountX_asXrX_selectXp_rX",
        reason = "Not support map")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.SelectTest",
        method = "g_V_chooseXoutE_count_isX0X__asXaX__asXbXX_chooseXselectXaX__selectXaX__selectXbXX",
        reason = "Not support choose")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.SelectTest",
        method = "g_VX1X_asXaX_repeatXout_asXaXX_timesX2X_selectXlast_aX",
        reason = "Not support choose")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.SelectTest",
        method = "g_V_asXaX_groupXmX_by_byXbothE_countX_barrier_selectXmX_selectXselectXaXX",
        reason = "Not support group side effect")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.SelectTest",
        method = "g_V_asXaX_groupXmX_by_byXbothE_countX_barrier_selectXmX_selectXselectXaXX_byXmathX_plus_XX",
        reason = "Not support group side effect")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.SelectTest",
        method = "g_V_asXaX_outXknowsX_asXbX_localXselectXa_bX_byXnameXX",
        reason = "Not support local operator")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.SelectTest",
        method = "g_V_untilXout_outX_repeatXin_asXaXX_selectXaX_byXtailXlocalX_nameX",
        reason = "Not support tail operator")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.SelectTest",
        method = "g_V_asXa_bX_out_asXcX_path_selectXkeysX",
        reason = "Not support path keys value set not match list")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.SelectTest",
        method = "g_VX1X_groupXaX_byXconstantXaXX_byXnameX_selectXaX_selectXaX",
        reason = "Not support tail operator")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.SelectTest",
        method = "g_VX1X_asXaX_repeatXout_asXaXX_timesX2X_selectXfirst_aX",
        reason = "Not support tail operator")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.SelectTest",
        method = "g_V_asXaX_outXknowsX_asXaX_selectXall_constantXaXX",
        reason = "Not support select traversal")

@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexTest",
        method = "g_VX1_2_3_4X_name",
        reason = "Not support drop vertex in the case")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexTest",
        method = "g_V_hasLabelXpersonX_V_hasLabelXsoftwareX_name",
        reason = "Not support multiple source")

@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.ValueMapTest",
        method = "g_VX1X_valueMapXname_locationX_byXunfoldX_by",
        reason = "Not support value map by operator")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.ValueMapTest",
        method = "g_V_valueMapXname_ageX_withXtokens_labelsX_byXunfoldX",
        reason = "Not support value map by operator")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.PropertiesTest",
        method = "g_V_hasXageX_properties_hasXid_nameIdX_value",
        reason = "Not support value map by operator")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.PropertiesTest",
        method = "g_V_hasXageX_properties_hasXid_nameIdAsStringX_value",
        reason = "Not support value map by operator")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.PropertiesTest",
        method = "g_injectXg_VX1X_propertiesXnameX_nextX_value",
        reason = "Not support inject operator")
public class RemoteTestGraph extends DummyGraph {
    public static final String GRAPH_NAME = "test.graph.name";
    private RemoteGremlinConnection remoteGremlinConnection;

    public RemoteTestGraph(final Configuration configuration) {
        try {
            String gremlinEndpoint = configuration.getString(GRAPH_NAME);
            remoteGremlinConnection = new RemoteGremlinConnection(gremlinEndpoint);
        } catch (Exception e) {
            throw new RuntimeException("initiate remote test graph fail " + e);
        }
    }

    public static RemoteTestGraph open(final Configuration configuration) {
        return new RemoteTestGraph(configuration);
    }

    @Override
    public void close() throws Exception {
        remoteGremlinConnection.close();
    }

    @Override
    public Configuration configuration() {
        return null;
    }

    @Override
    public GraphTraversalSource traversal() {
        GraphTraversalSource graphTraversalSource = new GraphTraversalSource(
                this,
                TraversalStrategies.GlobalCache.getStrategies(this.getClass())).withRemote(remoteGremlinConnection);
        TraversalStrategies strategies = graphTraversalSource.getStrategies();
        strategies.removeStrategies(
                ProfileStrategy.class,
                FilterRankingStrategy.class);
        return graphTraversalSource;
    }
}

