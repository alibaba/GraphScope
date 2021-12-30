package com.alibaba.graphscope.gaia;

import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.finalization.ProfileStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.FilterRankingStrategy;
import org.apache.tinkerpop.gremlin.structure.Graph;

@Graph.OptIn("com.alibaba.graphscope.gaia.GaiaAdaptorGremlinTestSuite")
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
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.DedupTest",
        method =
                "g_V_both_group_by_byXout_dedup_foldX_unfold_selectXvaluesX_unfold_out_order_byXnameX_limitX1X_valuesXnameX",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasTest",
        method = "g_V_hasIdXwithoutXemptyXX_count",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasTest",
        method = "g_V_hasNotXageX_name",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasTest",
        method = "g_V_hasXname_containingXarkXX",
        reason = "unsupported")
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
// @Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasTest",
//        method = "g_V_hasXlabel_isXsoftwareXX",
//        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasTest",
        method = "g_VX1AsStringX_out_hasXid_2AsStringX",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasTest",
        method = "g_V_outXcreatedX_hasXname__mapXlengthX_isXgtX3XXX_name",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasTest",
        method = "g_V_hasXperson_name_containingXoX_andXltXmXXX",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasTest",
        method = "g_V_hasXname_not_startingWithXmarXX",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasTest",
        method = "g_V_hasXname_not_containingXarkXX",
        reason = "unsupported")
// @Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasTest",
//        method = "g_V_bothE_properties_dedup_hasKeyXweightX_hasValueXltX0d3XX_value",
//        reason = "unsupported")
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
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasTest",
        method = "g_V_hasXname_gtXmX_andXcontainingXoXXX",
        reason = "unsupported")
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
// @Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasTest",
//        method = "g_V_bothE_properties_dedup_hasKeyXweightX_value",
//        reason = "unsupported")
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
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasTest",
        method = "g_V_bothE_properties_dedup_hasKeyXweightX_hasValueXltX0d3XX_value",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasTest",
        method = "g_V_bothE_properties_dedup_hasKeyXweightX_value",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasTest",
        method = "g_V_hasXlabel_isXsoftwareXX",
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
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.WhereTest",
        method = "g_V_hasXageX_asXaX_out_in_hasXageX_asXbX_selectXa_bX_whereXa_eqXbXX",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.WhereTest",
        method =
                "g_withSideEffectXa_josh_peterX_VX1X_outXcreatedX_inXcreatedX_name_whereXwithinXaXX",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.WhereTest",
        method = "g_V_hasXageX_asXaX_out_in_hasXageX_asXbX_selectXa_bX_whereXa_outXknowsX_bX",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.WhereTest",
        method = "g_V_whereXoutXcreatedX_and_outXknowsX_or_inXknowsXX_valuesXnameX",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.WhereTest",
        method = "g_V_whereXnotXoutXcreatedXXX_name",
        reason = "unsupported")
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
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.WhereTest",
        method = "g_V_hasXageX_asXaX_out_in_hasXageX_asXbX_selectXa_bX_whereXb_hasXname_markoXX",
        reason = "unsupported")
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
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.WhereTest",
        method = "g_V_hasXageX_asXaX_out_in_hasXageX_asXbX_selectXa_bX_whereXa_neqXbXX",
        reason = "unsupported")
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
// @Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.OrderTest",
//        method = "g_VX1X_elementMap_orderXlocalX_byXkeys_descXunfold",
//        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.OrderTest",
        method =
                "g_V_hasLabelXpersonX_group_byXnameX_byXoutE_weight_sumX_unfold_order_byXvalues_descX",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.OrderTest",
        method = "g_V_both_hasLabelXpersonX_order_byXage_descX_name",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.OrderTest",
        method = "g_V_order_byXname_incrX_name",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.OrderTest",
        method = "g_V_order_byXoutE_count_descX",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.OrderTest",
        method = "g_V_order_byXname_a1_b1X_byXname_b2_a2X_name",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.OrderTest",
        method = "g_V_outE_order_byXweight_decrX_weight",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.OrderTest",
        method = "g_VX1X_elementMap_orderXlocalX_byXkeys_descXunfold",
        reason = "unsupported")
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
// @Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.SelectTest",
//        method = "g_V_hasXperson_name_markoX_barrier_asXaX_outXknows_selectXaX",
//        reason = "unsupported")
// @Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.SelectTest",
//        method =
// "g_V_hasXperson_name_markoX_elementMapXnameX_asXaX_unionXidentity_identityX_selectXaX_selectXnameX",
//        reason = "unsupported")
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
// @Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.SelectTest",
//        method =
// "g_V_hasXperson_name_markoX_path_asXaX_unionXidentity_identityX_selectXaX_unfold",
//        reason = "unsupported")
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
// @Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.SelectTest",
//        method = "g_V_hasXperson_name_markoX_count_asXaX_unionXidentity_identityX_selectXaX",
//        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.SelectTest",
        method = "g_V_outE_weight_groupCount_selectXvaluesX_unfold",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.SelectTest",
        method = "g_V_valueMap_selectXaX",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.SelectTest",
        method = "g_V_hasXperson_name_markoX_path_asXaX_unionXidentity_identityX_selectXaX_unfold",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.SelectTest",
        method = "g_V_hasXperson_name_markoX_barrier_asXaX_outXknows_selectXaX",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.SelectTest",
        method = "g_V_hasXperson_name_markoX_count_asXaX_unionXidentity_identityX_selectXaX",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.SelectTest",
        method =
                "g_V_hasXperson_name_markoX_elementMapXnameX_asXaX_unionXidentity_identityX_selectXaX_selectXnameX",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexTest",
        method = "g_VX4X_bothE_hasXweight_lt_1X_otherV",
        reason = "unsupported")
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

// todo: fix global id error && impl otherV by runtime
// @Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.branch.RepeatTest",
//        method =
// "g_V_repeatXoutXknowsXX_untilXrepeatXoutXcreatedXX_emitXhasXname_lopXXX_path_byXnameX",
//        reason = "unsupported")
// @Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.branch.RepeatTest",
//        method = "g_V_repeatXoutX_timesX2X_emit_path",
//        reason = "unsupported")
// @Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.branch.RepeatTest",
//        method = "g_V_repeatXout_repeatXoutX_timesX1XX_timesX1X_limitX1X_path_by_name",
//        reason = "unsupported")
// @Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.branch.RepeatTest",
//        method = "g_V_emit_repeatXoutX_timesX2X_path",
//        reason = "unsupported")
// @Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.branch.RepeatTest",
//        method = "g_VX1X_repeatXoutX_untilXoutE_count_isX0XX_name",
//        reason = "unsupported")
// @Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.branch.RepeatTest",
//        method = "g_V_repeatXbothX_untilXname_eq_marko_or_loops_gt_1X_groupCount_byXnameX",
//        reason = "unsupported")
// @Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.branch.RepeatTest",
//        method = "g_VX1X_repeatXgroupCountXmX_byXloopsX_outX_timesX3X_capXmX",
//        reason = "unsupported")
// @Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.branch.RepeatTest",
//        method =
// "g_VX1X_repeatXrepeatXunionXout_uses_out_traversesXX_whereXloops_isX0X_timesX1X_timeX2X_name",
//        reason = "unsupported")
// @Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.branch.RepeatTest",
//        method = "g_VX1X_emitXhasXlabel_personXX_repeatXoutX_name",
//        reason = "unsupported")
// @Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.branch.RepeatTest",
//        method =
// "g_V_hasXname_markoX_repeatXoutE_inV_simplePathX_untilXhasXname_rippleXX_path_byXnameX_byXlabelX",
//        reason = "unsupported")
// @Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.branch.RepeatTest",
//        method =
// "g_V_untilXconstantXtrueXX_repeatXrepeatXout_createdXX_untilXhasXname_rippleXXXemit_lang",
//        reason = "unsupported")
// @Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.branch.RepeatTest",
//        method =
// "g_VX6X_repeatXa_bothXcreatedX_simplePathX_emitXrepeatXb_bothXknowsXX_untilXloopsXbX_asXb_whereXloopsXaX_asXbX_hasXname_vadasXX_dedup_name",
//        reason = "unsupported")
// @Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.branch.RepeatTest",
//        method =
// "g_VX3X_repeatXbothX_createdXX_untilXloops_is_40XXemit_repeatXin_knowsXX_emit_loopsXisX1Xdedup_values",
//        reason = "unsupported")
// @Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.branch.RepeatTest",
//        method = "g_V_hasXloop_name_loopX_repeatXinX_timesX5X_path_by_name",
//        reason = "unsupported")
// @Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.branch.RepeatTest",
//        method = "g_V_repeatXgroupCountXmX_byXnameX_outX_timesX2X_capXmX",
//        reason = "unsupported")
// @Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.branch.RepeatTest",
//        method = "g_V_repeatXrepeatXout_createdXX_untilXhasXname_rippleXXXemit_lang",
//        reason = "unsupported")
// @Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.branch.RepeatTest",
//        method = "g_V_emit_repeatXa_outXknows_filterXloops_isX0XX_lang",
//        reason = "unsupported")
// @Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.branch.RepeatTest",
//        method = "g_V_repeatXoutX_timesX2X_emit",
//        reason = "unsupported")
// @Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.branch.RepeatTest",
//        method = "g_V_emit_timesX2X_repeatXoutX_path",
//        reason = "unsupported")
// @Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.branch.UnionTest",
//        method = "g_VX1_2X_localXunionXoutE_count__inE_count__outE_weight_sumXX",
//        reason = "unsupported")
// @Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.branch.UnionTest",
//        method = "g_V_chooseXlabel_is_person__unionX__out_lang__out_nameX__in_labelX",
//        reason = "unsupported")
// @Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.branch.UnionTest",
//        method = "g_VX1_2X_localXunionXcountXX",
//        reason = "unsupported")
// @Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.branch.UnionTest",
//        method = "g_V_chooseXlabel_is_person__unionX__out_lang__out_nameX__in_labelX_groupCount",
//        reason = "unsupported")
// @Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.branch.UnionTest",
//        method = "g_VX1_2X_unionXoutE_count__inE_count__outE_weight_sumX",
//        reason = "unsupported")
// @Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.branch.UnionTest",
//        method =
// "g_V_unionXrepeatXunionXoutXcreatedX__inXcreatedXX_timesX2X__repeatXunionXinXcreatedX__outXcreatedXX_timesX2XX_label_groupCount",
//        reason = "unsupported")
// @Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.DedupTest",
//        method =
// "g_V_asXaX_repeatXbothX_timesX3X_emit_name_asXbX_group_byXselectXaXX_byXselectXbX_dedup_order_foldX_selectXvaluesX_unfold_dedup",
//        reason = "unsupported")
// @Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.DedupTest",
//        method = "g_V_both_both_dedup_byXlabelX",
//        reason = "unsupported")
// @Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.DedupTest",
//        method = "g_V_both_hasXlabel_softwareX_dedup_byXlangX_name",
//        reason = "unsupported")
// @Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.DedupTest",
//        method = "g_V_out_asXxX_in_asXyX_selectXx_yX_byXnameX_fold_dedupXlocal_x_yX_unfold",
//        reason = "unsupported")
// @Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.DedupTest",
//        method = "g_V_both_name_order_byXa_bX_dedup_value",
//        reason = "unsupported")
// @Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.DedupTest",
//        method = "g_V_asXaX_outXcreatedX_asXbX_inXcreatedX_asXcX_dedupXa_bX_path",
//        reason = "unsupported")
// @Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.DedupTest",
//        method = "g_V_both_both_dedup_byXoutE_countX_name",
//        reason = "unsupported")
// @Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.DedupTest",
//        method = "g_V_asXaX_both_asXbX_dedupXa_bX_byXlabelX_selectXa_bX",
//        reason = "unsupported")
// @Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.FilterTest",
//        method = "g_V_filterXfalseX",
//        reason = "unsupported")
// @Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.FilterTest",
//        method = "g_VX1X_out_filterXage_gt_30X",
//        reason = "unsupported")
// @Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.FilterTest",
//        method = "g_E_filterXfalseX",
//        reason = "unsupported")
// @Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.FilterTest",
//        method = "g_VX1X_filterXage_gt_30X",
//        reason = "unsupported")
// @Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.FilterTest",
//        method = "g_V_filterXname_startsWith_m_OR_name_startsWith_pX",
//        reason = "unsupported")
// @Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.FilterTest",
//        method = "g_V_filterXtrueX",
//        reason = "unsupported")
// @Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.FilterTest",
//        method = "g_E_filterXtrueX",
//        reason = "unsupported")
// @Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.FilterTest",
//        method = "g_V_filterXlang_eq_javaX",
//        reason = "unsupported")
// @Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.FilterTest",
//        method = "g_VX2X_filterXage_gt_30X",
//        reason = "unsupported")
// @Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasTest",
//        method = "g_EX11X_outV_outE_hasXid_10AsStringX",
//        reason = "unsupported")
// @Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GroupTest",
//        method = "g_V_repeatXbothXfollowedByXX_timesX2X_group_byXsongTypeX_byXcountX",
//        reason = "unsupported")
// @Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GroupTest",
//        method = "g_V_groupXaX_byXnameX_capXaX",
//        reason = "unsupported")
// @Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GroupTest",
//        method = "g_V_out_group_byXlabelX_selectXpersonX_unfold_outXcreatedX_name_limitX2X",
//        reason = "unsupported")
// @Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GroupTest",
//        method =
// "g_V_repeatXunionXoutXknowsX_groupXaX_byXageX__outXcreatedX_groupXbX_byXnameX_byXcountXX_groupXaX_byXnameXX_timesX2X_capXa_bX",
//        reason = "unsupported")
// @Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GroupTest",
//        method = "g_V_hasLabelXsongX_group_byXnameX_byXproperties_groupCount_byXlabelXX",
//        reason = "unsupported")
// @Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GroupTest",
//        method = "g_V_group_byXname_substring_1X_byXconstantX1XX",
//        reason = "unsupported")
// @Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GroupTest",
//        method = "g_V_groupXmX_byXnameX_byXinXknowsX_nameX_capXmX",
//        reason = "unsupported")
// @Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GroupTest",
//        method = "g_V_groupXaX_byXname_substring_1X_byXconstantX1XX_capXaX",
//        reason = "unsupported")
// @Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GroupTest",
//        method =
// "g_V_outXfollowedByX_group_byXsongTypeX_byXbothE_group_byXlabelX_byXweight_sumXX",
//        reason = "unsupported")
// @Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.branch.RepeatTest",
//        method = "g_VX1X_timesX2X_repeatXoutX_name",
//        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.branch.RepeatTest",
        method = "g_V_repeatXbothX_timesX10X_asXaX_out_asXbX_selectXa_bX",
        reason = "unsupported")
// @Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.branch.UnionTest",
//        method = "g_VX1X_unionXrepeatXoutX_timesX2X__outX_name",
//        reason = "unsupported")
// @Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.CyclicPathTest",
//        method = "g_VX1X_outXcreatedX_inXcreatedX_cyclicPath_path",
//        reason = "unsupported")
// @Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.CyclicPathTest",
//        method = "g_VX1X_outXcreatedX_inXcreatedX_cyclicPath",
//        reason = "unsupported")
// @Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasTest",
//        method = "g_VXv1X_hasXage_gt_30X",
//        reason = "unsupported")
// @Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasTest",
//        method = "g_V_in_hasIdXneqX1XX",
//        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasTest",
        method = "g_EX7X_hasXlabelXknowsX",
        reason = "unsupported")
// @Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasTest",
//        method = "g_VX1X_out_hasXid_2X",
//        reason = "unsupported")
// @Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasTest",
//        method = "g_VX1X_out_hasXid_2_3X",
//        reason = "unsupported")
// @Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasTest",
//        method = "g_VX1X_hasXnameX",
//        reason = "unsupported")
// @Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasTest",
//        method = "g_VX4X_hasXage_gt_30X",
//        reason = "unsupported")
// @Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasTest",
//        method = "g_VX1X_hasXname_markoX",
//        reason = "unsupported")
// @Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasTest",
//        method = "g_VX1X_outE_hasXweight_inside_0_06X_inV",
//        reason = "unsupported")
// @Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasTest",
//        method = "g_VX1X_hasXage_gt_30X",
//        reason = "unsupported")
// @Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasTest",
//        method = "g_V_hasIdX1X_hasIdX2X",
//        reason = "unsupported")
// @Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasTest",
//        method = "g_VXv4X_hasXage_gt_30X",
//        reason = "unsupported")
// @Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.RangeTest",
//        method = "g_VX1X_out_limitX2X",
//        reason = "unsupported")
// @Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.RangeTest",
//        method = "g_VX1X_outXknowsX_outEXcreatedX_rangeX0_1X_inV",
//        reason = "unsupported")
// @Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.RangeTest",
//        method = "g_VX1X_outXknowsX_outXcreatedX_rangeX0_1X",
//        reason = "unsupported")
// @Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.SimplePathTest",
//        method = "g_VX1X_outXcreatedX_inXcreatedX_simplePath",
//        reason = "unsupported")
// @Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.WhereTest",
//        method = "g_VX1X_asXaX_outXcreatedX_inXcreatedX_whereXeqXaXX_name",
//        reason = "unsupported")
// @Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.WhereTest",
//        method = "g_VX1X_asXaX_outXcreatedX_inXcreatedX_whereXneqXaXX_name",
//        reason = "unsupported")
// @Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.WhereTest",
//        method =
// "g_VX1X_asXaX_outXcreatedX_inXcreatedX_asXbX_whereXasXbX_outXcreatedX_hasXname_rippleXX_valuesXage_nameX",
//        reason = "unsupported")
// @Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.WhereTest",
//        method = "g_VX1X_asXaX_outXcreatedX_inXcreatedX_asXbX_whereXa_neqXbXX_name",
//        reason = "unsupported")
// @Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.WhereTest",
//        method = "g_VX1X_asXaX_out_hasXageX_whereXgtXaXX_byXageX_name",
//        reason = "unsupported")
// @Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.WhereTest",
//        method =
// "g_V_asXaX_outXcreatedX_asXbX_inXcreatedX_asXcX_bothXknowsX_bothXknowsX_asXdX_whereXc__notXeqXaX_orXeqXdXXXX_selectXa_b_c_dX",
//        reason = "unsupported")
// @Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.PathTest",
//        method = "g_VX1X_outEXcreatedX_inV_inE_outV_path",
//        reason = "unsupported")
// @Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.SelectTest",
//        method =
// "g_VX1X_outEXknowsX_asXhereX_hasXweight_1X_asXfakeX_inV_hasXname_joshX_selectXhereX",
//        reason = "unsupported")
// @Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.SelectTest",
//        method = "g_VX1X_asXaX_outXknowsX_asXbX_selectXaX_byXnameX",
//        reason = "unsupported")
// @Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.SelectTest",
//        method = "g_VX1X_outEXknowsX_asXhereX_hasXweight_1X_inV_hasXname_joshX_selectXhereX",
//        reason = "unsupported")
// @Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.SelectTest",
//        method = "g_VX1X_asXaX_repeatXout_asXaXX_timesX2X_selectXlast_aX",
//        reason = "unsupported")
// @Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.SelectTest",
//        method = "g_V_asXaX_hasXname_markoX_asXbX_asXcX_selectXa_b_cX_by_byXnameX_byXageX",
//        reason = "unsupported")
// @Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.SelectTest",
//        method = "g_VX1X_outEXknowsX_hasXweight_1X_asXhereX_inV_hasXname_joshX_selectXhereX",
//        reason = "unsupported")
// @Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.SelectTest",
//        method = "g_VX1X_outE_asXhereX_inV_hasXname_vadasX_selectXhereX",
//        reason = "unsupported")
// @Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.SelectTest",
//        method = "g_VX1X_asXaX_outXknowsX_asXbX_selectXa_bX",
//        reason = "unsupported")
// @Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.SelectTest",
//        method = "g_VX1X_asXhereX_out_selectXhereX",
//        reason = "unsupported")
// @Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.SelectTest",
//        method = "g_VX1X_asXaX_outXknowsX_asXbX_selectXaX",
//        reason = "unsupported")
// @Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.SelectTest",
//        method = "g_VX1X_asXaX_outXknowsX_asXbX_selectXa_bX_byXnameX",
//        reason = "unsupported")
// @Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.SelectTest",
//        method = "g_VX4X_out_asXhereX_hasXlang_javaX_selectXhereX_name",
//        reason = "unsupported")
// @Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.SelectTest",
//        method = "g_VX4X_out_asXhereX_hasXlang_javaX_selectXhereX",
//        reason = "unsupported")
// @Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexTest",
//        method = "g_VX1X_outE_otherV",
//        reason = "unsupported")
// @Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexTest",
//        method = "g_VX4X_bothE_otherV",
//        reason = "unsupported")
// @Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexTest",
//        method = "g_VX1AsStringX_outXknowsX",
//        reason = "unsupported")
// @Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexTest",
//        method = "g_VX1X_out",
//        reason = "unsupported")
// @Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexTest",
//        method = "g_VX1X_outE",
//        reason = "unsupported")
// @Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexTest",
//        method = "g_VX1X_outEXknows_createdX_inV",
//        reason = "unsupported")
// @Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexTest",
//        method = "g_VX1X_to_XOUT_knowsX",
//        reason = "unsupported")
// @Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexTest",
//        method = "g_VXlistX1_2_3XX_name",
//        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexTest",
        method = "g_EX11X",
        reason = "unsupported")
// @Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexTest",
//        method = "g_VX1X_outXknows_createdX",
//        reason = "unsupported")
// @Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexTest",
//        method = "g_VX4X_bothEXcreatedX",
//        reason = "unsupported")
// @Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexTest",
//        method = "g_VX1X_outEXknowsX_inV",
//        reason = "unsupported")
// @Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexTest",
//        method = "g_VXlistXv1_v2_v3XX_name",
//        reason = "unsupported")
// @Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexTest",
//        method = "g_VX1X_outXknowsX",
//        reason = "unsupported")
// @Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexTest",
//        method = "g_VX1X_outE_inV",
//        reason = "unsupported")
// @Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexTest",
//        method = "g_VX1X_out_name",
//        reason = "unsupported")
// @Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexTest",
//        method = "g_VX4X_bothE",
//        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexTest",
        method = "g_EX11AsStringX",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexTest",
        method = "g_EXe11X",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexTest",
        method = "g_EXe7_e11X",
        reason = "unsupported")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexTest",
        method = "g_EXlistXe7_e11XX",
        reason = "unsupported")
// @Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexTest",
//        method = "g_VX4X_both",
//        reason = "unsupported")
// @Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexTest",
//        method = "g_VX1X_out_out_out",
//        reason = "unsupported")

public class GaiaAdaptorTestGraph extends DummyGraph {
    public static final String GRAPH_NAME = "test.graph.name";
    private RemoteGremlinConnection remoteGremlinConnection;

    public GaiaAdaptorTestGraph(final Configuration configuration) {
        try {
            String gremlinEndpoint = configuration.getString(GRAPH_NAME);
            this.remoteGremlinConnection = new RemoteGremlinConnection(gremlinEndpoint);
        } catch (Exception e) {
            throw new RuntimeException("initiate remote test graph fail " + e);
        }
    }

    public static RemoteTestGraph open(final Configuration configuration) {
        return new RemoteTestGraph(configuration);
    }

    @Override
    public void close() throws Exception {
        this.remoteGremlinConnection.close();
    }

    @Override
    public Configuration configuration() {
        return null;
    }

    @Override
    public GraphTraversalSource traversal() {
        GraphTraversalSource graphTraversalSource =
                new GraphTraversalSource(
                                this,
                                TraversalStrategies.GlobalCache.getStrategies(this.getClass()))
                        .withRemote(this.remoteGremlinConnection);
        TraversalStrategies strategies = graphTraversalSource.getStrategies();
        strategies.removeStrategies(ProfileStrategy.class, FilterRankingStrategy.class);
        return graphTraversalSource;
    }
}
