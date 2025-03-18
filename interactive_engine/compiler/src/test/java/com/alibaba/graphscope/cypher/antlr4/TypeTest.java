/*
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.graphscope.cypher.integration.flex.bench;

import com.alibaba.graphscope.common.config.Configs;
import com.alibaba.graphscope.common.ir.meta.IrMeta;
import com.alibaba.graphscope.common.ir.planner.GraphIOProcessor;
import com.alibaba.graphscope.common.ir.planner.GraphRelOptimizer;
import com.alibaba.graphscope.common.ir.runtime.proto.GraphRelProtoPhysicalBuilder;
import com.alibaba.graphscope.common.ir.tools.GraphBuilder;
import com.alibaba.graphscope.common.ir.tools.LogicalPlan;
import com.alibaba.graphscope.common.utils.FileUtils;
import com.alibaba.graphscope.cypher.antlr4.Utils;
import com.google.common.collect.ImmutableMap;

import org.apache.calcite.rel.RelNode;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.math.BigDecimal;
import java.math.BigInteger;

public class UnitTypeTest {
    private static Configs configs;
    private static IrMeta irMeta;
    private static GraphRelOptimizer optimizer;

    @BeforeClass
    public static void beforeClass() {
        configs =
                new Configs(
                        ImmutableMap.of(
                                "graph.planner.is.on",
                                "true",
                                "graph.planner.opt",
                                "CBO",
                                "graph.planner.rules",
                                "FilterIntoJoinRule, FilterMatchRule, ExtendIntersectRule,"
                                        + " ExpandGetVFusionRule"));
        optimizer = new GraphRelOptimizer(configs);
        irMeta =
                com.alibaba.graphscope.common.ir.Utils.mockIrMeta(
                        "flex_bench/modern.yaml", "", optimizer);
    }

    @Test
    public void compare_uint64_uint64_test() {
        GraphBuilder builder =
                com.alibaba.graphscope.common.ir.Utils.mockGraphBuilder(optimizer, irMeta);
        RelNode before =
                com.alibaba.graphscope.cypher.antlr4.Utils.eval(
                                "MATCH (p:person)\n"
                                        + "    WHERE p.prop_uint64 = +18446744073709551602L\n"
                                        + "    RETURN p.prop_uint64",
                                builder)
                        .build();
        RelNode after = optimizer.optimize(before, new GraphIOProcessor(builder, irMeta));
        GraphRelProtoPhysicalBuilder builder1 =
                new GraphRelProtoPhysicalBuilder(configs, irMeta, new LogicalPlan(after));
        Assert.assertEquals(
                FileUtils.readJsonFromResource("proto/compare_uint64_uint64_test.json"),
                builder1.build().explain().trim());
    }

    @Test
    public void compare_double_float_test() {
        GraphBuilder builder =
                com.alibaba.graphscope.common.ir.Utils.mockGraphBuilder(optimizer, irMeta);
        RelNode before =
                com.alibaba.graphscope.cypher.antlr4.Utils.eval(
                                "MATCH (p:person)\n"
                                        + "    WHERE p.prop_double = 1.2f\n"
                                        + "    RETURN p.prop_double",
                                builder)
                        .build();
        RelNode after = optimizer.optimize(before, new GraphIOProcessor(builder, irMeta));
        GraphRelProtoPhysicalBuilder builder1 =
                new GraphRelProtoPhysicalBuilder(configs, irMeta, new LogicalPlan(after));
        Assert.assertEquals(
                FileUtils.readJsonFromResource("proto/compare_double_float_test.json"),
                builder1.build().explain().trim());
    }

    @Test
    public void divide_int32_int32_overflow_test() {
        GraphBuilder builder =
                com.alibaba.graphscope.common.ir.Utils.mockGraphBuilder(optimizer, irMeta);
        builder.disableSimplify();
        RelNode before =
                com.alibaba.graphscope.cypher.antlr4.Utils.eval(
                                "MATCH (p:person)\n" + "    RETURN -2147483648 / -1", builder)
                        .build();
        RelNode after = optimizer.optimize(before, new GraphIOProcessor(builder, irMeta));
        Assert.assertEquals(
                "GraphLogicalProject($f0=[/(-2147483648, -1)], isAppend=[false])\n"
                        + "  GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}],"
                        + " alias=[p], opt=[VERTEX])",
                after.explain().trim());
    }

    @Test
    public void divide_int64_int32_overflow_test() {
        GraphBuilder builder =
                com.alibaba.graphscope.common.ir.Utils.mockGraphBuilder(optimizer, irMeta);
        builder.disableSimplify();
        RelNode before =
                com.alibaba.graphscope.cypher.antlr4.Utils.eval(
                                "MATCH (p:person)\n" + "    RETURN -9223372036854775808L / -1",
                                builder)
                        .build();
        RelNode after = optimizer.optimize(before, new GraphIOProcessor(builder, irMeta));
        Assert.assertEquals(
                "GraphLogicalProject($f0=[/(-9223372036854775808:BIGINT, -1)], isAppend=[false])\n"
                        + "  GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}],"
                        + " alias=[p], opt=[VERTEX])",
                after.explain().trim());
    }

    @Test
    public void convert_int32_to_uint32_test() {
        BigDecimal expected = new BigDecimal("4294967284");
        long signedVal = expected.longValue();
        BigDecimal unsignedVal = new BigDecimal(new BigInteger(1, Utils.longToBytes(signedVal)));
        Assert.assertEquals(expected, unsignedVal);
    }

    @Test
    public void convert_int64_to_uint64_test() {
        BigDecimal expected = new BigDecimal("18446744073709551602");
        long signedVal = expected.longValue();
        BigDecimal unsignedVal = new BigDecimal(new BigInteger(1, Utils.longToBytes(signedVal)));
        Assert.assertEquals(expected, unsignedVal);
    }

    /**
     * When plus/minus/multiply/divide an int32 value with an uint32 value, the expected result range should be [INT32_MIN, UINT32_MAX],
     * but there is not a type that can represent this range, currently we use uint32 instead.
     *
     * The return type is the least restrictive type of two parameters, specifically:
     *
     * uint32, uint32 -> uint32
     * uint32, int32 -> uint32
     * uint32, uint64 -> uint64
     * uint32, int64 -> int64
     * int32, int64 -> int64
     * int32, uint64 -> uint64
     */
    @Test
    public void divide_int32_uint64_test() {
        GraphBuilder builder =
                com.alibaba.graphscope.common.ir.Utils.mockGraphBuilder(optimizer, irMeta);
        builder.disableSimplify();
        RelNode before =
                com.alibaba.graphscope.cypher.antlr4.Utils.eval(
                                "MATCH (p:person) RETURN 2 / +3L", builder)
                        .build();
        RelNode after = optimizer.optimize(before, new GraphIOProcessor(builder, irMeta));
        GraphRelProtoPhysicalBuilder builder1 =
                new GraphRelProtoPhysicalBuilder(configs, irMeta, new LogicalPlan(after));
        Assert.assertEquals(
                FileUtils.readJsonFromResource("proto/divide_int32_uint32_int32.json"),
                builder1.build().explain().trim());
    }

    @Test
    public void compare_date32_i32_test() {
        GraphBuilder builder =
                com.alibaba.graphscope.common.ir.Utils.mockGraphBuilder(optimizer, irMeta);
        RelNode before =
                com.alibaba.graphscope.cypher.antlr4.Utils.eval(
                                "MATCH (p:person)\n"
                                        + "    WHERE p.prop_date = 20132\n"
                                        + "    RETURN p.prop_date",
                                builder)
                        .build();
        RelNode after = optimizer.optimize(before, new GraphIOProcessor(builder, irMeta));
        Assert.assertEquals(
                "GraphLogicalProject(prop_date=[p.prop_date], isAppend=[false])\n"
                        + "  GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}],"
                        + " alias=[p], fusedFilter=[[=(_.prop_date, 20132)]], opt=[VERTEX])",
                after.explain().trim());
    }

    @Test
    public void compare_timestamp_i64_test() {
        GraphBuilder builder =
                com.alibaba.graphscope.common.ir.Utils.mockGraphBuilder(optimizer, irMeta);
        RelNode before =
                com.alibaba.graphscope.cypher.antlr4.Utils.eval(
                                "MATCH (p:person)\n"
                                        + "    WHERE p.prop_ts = 1739454301000L\n"
                                        + "    RETURN p.prop_ts",
                                builder)
                        .build();
        RelNode after = optimizer.optimize(before, new GraphIOProcessor(builder, irMeta));
        Assert.assertEquals(
                "GraphLogicalProject(prop_ts=[p.prop_ts], isAppend=[false])\n"
                        + "  GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}],"
                        + " alias=[p], fusedFilter=[[=(_.prop_ts, 1739454301000:BIGINT)]],"
                        + " opt=[VERTEX])",
                after.explain().trim());
    }

    @Test
    public void compare_int32_int64_array_test() {
        String query = "MATCH (p:person) Where p.prop_int32 in [123L, 456] RETURN p.prop_int32";
        GraphBuilder builder =
                com.alibaba.graphscope.common.ir.Utils.mockGraphBuilder(optimizer, irMeta);
        RelNode before = com.alibaba.graphscope.cypher.antlr4.Utils.eval(query, builder).build();
        RelNode after = optimizer.optimize(before, new GraphIOProcessor(builder, irMeta));
        Assert.assertEquals(
                "GraphLogicalProject(prop_int32=[p.prop_int32], isAppend=[false])\n"
                        + "  GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}],"
                        + " alias=[p], opt=[VERTEX], uniqueKeyFilters=[SEARCH(_.prop_int32,"
                        + " Sarg[123L:BIGINT, 456L:BIGINT]:BIGINT)])",
                after.explain().trim());
    }
}
