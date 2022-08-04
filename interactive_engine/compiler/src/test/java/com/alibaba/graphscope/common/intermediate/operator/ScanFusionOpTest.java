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

package com.alibaba.graphscope.common.intermediate.operator;

import com.alibaba.graphscope.common.IrPlan;
import com.alibaba.graphscope.common.intermediate.ArgUtils;
import com.alibaba.graphscope.common.jna.type.FfiConst;
import com.alibaba.graphscope.common.jna.type.FfiScanOpt;
import com.alibaba.graphscope.common.utils.FileUtils;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

public class ScanFusionOpTest {
    private IrPlan irPlan;

    @Test
    public void scanOptTest() throws IOException {
        ScanFusionOp op = new ScanFusionOp();
        op.setScanOpt(new OpArg<>(FfiScanOpt.Entity, Function.identity()));
        irPlan = DedupOpTest.getTestIrPlan(op);
        String actual = irPlan.getPlanAsJson();
        Assert.assertEquals(FileUtils.readJsonFromResource("scan_opt.json"), actual);
    }

    @Test
    public void addSnapshotTest() throws IOException {
        ScanFusionOp op = new ScanFusionOp();
        op.setScanOpt(new OpArg<>(FfiScanOpt.Entity, Function.identity()));

        QueryParams params = new QueryParams();
        params.addExtraParams("SID", "1");
        op.setParams(params);

        irPlan = DedupOpTest.getTestIrPlan(op);
        String actual = irPlan.getPlanAsJson();
        Assert.assertEquals(FileUtils.readJsonFromResource("scan_snapshot.json"), actual);
    }

    @Test
    public void predicateTest() throws IOException {
        ScanFusionOp op = new ScanFusionOp();
        op.setScanOpt(new OpArg<>(FfiScanOpt.Entity, Function.identity()));

        QueryParams params = new QueryParams();
        params.setPredicate("@.id == 1");
        op.setParams(params);

        irPlan = DedupOpTest.getTestIrPlan(op);
        String actual = irPlan.getPlanAsJson();
        Assert.assertEquals(FileUtils.readJsonFromResource("scan_expr.json"), actual);
    }

    @Test
    public void labelsTest() throws IOException {
        ScanFusionOp op = new ScanFusionOp();
        op.setScanOpt(new OpArg<>(FfiScanOpt.Entity, Function.identity()));

        QueryParams params = new QueryParams();
        params.addTable(ArgUtils.asNameOrId("person"));
        op.setParams(params);

        irPlan = DedupOpTest.getTestIrPlan(op);
        String actual = irPlan.getPlanAsJson();
        Assert.assertEquals(FileUtils.readJsonFromResource("scan_labels.json"), actual);
    }

    @Test
    public void idsTest() throws IOException {
        ScanFusionOp op = new ScanFusionOp();
        op.setScanOpt(new OpArg<>(FfiScanOpt.Entity, Function.identity()));
        List<FfiConst.ByValue> values = Arrays.asList(ArgUtils.asConst(1L), ArgUtils.asConst(2L));
        op.setIds(new OpArg<List, List>(values, Function.identity()));
        irPlan = DedupOpTest.getTestIrPlan(op);
        String actual = irPlan.getPlanAsJson();
        Assert.assertEquals(FileUtils.readJsonFromResource("scan_ids.json"), actual);
    }

    @Test
    public void aliasTest() throws IOException {
        ScanFusionOp op = new ScanFusionOp();
        op.setScanOpt(new OpArg<>(FfiScanOpt.Entity, Function.identity()));
        op.setAlias(new OpArg(ArgUtils.asAlias("a", true), Function.identity()));
        irPlan = DedupOpTest.getTestIrPlan(op);
        String actual = irPlan.getPlanAsJson();
        Assert.assertEquals(FileUtils.readJsonFromResource("scan_alias.json"), actual);
    }

    @After
    public void after() {
        if (irPlan != null) {
            irPlan.close();
        }
    }
}
