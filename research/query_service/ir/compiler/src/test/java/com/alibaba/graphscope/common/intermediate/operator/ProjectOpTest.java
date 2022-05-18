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
import com.alibaba.graphscope.common.jna.type.FfiAlias;
import com.alibaba.graphscope.common.jna.type.FfiScanOpt;
import com.alibaba.graphscope.common.utils.FileUtils;

import org.javatuples.Pair;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

public class ProjectOpTest {
    private IrPlan irPlan;

    @Test
    public void projectKeyTest() throws IOException {
        ProjectOp op = new ProjectOp();

        String projectExpr = "@.name";
        FfiAlias.ByValue alias = ArgUtils.asFfiAlias("name", false);
        List<Pair> exprWithAlias = Arrays.asList(Pair.with(projectExpr, alias));

        op.setExprWithAlias(new OpArg(exprWithAlias, Function.identity()));
        irPlan = DedupOpTest.getTestIrPlan(op);
        Assert.assertEquals(
                FileUtils.readJsonFromResource("project_key.json"), irPlan.getPlanAsJson());
    }

    @Test
    public void projectTagKeyTest() throws IOException {
        ScanFusionOp scanOp = new ScanFusionOp();
        scanOp.setScanOpt(new OpArg<>(FfiScanOpt.Entity, Function.identity()));
        scanOp.setAlias(new OpArg(ArgUtils.asFfiAlias("a", true), Function.identity()));

        ProjectOp projectOp = new ProjectOp();
        String projectExpr = "@a.name";
        FfiAlias.ByValue alias = ArgUtils.asFfiAlias("a_name", false);
        List<Pair> exprWithAlias = Collections.singletonList(Pair.with(projectExpr, alias));
        projectOp.setExprWithAlias(new OpArg(exprWithAlias, Function.identity()));

        irPlan = DedupOpTest.getTestIrPlan(scanOp, projectOp);
        Assert.assertEquals(
                FileUtils.readJsonFromResource("project_tag_key.json"), irPlan.getPlanAsJson());
    }

    @Test
    public void projectTagKeysTest() throws IOException {
        ScanFusionOp scanOp = new ScanFusionOp();
        scanOp.setScanOpt(new OpArg<>(FfiScanOpt.Entity, Function.identity()));
        scanOp.setAlias(new OpArg(ArgUtils.asFfiAlias("a", true), Function.identity()));

        ProjectOp op = new ProjectOp();
        String projectExpr = "{@a.name, @a.id}";
        FfiAlias.ByValue alias = ArgUtils.asFfiAlias("a_{name, id}", false);
        List<Pair> exprWithAlias = Collections.singletonList(Pair.with(projectExpr, alias));
        op.setExprWithAlias(new OpArg(exprWithAlias, Function.identity()));

        irPlan = DedupOpTest.getTestIrPlan(scanOp, op);
        Assert.assertEquals(
                FileUtils.readJsonFromResource("project_tag_keys.json"), irPlan.getPlanAsJson());
    }

    @After
    public void after() {
        if (irPlan != null) {
            irPlan.close();
        }
    }
}
