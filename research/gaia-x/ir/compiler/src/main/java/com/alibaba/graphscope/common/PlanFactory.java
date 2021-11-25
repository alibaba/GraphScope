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

package com.alibaba.graphscope.common;

import com.alibaba.graphscope.common.intermediate.operator.ExpandOp;
import com.alibaba.graphscope.common.intermediate.operator.LimitOp;
import com.alibaba.graphscope.common.intermediate.operator.OpArg;
import com.alibaba.graphscope.common.intermediate.operator.ScanFusionOp;
import com.alibaba.graphscope.common.jna.IrCoreLibrary;
import com.alibaba.graphscope.common.jna.type.FfiDirection;
import com.alibaba.graphscope.common.jna.type.FfiScanOpt;

import java.util.Arrays;
import java.util.function.Function;

public class PlanFactory {
    private static IrCoreLibrary irCoreLib = IrCoreLibrary.INSTANCE;

    public static IrPlan getPocPlan() {
        IrPlan irPlan = new IrPlan();

        // init ScanFusionOp
        ScanFusionOp scanOp = new ScanFusionOp();
        scanOp.setScanOpt(new OpArg(FfiScanOpt.Vertex, Function.identity()));
        scanOp.setLabels(new OpArg(Arrays.asList(irCoreLib.cstrAsNameOrId("person")), Function.identity()));
        scanOp.setPredicate(new OpArg("@.id == 1", Function.identity()));
        irPlan.appendInterOp(scanOp);

        // init ExpandOp
        ExpandOp expandOp = new ExpandOp();
        expandOp.setDirection(new OpArg(FfiDirection.Out, Function.identity()));
        expandOp.setLabels(new OpArg(Arrays.asList(irCoreLib.cstrAsNameOrId("knows")), Function.identity()));
        expandOp.setEdgeOpt(new OpArg(Boolean.valueOf(false), Function.identity()));
        irPlan.appendInterOp(expandOp);

        // init LimitOp
        LimitOp limitOp = new LimitOp();
        limitOp.setLower(new OpArg(Integer.valueOf(1), Function.identity()));
        limitOp.setUpper(new OpArg(Integer.valueOf(4), Function.identity()));
        irPlan.appendInterOp(limitOp);

        return irPlan;
    }
}
