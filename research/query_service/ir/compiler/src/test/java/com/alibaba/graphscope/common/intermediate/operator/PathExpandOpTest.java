///*
// * Copyright 2020 Alibaba Group Holding Limited.
// *
// * Licensed under the Apache License, Version 2.0 (the "License");
// * you may not use this file except in compliance with the License.
// * You may obtain a copy of the License at
// *
// * http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//package com.alibaba.graphscope.common.intermediate.operator;
//
//import com.alibaba.graphscope.common.IrPlan;
//import com.alibaba.graphscope.common.jna.type.FfiDirection;
//import com.alibaba.graphscope.common.utils.FileUtils;
//
//import org.junit.Assert;
//import org.junit.Test;
//
//import java.io.IOException;
//import java.util.function.Function;
//
//public class PathExpandOpTest {
//    private IrPlan irPlan = new IrPlan();
//
//    @Test
//    public void expand_1_5_Test() throws IOException {
//        PathExpandOp op = new PathExpandOp();
//        op.setEdgeOpt(new OpArg<>(Boolean.valueOf(false), Function.identity()));
//        op.setDirection(new OpArg<>(FfiDirection.Out, Function.identity()));
//        op.setLower(new OpArg(Integer.valueOf(1), Function.identity()));
//        op.setUpper(new OpArg(Integer.valueOf(5), Function.identity()));
//        irPlan.appendInterOp(-1, op);
//        String actual = irPlan.getPlanAsJson();
//        Assert.assertEquals(FileUtils.readJsonFromResource("path_expand.json"), actual);
//    }
//}
