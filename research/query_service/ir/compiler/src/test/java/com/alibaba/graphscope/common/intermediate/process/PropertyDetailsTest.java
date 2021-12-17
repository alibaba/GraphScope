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

package com.alibaba.graphscope.common.intermediate.process;

import com.alibaba.graphscope.common.intermediate.operator.OpArg;
import com.alibaba.graphscope.common.intermediate.operator.OrderOp;
import com.alibaba.graphscope.common.intermediate.operator.ProjectOp;
import com.alibaba.graphscope.common.intermediate.operator.SelectOp;
import com.alibaba.graphscope.common.jna.IrCoreLibrary;
import com.alibaba.graphscope.common.jna.type.FfiNameOrId;
import com.alibaba.graphscope.common.jna.type.FfiOrderOpt;
import com.alibaba.graphscope.common.jna.type.FfiProperty;
import com.google.common.collect.Sets;
import org.javatuples.Pair;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.function.Function;

public class PropertyDetailsTest {
    private static IrCoreLibrary irCoreLib = IrCoreLibrary.INSTANCE;

    @Test
    public void selectDetailsTest() {
        SelectOp op = new SelectOp();
        op.setPredicate(new OpArg("@a.name == \"marko\" && @a.id == 1", Function.identity()));
        TagRequiredProperties properties = (TagRequiredProperties) PropertyDetailsProcessor.PropertyDetailsFactory.REQUIRE.apply(op);
        FfiNameOrId.ByValue expectedTag = irCoreLib.cstrAsNameOrId("a");
        Assert.assertEquals(Sets.newHashSet(expectedTag), properties.getTags());
        Assert.assertEquals(Sets.newHashSet(irCoreLib.asPropertyKey(irCoreLib.cstrAsNameOrId("name")), irCoreLib.asPropertyKey(irCoreLib.cstrAsNameOrId("id"))),
                properties.getTagProperties(expectedTag, true));

        op.setPredicate(new OpArg("@a", Function.identity()));
        properties = (TagRequiredProperties) PropertyDetailsProcessor.PropertyDetailsFactory.REQUIRE.apply(op);
        Assert.assertEquals(Collections.EMPTY_SET, properties.getTags());

        op.setPredicate(new OpArg("@.name == \"marko\" && @.id == 1", Function.identity()));
        properties = (TagRequiredProperties) PropertyDetailsProcessor.PropertyDetailsFactory.REQUIRE.apply(op);
        expectedTag = FfiNameOrId.ByValue.getHead();
        Assert.assertEquals(Sets.newHashSet(expectedTag), properties.getTags());
        Assert.assertEquals(
                Sets.newHashSet(irCoreLib.asPropertyKey(irCoreLib.cstrAsNameOrId("name")), irCoreLib.asPropertyKey(irCoreLib.cstrAsNameOrId("id"))),
                properties.getTagProperties(expectedTag, true));

        op.setPredicate(new OpArg("@a.name == \"marko\" && @.id == 1", Function.identity()));
        properties = (TagRequiredProperties) PropertyDetailsProcessor.PropertyDetailsFactory.REQUIRE.apply(op);
        FfiNameOrId.ByValue expected1 = FfiNameOrId.ByValue.getHead();
        FfiNameOrId.ByValue expected2 = irCoreLib.cstrAsNameOrId("a");
        Assert.assertEquals(Sets.newHashSet(expected1, expected2), properties.getTags());
        Assert.assertEquals(Sets.newHashSet(irCoreLib.asPropertyKey(irCoreLib.cstrAsNameOrId("name"))), properties.getTagProperties(expected2, true));
        Assert.assertEquals(Sets.newHashSet(irCoreLib.asPropertyKey(irCoreLib.cstrAsNameOrId("id"))), properties.getTagProperties(expected1, true));
    }

    @Test
    public void projectDetailsTest() {
        ProjectOp op = new ProjectOp();
        op.setProjectExprWithAlias(new OpArg(Arrays.asList(Pair.with("@a.name", irCoreLib.cstrAsNameOrId("~@a.name")),
                Pair.with("@.id", irCoreLib.cstrAsNameOrId("~@.id"))),
                Function.identity()));
        TagRequiredProperties properties = (TagRequiredProperties) PropertyDetailsProcessor.PropertyDetailsFactory.REQUIRE.apply(op);
        FfiNameOrId.ByValue expected1 = FfiNameOrId.ByValue.getHead();
        FfiNameOrId.ByValue expected2 = irCoreLib.cstrAsNameOrId("a");
        Assert.assertEquals(Sets.newHashSet(expected1, expected2), properties.getTags());
        Assert.assertEquals(Sets.newHashSet(irCoreLib.asPropertyKey(irCoreLib.cstrAsNameOrId("name"))), properties.getTagProperties(expected2, true));
        Assert.assertEquals(Sets.newHashSet(irCoreLib.asPropertyKey(irCoreLib.cstrAsNameOrId("id"))), properties.getTagProperties(expected1, true));
    }

    @Test
    public void orderDetailsTest() {
        OrderOp op = new OrderOp();
        op.setOrderVarWithOrder(new OpArg(Arrays.asList(Pair.with(irCoreLib.asNoneVar(), FfiOrderOpt.Asc)), Function.identity()));
        TagRequiredProperties properties = (TagRequiredProperties) PropertyDetailsProcessor.PropertyDetailsFactory.REQUIRE.apply(op);
        Assert.assertEquals(Collections.EMPTY_SET, properties.getTags());

        FfiNameOrId.ByValue head = FfiNameOrId.ByValue.getHead();
        FfiProperty.ByValue p1 = irCoreLib.asPropertyKey(irCoreLib.cstrAsNameOrId("name"));
        FfiNameOrId.ByValue tagA = irCoreLib.cstrAsNameOrId("a");
        FfiProperty.ByValue p2 = irCoreLib.asPropertyKey(irCoreLib.cstrAsNameOrId("id"));
        op.setOrderVarWithOrder(new OpArg(Arrays.asList(Pair.with(irCoreLib.asVarPropertyOnly(p1), FfiOrderOpt.Asc),
                Pair.with(irCoreLib.asVar(tagA, p2), FfiOrderOpt.Asc)), Function.identity()));
        properties = (TagRequiredProperties) PropertyDetailsProcessor.PropertyDetailsFactory.REQUIRE.apply(op);
        Assert.assertEquals(Sets.newHashSet(head, tagA), properties.getTags());
        Assert.assertEquals(Sets.newHashSet(p1), properties.getTagProperties(head, true));
        Assert.assertEquals(Sets.newHashSet(p2), properties.getTagProperties(tagA, true));
    }
}
