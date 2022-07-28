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

import com.alibaba.graphscope.common.jna.IrCoreLibrary;
import com.alibaba.graphscope.common.jna.type.*;

import org.junit.Assert;
import org.junit.Test;

public class ByValueTest {
    private static IrCoreLibrary irCoreLib = IrCoreLibrary.INSTANCE;

    @Test
    public void cstrAsNameOrIdTest() {
        String tag = "p";
        FfiNameOrId.ByValue nameOrId = irCoreLib.cstrAsNameOrId(tag);
        Assert.assertEquals(FfiNameIdOpt.Name, nameOrId.opt);
        Assert.assertEquals(tag, nameOrId.name);
    }

    @Test
    public void cstrAsConstTest() {
        String value = "marko";
        FfiConst.ByValue ffiConst = irCoreLib.cstrAsConst(value);
        Assert.assertEquals(FfiDataType.Str, ffiConst.dataType);
        Assert.assertEquals(value, ffiConst.cstr);
    }

    @Test
    public void int64AsConstTest() {
        long value = 10L;
        FfiConst.ByValue ffiConst = irCoreLib.int64AsConst(value);
        Assert.assertEquals(FfiDataType.I64, ffiConst.dataType);
        Assert.assertEquals(value, ffiConst.int64);
    }

    @Test
    public void asLabelKeyTest() {
        FfiProperty.ByValue property = irCoreLib.asLabelKey();
        Assert.assertEquals(FfiPropertyOpt.Label, property.opt);
    }

    @Test
    public void asIdKeyTest() {
        FfiProperty.ByValue property = irCoreLib.asIdKey();
        Assert.assertEquals(FfiPropertyOpt.Id, property.opt);
    }

    @Test
    public void asPropertyKeyTest() {
        String key = "age";
        FfiProperty.ByValue property = irCoreLib.asPropertyKey(irCoreLib.cstrAsNameOrId(key));
        Assert.assertEquals(FfiPropertyOpt.Key, property.opt);
        Assert.assertEquals(FfiNameIdOpt.Name, property.key.opt);
        Assert.assertEquals(key, property.key.name);
    }

    @Test
    public void asVarTagOnlyTest() {
        String tag = "p";
        FfiVariable.ByValue variable = irCoreLib.asVarTagOnly(irCoreLib.cstrAsNameOrId(tag));
        Assert.assertEquals(FfiNameIdOpt.Name, variable.tag.opt);
        Assert.assertEquals(tag, variable.tag.name);
        Assert.assertEquals(FfiPropertyOpt.None, variable.property.opt);
    }

    @Test
    public void asVarPropertyOnlyTest() {
        String key = "age";
        FfiVariable.ByValue variable =
                irCoreLib.asVarPropertyOnly(irCoreLib.asPropertyKey(irCoreLib.cstrAsNameOrId(key)));
        Assert.assertEquals(FfiPropertyOpt.Key, variable.property.opt);
        Assert.assertEquals(FfiNameIdOpt.Name, variable.property.key.opt);
        Assert.assertEquals(key, variable.property.key.name);
        Assert.assertEquals(FfiNameIdOpt.None, variable.tag.opt);
    }

    @Test
    public void asVarTest() {
        String tag = "p";
        String key = "age";
        FfiVariable.ByValue variable =
                irCoreLib.asVar(
                        irCoreLib.cstrAsNameOrId(tag),
                        irCoreLib.asPropertyKey(irCoreLib.cstrAsNameOrId(key)));
        Assert.assertEquals(FfiNameIdOpt.Name, variable.tag.opt);
        Assert.assertEquals(tag, variable.tag.name);
        Assert.assertEquals(FfiPropertyOpt.Key, variable.property.opt);
        Assert.assertEquals(FfiNameIdOpt.Name, variable.property.key.opt);
        Assert.assertEquals(key, variable.property.key.name);
    }

    @Test
    public void asNoneVarTest() {
        FfiVariable.ByValue variable = irCoreLib.asNoneVar();
        Assert.assertEquals(FfiPropertyOpt.None, variable.property.opt);
        Assert.assertEquals(FfiNameIdOpt.None, variable.tag.opt);
    }
}
