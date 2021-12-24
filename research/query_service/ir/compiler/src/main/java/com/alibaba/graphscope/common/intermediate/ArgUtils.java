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

package com.alibaba.graphscope.common.intermediate;

import com.alibaba.graphscope.common.jna.IrCoreLibrary;
import com.alibaba.graphscope.common.jna.type.*;

public class ArgUtils {
    private static IrCoreLibrary irCoreLib = IrCoreLibrary.INSTANCE;
    private static String LABEL = "~label";
    private static String ID = "~id";
    public static String GROUP_KEYS = "~keys";
    public static String GROUP_VALUES = "~values";

    public static FfiNameOrId.ByValue strAsNameId(String value) {
        return irCoreLib.cstrAsNameOrId(value);
    }

    public static FfiConst.ByValue intAsConst(int id) {
        return irCoreLib.int32AsConst(id);
    }

    public static FfiConst.ByValue longAsConst(long id) {
        return irCoreLib.int64AsConst(id);
    }

    public static FfiProperty.ByValue asFfiProperty(String key) {
        if (key.equals(LABEL)) {
            return irCoreLib.asLabelKey();
        } else if (key.equals(ID)) {
            return irCoreLib.asIdKey();
        } else {
            return irCoreLib.asPropertyKey(irCoreLib.cstrAsNameOrId(key));
        }
    }

    public static FfiVariable.ByValue asVarPropertyOnly(FfiProperty.ByValue property) {
        return irCoreLib.asVarPropertyOnly(property);
    }

    public static FfiVariable.ByValue asNoneVar() {
        return irCoreLib.asNoneVar();
    }

    public static FfiAlias.ByValue asFfiAlias(String aliasName, boolean isQueryGiven) {
        FfiNameOrId.ByValue alias = irCoreLib.cstrAsNameOrId(aliasName);
        FfiAlias.ByValue ffiAlias = new FfiAlias.ByValue();
        ffiAlias.alias = alias;
        ffiAlias.isQueryGiven = isQueryGiven;
        return ffiAlias;
    }

    public static FfiAlias.ByValue groupKeysAlias() {
        return asFfiAlias(GROUP_KEYS, false);
    }

    public static FfiAlias.ByValue groupValuesAlias() {
        return asFfiAlias(GROUP_VALUES, false);
    }

    public static FfiAggFn.ByValue asFfiAggFn(ArgAggFn aggFn) {
        FfiAggFn.ByValue ffiAggFn = irCoreLib.initAggFn(aggFn.getAggregate(), aggFn.getAlias());
        // todo: add var
        return ffiAggFn;
    }
}

