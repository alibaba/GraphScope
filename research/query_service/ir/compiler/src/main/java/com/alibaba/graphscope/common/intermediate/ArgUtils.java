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

import com.alibaba.graphscope.common.exception.OpArgIllegalException;
import com.alibaba.graphscope.common.jna.IrCoreLibrary;
import com.alibaba.graphscope.common.jna.type.*;

public class ArgUtils {
    private static IrCoreLibrary irCoreLib = IrCoreLibrary.INSTANCE;
    private static String LABEL = "~label";
    private static String ID = "~id";
    private static String LEN = "~len";
    private static String GROUP_KEYS = "keys";
    private static String GROUP_VALUES = "values";

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
        } else if (key.equals(LEN)) {
            return irCoreLib.asLenKey();
        } else {
            return irCoreLib.asPropertyKey(irCoreLib.cstrAsNameOrId(key));
        }
    }

    public static String getPropertyName(FfiProperty.ByValue property) {
        switch (property.opt) {
            case Id:
                return ID;
            case Label:
                return LABEL;
            case Key:
                return property.key.name;
            default:
                throw new OpArgIllegalException(OpArgIllegalException.Cause.INVALID_TYPE, "invalid type");
        }
    }

    public static FfiVariable.ByValue asVarPropertyOnly(FfiProperty.ByValue property) {
        return irCoreLib.asVarPropertyOnly(property);
    }

    public static FfiVariable.ByValue asNoneVar() {
        return irCoreLib.asNoneVar();
    }

    public static FfiVariable.ByValue asVarTagOnly(String tag) {
        if (tag.isEmpty()) {
            return irCoreLib.asNoneVar();
        } else {
            return irCoreLib.asVarTagOnly(irCoreLib.cstrAsNameOrId(tag));
        }
    }

    public static FfiVariable.ByValue asVar(String tag, String property) {
        FfiNameOrId.ByValue ffiTag;
        if (tag.isEmpty()) {
            ffiTag = asNoneNameOrId();
        } else {
            ffiTag = irCoreLib.cstrAsNameOrId(tag);
        }
        return irCoreLib.asVar(ffiTag, asFfiProperty(property));
    }

    public static FfiNameOrId.ByValue asNoneNameOrId() {
        return irCoreLib.noneNameOrId();
    }

    public static FfiProperty.ByValue asNoneProperty() {
        return irCoreLib.asNoneKey();
    }

    public static FfiAlias.ByValue asNoneAlias() {
        FfiAlias.ByValue ffiAlias = new FfiAlias.ByValue();
        ffiAlias.isQueryGiven = false;
        ffiAlias.alias = asNoneNameOrId();
        return ffiAlias;
    }

    public static FfiAlias.ByValue asFfiAlias(String aliasName, boolean isQueryGiven) {
        FfiNameOrId.ByValue alias = irCoreLib.cstrAsNameOrId(aliasName);
        FfiAlias.ByValue ffiAlias = new FfiAlias.ByValue();
        ffiAlias.alias = alias;
        ffiAlias.isQueryGiven = isQueryGiven;
        return ffiAlias;
    }

    public static String groupKeys() {
        return GROUP_KEYS;
    }

    public static String groupValues() {
        return GROUP_VALUES;
    }

    public static FfiAggFn.ByValue asFfiAggFn(ArgAggFn aggFn) {
        FfiAggFn.ByValue ffiAggFn = irCoreLib.initAggFn(aggFn.getAggregate(), aggFn.getAlias());
        // todo: add var
        return ffiAggFn;
    }
}

