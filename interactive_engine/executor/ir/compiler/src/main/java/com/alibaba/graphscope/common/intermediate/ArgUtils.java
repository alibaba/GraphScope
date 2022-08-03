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
    public static String PROPERTY_ALL = "~all";

    public static FfiConst.ByValue asFfiConst(int id) {
        return irCoreLib.int32AsConst(id);
    }

    public static FfiConst.ByValue asFfiConst(long id) {
        return irCoreLib.int64AsConst(id);
    }

    // "" indicates NONE
    public static FfiProperty.ByValue asFfiProperty(String property) {
        if (property.isEmpty()) {
            return irCoreLib.asNoneKey();
        } else if (property.equals(LABEL)) {
            return irCoreLib.asLabelKey();
        } else if (property.equals(ID)) {
            return irCoreLib.asIdKey();
        } else if (property.equals(LEN)) {
            return irCoreLib.asLenKey();
        } else {
            return irCoreLib.asPropertyKey(irCoreLib.cstrAsNameOrId(property));
        }
    }

    // "" indicates NONE or HEAD
    public static FfiNameOrId.ByValue asFfiTag(String tag) {
        if (tag.isEmpty()) {
            return irCoreLib.noneNameOrId();
        } else {
            return irCoreLib.cstrAsNameOrId(tag);
        }
    }

    public static FfiNameOrId.ByValue asFfiNoneTag() {
        return irCoreLib.noneNameOrId();
    }

    public static FfiVariable.ByValue asFfiVar(String tag, String property) {
        FfiNameOrId.ByValue ffiTag = asFfiTag(tag);
        FfiProperty.ByValue ffiProperty = asFfiProperty(property);
        return irCoreLib.asVar(ffiTag, ffiProperty);
    }

    public static FfiVariable.ByValue asFfiNoneVar() {
        return irCoreLib.asNoneVar();
    }

    public static FfiAlias.ByValue asFfiNoneAlias() {
        FfiNameOrId.ByValue alias = irCoreLib.noneNameOrId();
        FfiAlias.ByValue ffiAlias = new FfiAlias.ByValue();
        ffiAlias.alias = alias;
        ffiAlias.isQueryGiven = false;
        return ffiAlias;
    }

    public static FfiAlias.ByValue asFfiAlias(String aliasName, boolean isQueryGiven) {
        FfiNameOrId.ByValue alias = irCoreLib.cstrAsNameOrId(aliasName);
        FfiAlias.ByValue ffiAlias = new FfiAlias.ByValue();
        ffiAlias.alias = alias;
        ffiAlias.isQueryGiven = isQueryGiven;
        return ffiAlias;
    }

    public static String propertyName(FfiProperty.ByValue property) {
        switch (property.opt) {
            case None:
                return "";
            case Id:
                return ID;
            case Label:
                return LABEL;
            case Len:
                return LEN;
            case Key:
                return property.key.name;
            default:
                throw new OpArgIllegalException(
                        OpArgIllegalException.Cause.INVALID_TYPE, "invalid type");
        }
    }

    public static String tagName(FfiNameOrId.ByValue tag) {
        switch (tag.opt) {
            case None:
                return "";
            case Name:
                return tag.name;
            case Id:
            default:
                throw new OpArgIllegalException(
                        OpArgIllegalException.Cause.INVALID_TYPE, "invalid type");
        }
    }
}
