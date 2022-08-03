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
import com.alibaba.graphscope.common.jna.type.*;

import org.apache.commons.lang3.StringUtils;

public class ArgUtils {
    public static String LABEL = "~label";
    public static String ID = "~id";
    public static String LEN = "~len";
    public static String PROPERTY_ALL = "~all";

    public static FfiConst.ByValue asConst(int id) {
        FfiConst.ByValue ffiConst = new FfiConst.ByValue();
        ffiConst.dataType = FfiDataType.I32;
        ffiConst.int32 = id;
        return ffiConst;
    }

    public static FfiConst.ByValue asConst(long id) {
        FfiConst.ByValue ffiConst = new FfiConst.ByValue();
        ffiConst.dataType = FfiDataType.I64;
        ffiConst.int64 = id;
        return ffiConst;
    }

    public static FfiConst.ByValue asConst(String str) {
        FfiConst.ByValue ffiConst = new FfiConst.ByValue();
        ffiConst.dataType = FfiDataType.Str;
        ffiConst.cstr = str;
        return ffiConst;
    }

    // "" or null indicates NONE or HEAD
    public static FfiNameOrId.ByValue asNameOrId(String tag) {
        FfiNameOrId.ByValue ffiName = new FfiNameOrId.ByValue();
        if (!StringUtils.isEmpty(tag)) {
            ffiName.name = tag;
            ffiName.opt = FfiNameIdOpt.Name;
        }
        return ffiName;
    }

    public static FfiNameOrId.ByValue asNoneNameOrId() {
        return new FfiNameOrId.ByValue();
    }

    // "" or null indicates NONE
    public static FfiProperty.ByValue asKey(String property) {
        FfiProperty.ByValue ffiProperty = new FfiProperty.ByValue();
        if (!StringUtils.isEmpty(property)) {
            if (property.equals(LABEL)) {
                ffiProperty.opt = FfiPropertyOpt.Label;
            } else if (property.equals(ID)) {
                ffiProperty.opt = FfiPropertyOpt.Id;
            } else if (property.equals(LEN)) {
                ffiProperty.opt = FfiPropertyOpt.Len;
            } else {
                ffiProperty.opt = FfiPropertyOpt.Key;
                ffiProperty.key = asNameOrId(property);
            }
        }
        return ffiProperty;
    }

    public static FfiProperty.ByValue asNoneKey() {
        return new FfiProperty.ByValue();
    }

    public static FfiVariable.ByValue asVar(String tag, String property) {
        FfiVariable.ByValue ffiVar = new FfiVariable.ByValue();
        ffiVar.tag = asNameOrId(tag);
        ffiVar.property = asKey(property);
        return ffiVar;
    }

    public static FfiVariable.ByValue asNoneVar() {
        return new FfiVariable.ByValue();
    }

    public static FfiAlias.ByValue asAlias(String aliasName, boolean isQueryGiven) {
        FfiAlias.ByValue ffiAlias = new FfiAlias.ByValue();
        ffiAlias.alias = asNameOrId(aliasName);
        ffiAlias.isQueryGiven = isQueryGiven;
        return ffiAlias;
    }

    public static FfiAlias.ByValue asNoneAlias() {
        FfiAlias.ByValue ffiAlias = new FfiAlias.ByValue();
        ffiAlias.alias = asNoneNameOrId();
        ffiAlias.isQueryGiven = false;
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
