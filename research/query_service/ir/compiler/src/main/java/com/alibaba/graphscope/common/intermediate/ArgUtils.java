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
import com.alibaba.graphscope.common.jna.type.FfiConst;
import com.alibaba.graphscope.common.jna.type.FfiNameOrId;
import com.alibaba.graphscope.common.jna.type.FfiProperty;
import com.alibaba.graphscope.common.jna.type.FfiVariable;

public class ArgUtils {
    private static IrCoreLibrary irCoreLib = IrCoreLibrary.INSTANCE;
    private static String LABEL = "~label";
    private static String ID = "~id";

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
}

