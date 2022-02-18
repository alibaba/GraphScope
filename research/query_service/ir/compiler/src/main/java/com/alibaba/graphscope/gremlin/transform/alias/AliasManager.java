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

package com.alibaba.graphscope.gremlin.transform.alias;

import com.alibaba.graphscope.common.intermediate.ArgUtils;
import com.alibaba.graphscope.common.jna.type.FfiAlias;

public class AliasManager {
    public static FfiAlias.ByValue getFfiAlias(GetAliasArg prefix) {
        int stepIdx = prefix.getStepIdx();
        int subTraversalId = prefix.getSubTraversalIdx();
        String alias = prefix.getPrefix() + "_" + stepIdx + "_" + subTraversalId;
        return ArgUtils.asFfiAlias(alias, false);
    }

    // prefix is as the gremlin result key which is used to display
    public static String getPrefix(String aliasName) {
        String[] splits = aliasName.split("_");
        return (splits.length == 0) ? "" : splits[0];
    }

    public static boolean isGroupKeysPrefix(String aliasName) {
        return aliasName.startsWith(GetAliasArg.GROUP_KEYS);
    }

    public static boolean isGroupValuesPrefix(String aliasName) {
        return aliasName.startsWith(GetAliasArg.GROUP_VALUES);
    }
}
