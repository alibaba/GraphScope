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

import com.alibaba.graphscope.common.jna.type.FfiAlias;
import com.alibaba.graphscope.common.jna.type.FfiJoinKind;

public class MatchSentence {
    private FfiAlias.ByValue startTag;
    private FfiAlias.ByValue endTag;
    private InterOpCollection binders;
    private FfiJoinKind joinKind;

    public MatchSentence(
            String startTag, String endTag, InterOpCollection binders, FfiJoinKind joinKind) {
        this.startTag = ArgUtils.asAlias(startTag, true);
        this.endTag = ArgUtils.asAlias(endTag, true);
        this.joinKind = joinKind;
        this.binders = binders;
    }

    public FfiAlias.ByValue getStartTag() {
        return startTag;
    }

    public FfiAlias.ByValue getEndTag() {
        return endTag;
    }

    public InterOpCollection getBinders() {
        return binders;
    }

    public FfiJoinKind getJoinKind() {
        return joinKind;
    }
}
