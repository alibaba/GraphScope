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

public class AliasArg {
    public static String GROUP_KEYS = "~keys";
    public static String GROUP_VALUES = "~values";
    public static String DEFAULT = "~alias";

    // the prefix is composed of the type and the tag
    // there are 4 kinds of alias prefix, {keys, values, default, tag}
    private String tag;
    private AliasPrefixType type;
    // stepIdx_subTraversalIdx is as suffix
    // stepIdx is used to differentiate multiple select(..) in a gremlin query
    // subTraversalIdx is used to differentiate multiple apply from a same operator
    private int stepIdx;
    private int subTraversalIdx;

    private AliasArg() {
        this.tag = "";
        this.type = AliasPrefixType.DEFAULT;
        this.stepIdx = 0;
        this.subTraversalIdx = 0;
    }

    public AliasArg(AliasPrefixType type) {
        super();
        this.type = type;
    }

    public AliasArg(AliasPrefixType type, int stepIdx) {
        this(type);
        this.stepIdx = stepIdx;
    }

    public AliasArg(AliasPrefixType type, int stepIdx, int subTraversalIdx) {
        this(type, stepIdx);
        this.subTraversalIdx = subTraversalIdx;
    }

    public AliasArg(AliasPrefixType type, String tag) {
        super();
        if (tag.isEmpty()) {
            this.type = AliasPrefixType.DEFAULT;
        } else {
            this.tag = tag;
            this.type = type;
        }
    }

    public AliasArg(AliasPrefixType type, String tag, int stepIdx) {
        this(type, tag);
        this.stepIdx = stepIdx;
    }

    public AliasArg(AliasPrefixType type, String tag, int stepIdx, int subTraversalIdx) {
        this(type, tag, stepIdx);
        this.subTraversalIdx = subTraversalIdx;
    }

    public String getPrefix() {
        switch (type) {
            case PROJECT_TAG:
                return this.tag;
            case GROUP_KEYS:
                return GROUP_KEYS;
            case GROUP_VALUES:
                return GROUP_VALUES;
            case DEFAULT:
            default:
                return DEFAULT;
        }
    }

    public int getStepIdx() {
        return stepIdx;
    }

    public int getSubTraversalIdx() {
        return subTraversalIdx;
    }
}
