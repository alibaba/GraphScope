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
        this.startTag = ArgUtils.asFfiAlias(startTag, true);
        this.endTag = ArgUtils.asFfiAlias(endTag, true);
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
