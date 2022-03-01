package com.alibaba.graphscope.common.intermediate;

import com.alibaba.graphscope.common.jna.type.FfiAlias;

public class MatchSentence {
    private FfiAlias.ByValue startTag;
    private FfiAlias.ByValue endTag;
    private InterOpCollection binders;
    private boolean isAnti;

    public MatchSentence(String startTag, String endTag, InterOpCollection binders, boolean isAnti) {
        this.startTag = ArgUtils.asFfiAlias(startTag, true);
        this.endTag = ArgUtils.asFfiAlias(endTag, true);
        this.isAnti = isAnti;
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

    public boolean isAnti() {
        return isAnti;
    }
}
