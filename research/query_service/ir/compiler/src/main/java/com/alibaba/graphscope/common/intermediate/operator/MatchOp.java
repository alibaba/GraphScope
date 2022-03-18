package com.alibaba.graphscope.common.intermediate.operator;

        import java.util.Optional;

public class MatchOp extends InterOpBase {
    // List<MatchSentence>
    private Optional<OpArg> sentences;

    public MatchOp() {
        super();
        this.sentences = Optional.empty();
    }

    public Optional<OpArg> getSentences() {
        return sentences;
    }

    public void setSentences(OpArg sentences) {
        this.sentences = Optional.of(sentences);
    }
}
