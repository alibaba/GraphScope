package s2scompiler.steps;

import s2scompiler.Result;
import s2scompiler.GenerateCode;

public final class Times extends TransStep{
    private long iter;

    public Times(final long i) {
        name = TIMES;
        iter = i;
    }

    @Override
    public Result translate() {
        return new Result(true, GenerateCode.generateUntil(Long.toString(iter)));
    }
}