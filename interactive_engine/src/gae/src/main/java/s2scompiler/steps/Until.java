package s2scompiler.steps;

import s2scompiler.Result;
import s2scompiler.GenerateCode;

import java.util.ArrayList;

public final class Until extends TransStep{
    private ArrayList<TransStep> s;

    public Until(final ArrayList<TransStep> ss) {
        name = UNTIL;
        s = ss;
    }

    public Until() {
        name = UNTIL;
        s = null;
    }

    @Override
    public Result translate() {
        if (s == null || s.isEmpty())
            return new Result(false, "Need a traversal defined in the until step.");
        if (s.size() == 2 && s.get(0).name.equals("count") && s.get(1).name.equals("is"))
            return new Result(true, GenerateCode.generateUntil(GenerateCode.INT_MAX));
        else
            return new Result(false, "Only until(count().is(0)) or times(...) is supported for the repeat step inside the process step.");
    }
}