package s2scompiler.steps;

import s2scompiler.Result;
import s2scompiler.GenerateCode;

public final class By extends TransStep{
    private TransStep s;

    public By(final TransStep ss) {
        name = BY;
        s = ss;
    }

    public By() {
        name = BY;
        s = null;
    }

    @Override
    public Result translate() {
        if (s == null) 
            return new Result(false, "Need a traversal defined in scatter().by(...).");
        if (s.name.equals("OUT")) {
            return new Result(true, GenerateCode.generateScatterEdges("OUT"));
        } else if (s.name.equals("IN")) {
            return new Result(true, GenerateCode.generateScatterEdges("IN"));
        } else if (s.name.equals("BOTH")) {
            return new Result(true, GenerateCode.generateScatterEdges("BOTH"));
        }
        return new Result(false, s.name + " is not supported in scatter().by(...).");
    }
}