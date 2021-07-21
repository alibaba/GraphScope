package s2scompiler.steps;

import s2scompiler.Result;
import s2scompiler.GenerateCode;

public final class Where extends TransStep{
    private TransStep e;

    public Where(final TransStep ee) {
        name = WHERE;
        e = ee;
    }

    @Override
    public Result translate() {
        if (!(e instanceof Expr)) 
            return new Result(false, "Only expr(String) is supported in where(...) inside the process step.");
        Result res = e.translate();
        if (!res.t)
            return res;
        else
            return new Result(true, GenerateCode.generateWhere(res.s));
    }
}