package s2scompiler.steps;

import s2scompiler.Result;
import s2scompiler.GenerateCode;

public final class Scatter extends TransStep{
    private Expr e;

    public Scatter(final String v) {
        name = SCATTER;
        e = new Expr(v);
    }

    public Scatter(final Expr ee) {
        name = SCATTER;
        e = ee;
    }

    @Override
    public Result translate() {
        Result res = e.translate();
        if (!res.t)
            return res;
        else 
            return new Result(true, GenerateCode.generateScatterValue(res.s));
    }
}