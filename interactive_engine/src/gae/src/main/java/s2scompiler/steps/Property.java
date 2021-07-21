package s2scompiler.steps;

import s2scompiler.Result;
import s2scompiler.GenerateCode;

public final class Property extends TransStep{
    private String vp;
    private TransStep e;

    public Property(final String p, final TransStep ee) {
        name = PROPERTY;
        vp = p;
        e = ee;
    }

    @Override
    public Result translate() {
        if (!vp.startsWith("$"))
            return new Result(false, "Only runtime properties are permitted in property(...) inside the process step.");
        if (!(e instanceof Expr)) 
            return new Result(false, "Only expr(String) is supported in property(...) inside the process step.");
        Result res = e.translate();
        if (!res.t)
            return res;
        else 
            return new Result(true, GenerateCode.generateProperty(vp, res.s));
    }
}