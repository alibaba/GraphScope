package s2scompiler.steps;

import s2scompiler.Result;
import s2scompiler.GenerateCode;

public final class V extends TransStep{

    public V() {
        name = V;
    }

    @Override
    public Result translate() {
        return new Result(true, GenerateCode.set_active_code);
    }
}