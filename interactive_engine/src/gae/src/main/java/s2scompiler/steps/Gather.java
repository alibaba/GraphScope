package s2scompiler.steps;

import s2scompiler.Result;
import s2scompiler.GenerateCode;

public final class Gather extends TransStep{
    private String vp;
    private String operator;

    public Gather(final String p, final String o) {
        name = GATHER;
        vp = p;
        operator = o;
    }

    @Override
    public Result translate() {
        if (!vp.startsWith("$"))
            return new Result(false, "Need a runtime property name in the gather step.");
        String res_index = GenerateCode.generateGatherIndex(vp);
        String init, cal;
        if (operator.equals("sum")) {
            init = "0";
            cal = "a + b";
        } else if (operator.equals("mult")) {
            init = "1";
            cal = "a * b";
        } else if (operator.equals("min")) {
            init = GenerateCode.INT_MAX;
            cal = "min(a, b)";
        } else if (operator.equals("max")) {
            init = GenerateCode.INT_MIN;
            cal = "max(a, b)";
        } else {
            return new Result(false, "The Operator \"" + operator + "\" is not supported in the gather step.");
        }
        String res_init = GenerateCode.generateGatherInit(init);
        String res_agg = GenerateCode.generateGatherAgg(cal);
        return new Result(true, res_index + res_init + res_agg);
    }
}