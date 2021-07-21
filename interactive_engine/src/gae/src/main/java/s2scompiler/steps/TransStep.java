package s2scompiler.steps;

import s2scompiler.Result;

public class TransStep{
    public static final String PROCESS = "process";
    public static final String V = "V";
    public static final String PROPERTY = "property";
    public static final String EXPR = "expr";
    public static final String WHERE = "where";
    public static final String REPEAT = "repeat";
    public static final String UNTIL = "until";
    public static final String TIMES = "times";
    public static final String SCATTER = "scatter";
    public static final String GATHER = "gather";
    public static final String BY = "by";

    public String name;

    public TransStep(final String s) {
        name = s;
    }

    public TransStep() {
    }
    
    public Result translate() {
        return new Result(false, "\"" + name + "\" is not supported inside the process Step.");
    }
}