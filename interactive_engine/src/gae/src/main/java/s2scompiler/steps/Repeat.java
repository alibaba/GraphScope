package s2scompiler.steps;

import s2scompiler.Result;
import s2scompiler.GenerateCode;

import java.util.ArrayList;

public final class Repeat extends TransStep{
    private ArrayList<TransStep> steps;

    public Repeat(final ArrayList<TransStep> s) {
        name = REPEAT;
        steps = s;
    }

    public Repeat() {
        name = REPEAT;
        steps = null;
    }

    @Override
    public Result translate() {
        if (steps == null || steps.isEmpty())
            return new Result(false, "The repeat step is empty.");
        int scatter_pos = -1, by_pos = -1, gather_pos = -1;
        String pre_code = "", post_code = "", scatter_code = "", gather_code = "";

        for (int i = 0; i < steps.size(); i++) {
            TransStep s = steps.get(i);
            Result res = s.translate();
            if (!res.t) return res;
            if (s.name.equals(V) || s.name.equals(PROPERTY) || s.name.equals(WHERE)) {
                if (scatter_pos == -1) {
                    pre_code = pre_code + res.s;
                } else {
                    post_code = post_code + res.s;
                }
            } else if (s.name.equals(SCATTER)) {
                if (scatter_pos != -1)
                    return new Result(false, "multiple scatter steps is not supported in the repeat step of process(...).");
                scatter_pos = i;
                scatter_code = res.s;
            } else if (s.name.equals(BY)) {
                if (by_pos != -1) 
                    return new Result(false, "multiple by steps is not supported in the repeat step of process(...).");
                by_pos = i;
                scatter_code = scatter_code + res.s;
            } else if (s.name.equals(GATHER)) {
                if (gather_pos != -1) 
                    return new Result(false, "multiple gather steps is not supported in the repeat step of process(...).");
                gather_pos = i;
                gather_code = gather_code + res.s;
            } else {
                return new Result(false, s.name + " is not supported in the repeat step of process(...).");
            }
        }
        if (scatter_pos == -1) 
            return new Result(false, "Need a scatter step in the repeat step of process(...)");
        if (by_pos != scatter_pos + 1 || gather_pos != by_pos + 1)
            return new Result(false, "Need scatter().by().gather() for the scatter step.");
        pre_code = GenerateCode.generatePre(pre_code);
        post_code = GenerateCode.generatePost(post_code);
        return new Result(true, pre_code + post_code + scatter_code + gather_code);
    }

}