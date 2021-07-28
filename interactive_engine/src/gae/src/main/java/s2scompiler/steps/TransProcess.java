package s2scompiler.steps;

import s2scompiler.Result;
import s2scompiler.GenerateCode;

import java.util.ArrayList;
import java.util.HashMap;

public final class TransProcess extends TransStep{
    private ArrayList<TransStep> steps;
    private HashMap<String, String> properties;

    public TransProcess(final ArrayList<TransStep> s, final HashMap<String, String> p) {
        name = PROCESS;
        steps = s;
        properties = p;
    }

    public TransProcess() {
        name = PROCESS;
        steps = null;
        properties = null;
    }

    @Override
    public Result translate() {
        if (steps == null || steps.isEmpty())
            return new Result(false, "The process step is empty.");
        int repeat_pos = -1, until_pos = -1, times_pos = -1;
        String set_code = "", init_code = "", repeat_code = "", until_code = "";

        for (int i = 0; i < steps.size(); i++) {
            TransStep s = steps.get(i);
            Result res = s.translate();
            if (!res.t) return res;
            if (s.name.equals(V) || s.name.equals(PROPERTY) || s.name.equals(WHERE)) {
                if (repeat_pos != -1)
                    return new Result(false, "Steps can not be defined after repeat(...) in the process step.");
                init_code = init_code + res.s;
            } else if (s.name.equals(REPEAT)) {
                if (repeat_pos != -1)
                    return new Result(false, "multiple repeat steps is not supported in the process step.");
                repeat_pos = i;
                repeat_code = res.s;
            } else if (s.name.equals(UNTIL)) {
                if (until_pos != -1)
                    return new Result(false, "multiple until steps is not supported in the process step.");
                until_pos = i;
                until_code = res.s;
            } else if (s.name.equals(TIMES)) {
                if (times_pos != -1)
                    return new Result(false, "multiple times steps is not supported in the process step.");
                times_pos = i;
                until_code = res.s;
            } else {
                return new Result(false, s.name + " is can not be defined directly in the process step.");
            }
        }
        if (repeat_pos == -1) return new Result(false, "Need a repeat step in the process step");
        if ((times_pos == repeat_pos + 1 && until_pos == -1) || (times_pos == -1 && until_pos == repeat_pos + 1)) {
            set_code = GenerateCode.generateSetup(properties);
            init_code = GenerateCode.generateInit(init_code);
            String class_name = GenerateCode.generateRandomString(10);
            Result rlt = new Result(true, GenerateCode.generateHead(class_name) + set_code + init_code + repeat_code + until_code + GenerateCode.tail_code);
            rlt.set_class_name(class_name);
            rlt.set_app_type("cpp_gas");
            return rlt;
        } 
        return new Result(false, "Need repeat().until() or repeat().times() for the repeat step.");
    }
}
