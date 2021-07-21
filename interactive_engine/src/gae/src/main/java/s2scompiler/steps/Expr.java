package s2scompiler.steps;

import s2scompiler.Result;
import s2scompiler.GenerateCode;

public final class Expr extends TransStep{
    private String s;

    public Expr(final String ss) {
        name = EXPR;
        s = ss;
    }

    private boolean check(final char c) {
        if (c == '$' || c == '_') return true;
        if (c >= 'a' && c <= 'z') return true;
        if (c >= 'A' && c <= 'Z') return true;
        if (c >= '0' && c <= '9') return true;
        return false;
    }

    private boolean Ignore(final char c) {
        return c ==' ' || c =='\n' || c == '\t';
    }

    @Override
    public Result translate() {
        String tmp = "", res = "";
        for (int i = 0; i <= s.length(); i++) {
            char c = '#';
            if (i < s.length()) c = s.charAt(i);
            if (Ignore(c)) continue;
            if (check(c)) 
                tmp = tmp + c;
            else {
                if (c != '(') {
                    if (tmp.compareTo("TOTAL_V") == 0)
                        tmp = "context.num_vertices()";
                    else if (tmp.compareTo("ID") == 0)
                        tmp = "vertex.GetId()";
                    else if (tmp.compareTo("OUT_DEGREE") == 0)
                        tmp = "vertex.OutDegree()";
                    else if (tmp.compareTo("IN_DEGREE") == 0)
                        tmp = "vertex.InDegree()";
                    else if (tmp.compareTo("BOTH_DEGREE") == 0)
                        tmp = "(vertex.OutDegree() + vertex.InDegree())";
                    else if (tmp != "" && (tmp.charAt(0)< '0' || tmp.charAt(0) > '9')) 
                        tmp = GenerateCode.generateGetData(tmp);
                }
                res = res + tmp;
                tmp = "";
                if (i < s.length()) res = res + c;
            }
        }
        return new Result(true, res);
    }
}