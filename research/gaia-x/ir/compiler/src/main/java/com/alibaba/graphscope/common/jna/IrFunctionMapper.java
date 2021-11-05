package com.alibaba.graphscope.common.jna;

import com.sun.jna.FunctionMapper;
import com.sun.jna.NativeLibrary;

import java.lang.reflect.Method;

public class IrFunctionMapper implements FunctionMapper {
    public static IrFunctionMapper INSTANCE = new IrFunctionMapper();

    private IrFunctionMapper() {
        super();
    }

    @Override
    public String getFunctionName(NativeLibrary nativeLibrary, Method method) {
        String target = method.getName();
        String[] splits = target.split("(?=\\p{Lu})");
        StringBuilder cName = new StringBuilder();
        for (int i = 0; i < splits.length; ++i) {
            if (i != 0) {
                cName.append("_");
            }
            cName.append(splits[i].toLowerCase());
        }
        return cName.toString();
    }
}
