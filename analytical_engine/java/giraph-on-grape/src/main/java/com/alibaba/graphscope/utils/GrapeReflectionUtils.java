package com.alibaba.graphscope.utils;

public class GrapeReflectionUtils {

    public static <T> T loadAndCreate(String str){
        try{
            Class<? extends T> clz = (Class<? extends T>) Class.forName(str);
            return clz.newInstance();
        }
        catch (Exception e){
            e.printStackTrace();
        }
        return null;
    }
}
