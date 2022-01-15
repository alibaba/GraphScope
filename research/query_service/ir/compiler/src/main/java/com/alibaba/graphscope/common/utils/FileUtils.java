package com.alibaba.graphscope.common.utils;

import com.google.common.io.Resources;

import java.net.URL;
import java.nio.charset.StandardCharsets;

public class FileUtils {
    public static String readJsonFromResource(String file) {
        try {
            URL url = Thread.currentThread().getContextClassLoader().getResource(file);
            return Resources.toString(url, StandardCharsets.UTF_8).trim();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
