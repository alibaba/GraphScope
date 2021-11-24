package com.alibaba.grahscope.common;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.net.URL;

public class TestUtils {
    public static byte[] readBytesFromFile(String file) {
        try {
            URL url = TestUtils.class.getClassLoader().getResource(file);
            return FileUtils.readFileToByteArray(new File(url.toURI()));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
