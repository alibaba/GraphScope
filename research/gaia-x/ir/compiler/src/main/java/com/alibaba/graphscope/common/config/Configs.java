package com.alibaba.graphscope.common.config;

import org.apache.commons.lang3.NotImplementedException;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class Configs {
    private Properties properties;

    public Configs(String file, FileLoadType loadType) throws IOException, NotImplementedException {
        properties = new Properties();
        switch (loadType) {
            case RELATIVE_PATH:
                properties.load(new FileInputStream(new File(file)));
                break;
            default:
                throw new NotImplementedException("unimplemented load type " + loadType);
        }
    }

    public String get(String name, String defaultValue) {
        return this.properties.getProperty(name, defaultValue);
    }
}
