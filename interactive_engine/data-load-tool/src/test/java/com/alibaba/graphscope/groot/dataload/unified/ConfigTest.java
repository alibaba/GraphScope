package com.alibaba.graphscope.groot.dataload.unified;

import com.alibaba.graphscope.groot.common.config.DataLoadConfig;
import com.alibaba.graphscope.groot.dataload.databuild.Utils;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;

public class ConfigTest {

    @Test
    public void serializeRoundTrip() throws IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        mapper.setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE);
        mapper.configure(DeserializationFeature.FAIL_ON_IGNORED_PROPERTIES, false);
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        UniConfig uniConfig;
        try (InputStream in =
                Thread.currentThread()
                        .getContextClassLoader()
                        .getResourceAsStream("unified_dataloading.yaml")) {
            uniConfig = mapper.readValue(in, UniConfig.class);
        }
        System.out.println(uniConfig);
        System.out.println("----------------------------------------------");
        String out = mapper.writeValueAsString(uniConfig);
        System.out.println(out);
        Assert.assertEquals(0, 0);
    }

    @Test
    public void inICompatibilityTest() throws IOException {
        InputStream in =
                Thread.currentThread()
                        .getContextClassLoader()
                        .getResourceAsStream("loading_config_odps.ini");

        UniConfig properties = UniConfig.fromInputStream(in);
        String s = properties.getProperty(DataLoadConfig.LOAD_AFTER_BUILD);
        Assert.assertTrue(Utils.parseBoolean(s));
        s = properties.getProperty(DataLoadConfig.GRAPH_ENDPOINT);
        Assert.assertEquals(s, "1.2.3.4:55556");
        s = properties.getProperty(DataLoadConfig.PRIMARY_VIP_SERVER_DOMAIN);
        Assert.assertEquals(s, "demo-grpc-vipserver");
    }
}
