package com.alibaba.maxgraph.v2.store.executor;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

public class AddressUtils {
    private static final String SPLIT = ":";

    public static Pair<String, Integer> parseAddress(String address) {
        String[] array = StringUtils.splitByWholeSeparator(address, SPLIT);
        return Pair.of(array[0], Integer.parseInt(array[1]));
    }

    public static String joinAddress(String host, int port) {
        return host + SPLIT + port;
    }
}
