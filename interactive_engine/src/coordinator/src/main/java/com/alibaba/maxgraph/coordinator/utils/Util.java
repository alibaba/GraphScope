/**
 * Copyright 2020 Alibaba Group Holding Limited.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.maxgraph.coordinator.utils;

import java.text.MessageFormat;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 * @author beimian
 */
public final class Util {
    private Util() {}

    public static String formatServiceName(String nameSpace) {
        String key = MessageFormat.format("/{0}/{1}", nameSpace, Constant.COORDINATOR);
        return key;
    }

    public static String formatHttp(String path) {
        if (!path.startsWith("http://")) {
            return "http://" + path;
        }
        return path;
    }

    public static String concatMultiValues(final Collection<String> values) {
        StringBuilder s = new StringBuilder();
        for (String v : values) {
            s.append(v).append("&");
        }
        s.deleteCharAt(s.length() - 1);
        return s.toString();
    }

    public static <T> String concatMultiValues(final Collection<T> values, ToString<T> fun) {
        StringBuilder s = new StringBuilder();
        for (T v : values) {
            s.append(fun.to(v)).append("&");
        }
        s.deleteCharAt(s.length() - 1);
        return s.toString();
    }

    public static List<String> decompose2MultiValues(String value) {
        String[] split = value.split("&");
        return Arrays.asList(split);
    }

    public static byte[] int2Bytes(int num) {
        byte[] data = new byte[4];
        data[0] = (byte)((num >>> 24) & 0xFF);
        data[1] = (byte)((num >>> 16) & 0xFF);
        data[2] = (byte)((num >>> 8) & 0xFF);
        data[3] = (byte)(num & 0xFF);
        return data;
    }

    public static int bytes2int(byte[] data) {
        if (data.length > 4) {
            throw new NumberFormatException("Can't convert bytes of " + data.length + " to int");
        }
        int num = 0;
        num += data[0] << 24;
        num += data[1] << 16;
        num += data[2] << 8;
        num += data[3];
        return num;
    }


    @FunctionalInterface
    public interface ToString<T> {
        String to(T t);
    }
}
