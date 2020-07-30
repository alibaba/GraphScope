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
package com.alibaba.maxgraph.common.util;

import org.apache.commons.io.IOUtils;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * parse config file in this format:
 * key1     value1
 * key2             value2
 * # comment
 * key3 value3 # comment
 */
public class KVConfigParser {

    private static final Pattern EMPTY_LINE = Pattern.compile("\\s*");
    private static final Pattern COMMENT_LINE = Pattern.compile("\\s*#.*");
    private static final Pattern KV_LINE = Pattern.compile("^\\s*(\\S+)\\s+(\\S+)\\s*(?:(\\s+#.*)*)$");
    public static Map<String, String> parse(String filename) throws IOException{
        Map<String, String> ret = new HashMap<>();
        String text = IOUtils.toString(new FileInputStream(filename), "utf-8");
        for (String s : text.split("\n")) {
            if (EMPTY_LINE.matcher(s).matches() || COMMENT_LINE.matcher(s).matches()) {
                continue;
            }
            Matcher matcher = KV_LINE.matcher(s);
            if (matcher.matches()) {
                ret.put(matcher.group(1), matcher.group(2));
            } else {
                return null;
            }
        }
        return ret;
    }

    public static void main(String[] args) throws IOException {
        System.out.println(parse("/Users/wubincen/project/rust/maxgraph/java/loader/conf/mr.conf"));
    }

}

