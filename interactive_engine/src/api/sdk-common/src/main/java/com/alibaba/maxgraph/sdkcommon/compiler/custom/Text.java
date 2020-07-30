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
package com.alibaba.maxgraph.sdkcommon.compiler.custom;

public final class Text {
    public static RegexKeyPredicate match(String key, String regex) {
        return new RegexKeyPredicate(key, regex);
    }

    public static RegexPredicate match(String regex) {
        return new RegexPredicate(regex);
    }

    public static StringPredicate startsWith(String content) {
        return new StringPredicate(content, MatchType.STARTSWITH);
    }

    public static StringPredicate endsWith(String content) {
        return new StringPredicate(content, MatchType.ENDSWITH);
    }

    public static StringPredicate contains(String content) {
        return new StringPredicate(content, MatchType.CONTAINS);
    }

    public static StringKeyPredicate startsWith(String key, String content) {
        return new StringKeyPredicate(key, content, MatchType.STARTSWITH);
    }

    public static StringKeyPredicate endsWith(String key, String content) {
        return new StringKeyPredicate(key, content, MatchType.ENDSWITH);
    }

    public static StringKeyPredicate contains(String key, String content) {
        return new StringKeyPredicate(key, content, MatchType.CONTAINS);
    }
}
