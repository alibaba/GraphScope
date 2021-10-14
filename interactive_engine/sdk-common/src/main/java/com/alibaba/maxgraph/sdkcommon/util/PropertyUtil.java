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
package com.alibaba.maxgraph.sdkcommon.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

/**
 * @author xiafei.qiuxf
 * @date 2017/4/20
 */
public class PropertyUtil {
    public static Properties getProperties(String fileName, boolean resourcePath) {
        Properties props = new Properties();
        InputStream is = null;
        try {
            if (resourcePath) {
                is = Thread.currentThread().getContextClassLoader().getResourceAsStream(fileName);
            } else {
                is = new FileInputStream(new File(fileName));
            }
            props.load(is);
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            try {
                if (is != null) {
                    is.close();
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        return props;
    }
}
