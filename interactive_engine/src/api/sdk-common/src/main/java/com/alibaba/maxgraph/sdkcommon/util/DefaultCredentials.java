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

import com.alibaba.maxgraph.sdkcommon.exception.InvalidCredentialsException;
import org.apache.commons.lang3.StringUtils;

public class DefaultCredentials implements Credentials {

    private String accessKeyId;
    private String accessKeySecret;

    public DefaultCredentials(String accessKeyId, String accessKeySecret) {
        if (StringUtils.isEmpty(accessKeyId)) {
            throw new InvalidCredentialsException("Access key id should not be null or empty.");
        }
        if (StringUtils.isEmpty(accessKeySecret)) {
            throw new InvalidCredentialsException("Access key secret should not be null or empty.");
        }

        this.accessKeyId = accessKeyId;
        this.accessKeySecret = accessKeySecret;
    }

    @Override
    public String getAccessKeyId() {
        return accessKeyId;
    }

    @Override
    public String getAccessKeySecret() {
        return accessKeySecret;
    }
}
