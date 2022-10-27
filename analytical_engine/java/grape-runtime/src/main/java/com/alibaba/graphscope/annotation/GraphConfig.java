/*
 * Copyright 2021 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.graphscope.annotation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Defines the configuration for code generation.
 */
public class GraphConfig {

    private static Logger logger = LoggerFactory.getLogger(GraphConfig.class.getName());

    final String vidType;
    final String oidType;
    final String vdataType;
    final String edataType;
    final String fragmentType;
    final String cppVidType;
    final String cppOidType;
    final String cppVdataType;
    final String cppEdataType;
    String messageTypes;
    String vertexDataType;

    public GraphConfig(
            String oidType,
            String vidType,
            String vdataType,
            String edataType,
            String messageTypes,
            String fragmentType,
            String cppOidType,
            String cppVidType,
            String cppVdataType,
            String cppEdataType,
            String vertexDataType) {
        this.vidType = vidType;
        this.oidType = oidType;
        this.vdataType = vdataType;
        this.edataType = edataType;
        this.messageTypes = messageTypes;
        this.fragmentType = fragmentType;
        this.cppVidType = cppVidType;
        this.cppOidType = cppOidType;
        this.cppVdataType = cppVdataType;
        this.cppEdataType = cppEdataType;
        this.vertexDataType = vertexDataType;
        logger.info(
                "init graph config with: "
                        + this.oidType
                        + " "
                        + this.vidType
                        + " "
                        + this.vdataType
                        + " "
                        + this.edataType
                        + " "
                        + this.cppOidType
                        + " "
                        + this.cppVidType
                        + " "
                        + this.cppVdataType
                        + " "
                        + this.cppEdataType
                        + " "
                        + this.vertexDataType);
    }
}
