/*
 * Copyright 2022 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  	http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.alibaba.graphscope.groot.service.models;

import com.alibaba.graphscope.groot.service.deserializer.StringTypeStringDeserializer;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import java.util.*;

import javax.annotation.Generated;
import javax.validation.constraints.*;

@JsonTypeInfo(use = JsonTypeInfo.Id.NONE)
@JsonDeserialize(using = StringTypeStringDeserializer.class)
@Generated(
        value = "org.openapitools.codegen.languages.SpringCodegen",
        date = "2025-01-13T19:18:58.368636+08:00[Asia/Shanghai]",
        comments = "Generator version: 7.10.0")
public interface StringTypeString {}