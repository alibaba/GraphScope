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

package com.alibaba.graphscope.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoadLibrary {
    private static Logger logger = LoggerFactory.getLogger(LoadLibrary.class.getName());
    /**
     * Used by c++ to load compiled user app.
     *
     * @param userLibrary library to load, fully-specified path
     */
    public static void invoke(String userLibrary) throws UnsatisfiedLinkError {
        logger.info("loading library " + userLibrary);
        System.load(userLibrary);
    }
}
