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

package com.alibaba.graphscope.runtime;

import org.scijava.nativelib.NativeLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Vector;

/**
 * Load JNI library with library name.
 */
public class LoadLibrary {

    static java.lang.reflect.Field LIBRARIES = null;
    private static Logger logger = LoggerFactory.getLogger(LoadLibrary.class);

    static {
        try {
            NativeLoader.loadLibrary("grape-jni");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Loading the library with library name.
     *
     * @param userLibrary name for library to be loaded.
     */
    public static void invoke(String userLibrary) {
        logger.info("loading " + userLibrary);
        try {
            NativeLoader.loadLibrary(userLibrary);
        } catch (IOException e) {
            e.printStackTrace();
        }
        logger.info("Load libary cl: " + LoadLibrary.class.getClassLoader());
        try {
            LIBRARIES = ClassLoader.class.getDeclaredField("loadedLibraryNames");
            LIBRARIES.setAccessible(true);
            final Vector<String> libraries =
                    (Vector<String>) LIBRARIES.get(LoadLibrary.class.getClassLoader());
            logger.info(
                    "Loaded library in cl "
                            + LoadLibrary.class.getClassLoader()
                            + ": "
                            + String.join(",", libraries.toArray(new String[] {})));

        } catch (NoSuchFieldException | IllegalAccessException e) {
            e.printStackTrace();
        }
    }
}
