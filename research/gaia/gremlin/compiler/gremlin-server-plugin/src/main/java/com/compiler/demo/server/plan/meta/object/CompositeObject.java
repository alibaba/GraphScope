/*
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.compiler.demo.server.plan.meta.object;

import java.util.Collections;
import java.util.List;

public class CompositeObject {
    private Class<?> className;
    private List<CompositeObject> subs;

    public CompositeObject(Class<?> className) {
        this(className, Collections.EMPTY_LIST);
    }

    public CompositeObject(Class<?> className, List<CompositeObject> subs) {
        this.className = className;
        this.subs = subs;
    }

    public Class<?> getClassName() {
        return className;
    }

    public List<CompositeObject> getSubs() {
        return subs;
    }

    public CompositeObject getSub(int idx) {
        return subs.get(idx);
    }
}
