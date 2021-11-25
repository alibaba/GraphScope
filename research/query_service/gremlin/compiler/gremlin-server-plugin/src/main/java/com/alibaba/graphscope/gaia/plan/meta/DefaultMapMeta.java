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
package com.alibaba.graphscope.gaia.plan.meta;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class DefaultMapMeta<O, D> implements Meta<O, D> {
    protected volatile Map<O, D> mapMeta = new HashMap<>();

    @Override
    public void add(O object, D data) {
        mapMeta.put(object, data);
    }

    @Override
    public void delete(O object) {
        mapMeta.remove(object);
    }

    @Override
    public Optional<D> get(O object) {
        return Optional.ofNullable(mapMeta.get(object));
    }

    @Override
    public List<O> getAllObjects() {
        return mapMeta.keySet().stream().collect(Collectors.toList());
    }

    @Override
    public void clear() {
        this.mapMeta.clear();
    }

    public Map<O, D> getMap() {
        return this.mapMeta;
    }
}
