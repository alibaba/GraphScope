package com.alibaba.graphscope.common.intermediate.process;

import com.alibaba.graphscope.common.jna.type.FfiNameOrId;
import com.alibaba.graphscope.common.jna.type.FfiProperty;
import com.alibaba.graphscope.common.jna.type.FfiPropertyOpt;
import org.apache.commons.collections.set.UnmodifiableSet;

import java.util.*;
import java.util.stream.Collectors;

public class TagRequiredProperties {
    // tag -> list of properties required
    private Map<FfiNameOrId.ByValue, Set<FfiProperty.ByValue>> properties;

    public TagRequiredProperties() {
        this.properties = new HashMap<>();
    }

    public void addTagProperty(FfiNameOrId.ByValue tag, FfiProperty.ByValue property) {
        this.properties.computeIfAbsent(tag, k -> new HashSet<>()).add(property);
    }

    /**
     * @param isPure if isPure is true, the returned list will ignore label and id
     * @return
     */
    public Set<FfiProperty.ByValue> getTagProperties(FfiNameOrId.ByValue tag, boolean isPure) {
        if (properties != null && properties.get(tag) != null) {
            return properties.get(tag).stream().filter(k -> (isPure) ? k.opt == FfiPropertyOpt.Key : true).collect(Collectors.toSet());
        }
        return new HashSet<>();
    }

    public Set<FfiNameOrId.ByValue> getTags() {
        return UnmodifiableSet.decorate(properties.keySet());
    }
}
