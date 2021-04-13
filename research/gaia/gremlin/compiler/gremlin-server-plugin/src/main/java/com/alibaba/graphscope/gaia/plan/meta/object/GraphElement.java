package com.alibaba.graphscope.gaia.plan.meta.object;

import com.google.common.base.Objects;

import java.util.UUID;

public class GraphElement {
    private UUID uuid;

    protected GraphElement(UUID uuid) {
        this.uuid = uuid;
    }

    public UUID getUuid() {
        return uuid;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        GraphElement that = (GraphElement) o;
        return Objects.equal(uuid, that.uuid);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(uuid);
    }

    public static GraphElement Empty = new GraphElement(UUID.randomUUID());
}
