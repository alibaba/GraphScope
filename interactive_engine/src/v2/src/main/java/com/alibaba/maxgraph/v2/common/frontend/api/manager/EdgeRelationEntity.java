package com.alibaba.maxgraph.v2.common.frontend.api.manager;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;

public class EdgeRelationEntity {
    private String sourceLabel;
    private String targetLabel;

    public EdgeRelationEntity(String sourceLabel, String targetLabel) {
        this.sourceLabel = sourceLabel;
        this.targetLabel = targetLabel;
    }

    public String getSourceLabel() {
        return sourceLabel;
    }

    public String getTargetLabel() {
        return targetLabel;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        EdgeRelationEntity that = (EdgeRelationEntity) o;
        return Objects.equal(sourceLabel, that.sourceLabel) &&
                Objects.equal(targetLabel, that.targetLabel);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(sourceLabel, targetLabel);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("sourceLabel", this.getSourceLabel())
                .add("targetLabel", this.getTargetLabel())
                .toString();
    }
}
