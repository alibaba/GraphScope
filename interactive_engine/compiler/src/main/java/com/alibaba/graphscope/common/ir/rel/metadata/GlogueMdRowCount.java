package com.alibaba.graphscope.common.ir.rel.metadata;

import org.apache.calcite.rel.metadata.RelMdRowCount;
import org.apache.calcite.rel.metadata.RelMetadataQuery;

import com.alibaba.graphscope.common.ir.rel.graph.GraphLogicalExpand;
import com.alibaba.graphscope.common.ir.rel.graph.GraphLogicalGetV;
import com.alibaba.graphscope.common.ir.rel.graph.GraphLogicalPathExpand;
import com.alibaba.graphscope.common.ir.rel.graph.GraphLogicalSource;
import com.alibaba.graphscope.common.ir.rel.graph.GraphLogicalPattern;

public class GlogueMdRowCount extends RelMdRowCount {

  // TODO:we can extend RelMdUtil; and if the input size > k, then call
  // RelMdUtil.estimatePatternRows() to get the result.

  // Handler for source.
  Double getRowCount(GraphLogicalSource logicalSource, RelMetadataQuery mq) {
    System.out.println("GlogueMdRowCount.getRowCount(GraphLogicalSource logicalSource, RelMetadataQuery mq)");
    return mq.getRowCount(logicalSource);
  }

  // Handler for expand
  Double getRowCount(GraphLogicalExpand logicalExpand, RelMetadataQuery mq) {
    return mq.getRowCount(logicalExpand);
  }

  // Handler for getv
  Double getRowCount(GraphLogicalGetV logicalGetV, RelMetadataQuery mq) {
    return mq.getRowCount(logicalGetV);
  }

  // Handler for path
  Double getRowCount(GraphLogicalPathExpand logicalPath, RelMetadataQuery mq) {
    return mq.getRowCount(logicalPath);
  }

  // Handler for pattern.
  Double getRowCount(GraphLogicalPattern logicalPattern, RelMetadataQuery mq) {
    return mq.getRowCount(logicalPattern);
  }

  public static void main(String[] args) {
    GlogueMdRowCount glogueMdRowCount = new GlogueMdRowCount();
    System.out.println(glogueMdRowCount.getClass().getName());
  }

}
