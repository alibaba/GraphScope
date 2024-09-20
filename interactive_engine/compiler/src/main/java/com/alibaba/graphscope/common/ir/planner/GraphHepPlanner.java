package com.alibaba.graphscope.common.ir.planner;

import com.alibaba.graphscope.common.ir.meta.schema.CommonOptTable;
import com.alibaba.graphscope.common.ir.rel.*;
import com.alibaba.graphscope.common.ir.rel.graph.*;
import com.alibaba.graphscope.common.ir.rel.graph.match.GraphLogicalMultiMatch;
import com.alibaba.graphscope.common.ir.rel.graph.match.GraphLogicalSingleMatch;

import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rel.rules.MultiJoin;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexSubQuery;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.IdentityHashMap;

/**
 * Original {@code HepPlanner} skip optimizations to the nested structures, i.e. {@code RelNode} nested in the {@code CommonTableScan} or {@code RexSubQuery},
 * we supplement this functionality by overriding the {@code findBestExp} method.
 */
public class GraphHepPlanner extends HepPlanner {
    private @Nullable RelNode originalRoot;

    public GraphHepPlanner(HepProgram program) {
        super(program);
    }

    @Override
    public RelNode findBestExp() {
        return originalRoot.accept(new PlannerVisitor(originalRoot));
    }

    @Override
    public void setRoot(RelNode rel) {
        this.originalRoot = rel;
    }

    public RelNode findBestExpOfRoot(RelNode root) {
        super.setRoot(root);
        return super.findBestExp();
    }

    private class PlannerVisitor extends GraphShuttle {
        private final RelNode root;
        private final IdentityHashMap<RelNode, RelNode> commonRelVisitedMap;
        // apply optimization to the sub-query
        private final RexShuttle subQueryPlanner;

        public PlannerVisitor(RelNode root) {
            this.root = root;
            this.commonRelVisitedMap = new IdentityHashMap<>();
            this.subQueryPlanner =
                    new RexShuttle() {
                        @Override
                        public RexNode visitSubQuery(RexSubQuery subQuery) {
                            RelNode subRel = subQuery.rel;
                            RelNode newSubRel = subRel.accept(new PlannerVisitor(subRel));
                            if (newSubRel == subRel) {
                                return subQuery;
                            }
                            return subQuery.clone(newSubRel);
                        }
                    };
        }

        @Override
        public RelNode visit(GraphLogicalSource source) {
            return findBestIfRoot(source, source);
        }

        @Override
        public RelNode visit(GraphLogicalExpand expand) {
            return findBestIfRoot(expand, visitChildren(expand));
        }

        @Override
        public RelNode visit(GraphLogicalGetV getV) {
            return findBestIfRoot(getV, visitChildren(getV));
        }

        @Override
        public RelNode visit(GraphLogicalPathExpand expand) {
            return findBestIfRoot(expand, visitChildren(expand));
        }

        @Override
        public RelNode visit(GraphLogicalSingleMatch match) {
            return findBestIfRoot(match, match);
        }

        @Override
        public RelNode visit(GraphLogicalMultiMatch match) {
            return findBestIfRoot(match, match);
        }

        @Override
        public RelNode visit(GraphLogicalAggregate aggregate) {
            return findBestIfRoot(aggregate, visitChildren(aggregate));
        }

        @Override
        public RelNode visit(GraphLogicalProject project) {
            return findBestIfRoot(project, visitChildren(project));
        }

        @Override
        public RelNode visit(GraphLogicalSort sort) {
            return findBestIfRoot(sort, visitChildren(sort));
        }

        @Override
        public RelNode visit(GraphPhysicalExpand physicalExpand) {
            return findBestIfRoot(physicalExpand, visitChildren(physicalExpand));
        }

        @Override
        public RelNode visit(GraphPhysicalGetV physicalGetV) {
            return findBestIfRoot(physicalGetV, visitChildren(physicalGetV));
        }

        @Override
        public RelNode visit(LogicalUnion union) {
            return findBestIfRoot(union, visitChildren(union));
        }

        @Override
        public RelNode visit(LogicalFilter filter) {
            return findBestIfRoot(filter, visitChildren(filter));
        }

        @Override
        public RelNode visit(MultiJoin join) {
            return findBestIfRoot(join, visitChildren(join));
        }

        @Override
        public RelNode visit(LogicalJoin join) {
            return findBestIfRoot(join, visitChildren(join));
        }

        @Override
        public RelNode visit(GraphProcedureCall procedureCall) {
            return findBestIfRoot(procedureCall, visitChildren(procedureCall));
        }

        @Override
        public RelNode visit(CommonTableScan tableScan) {
            RelOptTable optTable = tableScan.getTable();
            if (optTable instanceof CommonOptTable) {
                RelNode common = ((CommonOptTable) optTable).getCommon();
                RelNode visited = commonRelVisitedMap.get(common);
                if (visited == null) {
                    visited = common.accept(new PlannerVisitor(common));
                    commonRelVisitedMap.put(common, visited);
                }
                return new CommonTableScan(
                        tableScan.getCluster(),
                        tableScan.getTraitSet(),
                        new CommonOptTable(visited));
            }
            return tableScan;
        }

        private RelNode findBestIfRoot(RelNode oldRel, RelNode newRel) {
            newRel = newRel.accept(this.subQueryPlanner);
            return oldRel == root ? findBestExpOfRoot(newRel) : newRel;
        }
    }
}
