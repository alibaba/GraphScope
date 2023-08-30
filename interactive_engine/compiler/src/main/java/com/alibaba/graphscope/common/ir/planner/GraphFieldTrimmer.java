package com.alibaba.graphscope.common.ir.planner;

import com.alibaba.graphscope.common.ir.rel.GraphLogicalAggregate;
import com.alibaba.graphscope.common.ir.rel.GraphLogicalProject;
import com.alibaba.graphscope.common.ir.rel.GraphLogicalSort;
import com.alibaba.graphscope.common.ir.rel.graph.AbstractBindableTableScan;
import com.alibaba.graphscope.common.ir.rel.graph.match.GraphLogicalMultiMatch;
import com.alibaba.graphscope.common.ir.rel.graph.match.GraphLogicalSingleMatch;
import com.alibaba.graphscope.common.ir.rex.RexGraphVariable;
import com.alibaba.graphscope.common.ir.rex.RexVariableAliasCollector;
import com.alibaba.graphscope.common.ir.type.GraphProperty;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql2rel.RelFieldTrimmer;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ReflectUtil;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

public class GraphFieldTrimmer extends RelFieldTrimmer {


    public class GraphVariable{
       private int aliasId;
       private @Nullable GraphProperty property;
       public GraphVariable(int aliasId,@Nullable GraphProperty property){
         this.aliasId=aliasId;
         this.property=property;
       }

       public GraphVariable(int aliasId){
           this.aliasId=aliasId;
       }

       @Override
       public boolean equals(Object o){
           if(this==o) return true;
           if (o == null || getClass() != o.getClass()) return false;
           if (!super.equals(o)) return false;
           RexGraphVariable rhs = (RexGraphVariable) o;
           if(rhs.getProperty()==null){
               return aliasId==rhs.getAliasId();
           }
           return  aliasId==rhs.getAliasId()&&Objects.equals(property,rhs.getProperty());

       }
    }
    private final ReflectUtil.MethodDispatcher<RelNode> graphTrimFieldsDispatcher;

    public GraphFieldTrimmer(@Nullable SqlValidator validator, RelBuilder relBuilder) {
        super(validator, relBuilder);
        graphTrimFieldsDispatcher  =
                ReflectUtil.createMethodDispatcher(
                        RelNode.class,
                        this,
                        "trimFields",
                        RelNode.class,
                        Set.class);


    }

    public RelNode trim(RelNode root) {
        final Set<GraphVariable> fields=Collections.emptySet();
        return dispatchTrimFields(root,fields);
    }


    public RelNode trimFields(GraphLogicalProject project, Set<GraphVariable> fieldsUsed){
        // TODO(huaiyu)
        return project;
    }

    public RelNode trimFields(GraphLogicalAggregate aggregate,Set<GraphVariable> fieldsUsed){
        // TODO(huaiyu)
        return aggregate;
    }

    public RelNode trimFields(GraphLogicalSort sort,Set<GraphVariable> fieldsUsed){
        // TODO(huaiyu)
        return sort;
    }

    public RelNode trimFields(AbstractBindableTableScan tableScan,Set<GraphVariable> fieldsUsed){
        // TODO(huaiyu)
        return tableScan;
    }

    public RelNode trimFields(LogicalFilter filter,Set<GraphVariable> fieldsUsed){
       // Find columns and PropertyRef used by filter.
        RexNode condition=filter.getCondition();
        List<GraphVariable> fields=condition.accept(new RexVariableAliasCollector<>(true,this::findField))
                .stream()
                .collect(Collectors.toList());

        fieldsUsed.addAll(fields);

        RelNode input=filter.getInput();
        RelNode result=trimChild(input,fieldsUsed);
        if(Objects.equals(input,result)){
            return filter;
        }


        return filter;

    }

    public RelNode trimFields(GraphLogicalSingleMatch singleMatch,Set<GraphVariable>fieldsUsed){
      List<RelNode> sentence=Collections.singletonList(singleMatch.getSentence());
      return  singleMatch;
    }

    public RelNode trimFields(GraphLogicalMultiMatch multiMatch,Set<GraphVariable> fieldUsed){
        List<RelNode> setences=multiMatch.getSentences();
        List<RelNode> result=Collections.emptyList();
        for(RelNode node:setences){
            trimChild(node,fieldUsed);
        }
        // TODO(huaiyu)
        return multiMatch;
    }

    protected RelNode trimChild(RelNode rel, Set<GraphVariable> fieldsUsed) {
        return dispatchTrimFields(rel,fieldsUsed);

    }
   final public GraphVariable findField(RexGraphVariable var){
        return new GraphVariable(var.getAliasId(),var.getProperty());
    }



    protected final RelNode dispatchTrimFields(
            RelNode rel,
            Set<GraphVariable> fieldsUsed
    ){
        return graphTrimFieldsDispatcher.invoke(rel,fieldsUsed);
    }

}