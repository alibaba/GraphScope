package s2scompiler;

import s2scompiler.steps.*;
import org.apache.tinkperpop.gremlin.groovy.custom.ExprStep;
import org.apache.tinkperpop.gremlin.groovy.custom.ScatterStep;
import org.apache.tinkperpop.gremlin.groovy.custom.GatherStep;
import org.apache.tinkperpop.gremlin.groovy.custom.TraversalProcessStep;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Compare;
import org.apache.tinkerpop.gremlin.process.traversal.Operator;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.CountGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.IsStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.TraversalFilterStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.RepeatStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.AddPropertyStep;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.LoopTraversal;

import java.util.Collections;
import java.util.List; 
import java.util.ArrayList; 
import java.util.HashMap;
import java.util.function.BiPredicate;
import java.util.function.Predicate;

public class S2SCompiler{
    private static String VPTYPE = "double";
    private HashMap<String, String> properties;
    
    public S2SCompiler() {
        properties = new HashMap<String, String> ();
    }

    public List<TransStep> translateGraphStep(final GraphStep s) { 
        if (s.returnsVertex())
            if (s.getIds() == null || s.getIds().length == 0) 
                return Collections.singletonList(new V());
        return Collections.singletonList(new TransStep(s.toString()));
    }

    public List<TransStep> translateExprStep(final ExprStep s) {
        return Collections.singletonList(new Expr(s.getExpressionString()));
    }

    public List<TransStep> translateVertexStep(final VertexStep s) {
        if (s.returnsVertex())
            if (s.getEdgeLabels() == null || s.getEdgeLabels().length == 0)
                return Collections.singletonList(new TransStep(s.getDirection().toString()));
        return Collections.singletonList(new TransStep(s.toString()));
    }

    public List<TransStep> translateAddPropertyStep(final AddPropertyStep s) {
        List<Object> list1 = s.getParameters().get(T.key, null);
        List<Object> list2 = s.getParameters().get(T.value, null);
        if (!list1.isEmpty() && !list2.isEmpty()) {
            String k = (String) list1.get(0);
            Object v = list2.get(0);
            if (v instanceof Traversal.Admin && ((Traversal.Admin)v).getSteps().size() == 1) {
                TransStep res1 = translateSteps(((Traversal.Admin)v).getStartStep()).get(0);
                properties.put(k, VPTYPE);
                return Collections.singletonList(new Property(k, res1));
            } 
        }
        return Collections.singletonList(new TransStep(s.toString()));
    }

    public List<TransStep> translateTraversalFilterStep(final TraversalFilterStep s) {
        if (!s.getLocalChildren().isEmpty()) {
            Traversal.Admin traversal = (Traversal.Admin)s.getLocalChildren().get(0);
            if (traversal.getSteps().size() == 1) {
                Step s1 = traversal.getStartStep();
                TransStep res1 = translateSteps(s1).get(0);
                return Collections.singletonList(new Where(res1));
            } 
        }
        return Collections.singletonList(new TransStep(s.toString()));
    }

    public List<TransStep> translateGatherStep(final GatherStep s) {
        properties.put(s.getGatherName(), VPTYPE);
        return Collections.singletonList(new Gather(s.getGatherName(), s.getOp().toString()));
    }

    public List<TransStep> translateScatterStep(final ScatterStep s) {
        List<TransStep> res = new ArrayList<TransStep>();
        res.add(new Scatter(s.getScatterName()));
        if (!s.getLocalChildren().isEmpty()) {
            Traversal.Admin traversal = (Traversal.Admin)s.getLocalChildren().get(0);
            if (traversal.getSteps().size() == 1) { //by(...)
                Step s1 = traversal.getStartStep();
                TransStep res1 = translateSteps(s1).get(0);
                res.add(new By(res1));
            } else {
               return Collections.singletonList(new TransStep(s.toString()));
            }
        }
        return res;    
    }

    public List<TransStep> translateRepeatStep(final RepeatStep s) {
        if (s.getRepeatTraversal() == null || s.getRepeatTraversal().getSteps().isEmpty()) 
            return Collections.singletonList(new TransStep(s.toString()));

        List<TransStep> res = new ArrayList<TransStep>();
        List<Step> list_r = s.getRepeatTraversal().getSteps();
        ArrayList<TransStep> a = new ArrayList<TransStep>();
        for (int i = 0; i < list_r.size(); i++) {
            if (list_r.get(i) instanceof RepeatStep.RepeatEndStep) break;
            List<TransStep> res1 = translateSteps(list_r.get(i));
            a.addAll(res1);
        }
        res.add(new Repeat(a));

        Traversal.Admin traversal_u = s.getUntilTraversal(); //until
        if (traversal_u instanceof LoopTraversal) { //times
            long x = ((LoopTraversal)traversal_u).getMaxLoops();
            res.add(new Times(x));
        } else {
            List<Step> list_u = traversal_u.getSteps(); 
            ArrayList<TransStep> b = new ArrayList<TransStep>();
            for (int i = 0; i < list_u.size(); i++) {
                List<TransStep> res1 = translateSteps(list_u.get(i));
                b.addAll(res1);
            }
            res.add(new Until(b));
        }
        return res;
    }

    public List<TransStep> translateTraversalProcessStep(final TraversalProcessStep s) {
        List<Step> list = s.getMapTraversal().getSteps();
        ArrayList<TransStep> a = new ArrayList<TransStep>();
        for (int i = 0; i < list.size(); i++) {
            List<TransStep> res1 = translateSteps(list.get(i));
            a.addAll(res1);
        }
        return Collections.singletonList(new TransProcess(a, properties));
    }

    public List<TransStep> translateSteps(final Step t) {
        if (t instanceof GraphStep)  //V
            return translateGraphStep((GraphStep) t);
        if (t instanceof ExprStep)  //expr 
            return translateExprStep((ExprStep) t);
        if (t instanceof VertexStep)  //in, out, both
            return translateVertexStep((VertexStep) t);
        if (t instanceof AddPropertyStep) //property
            return translateAddPropertyStep((AddPropertyStep) t);
        if (t instanceof TraversalFilterStep) //where
            return translateTraversalFilterStep((TraversalFilterStep) t);
        if (t instanceof GatherStep) //gather
            return translateGatherStep((GatherStep) t);

        if (t instanceof ScatterStep) //scatter
            return translateScatterStep((ScatterStep) t);
        if (t instanceof RepeatStep) //repeat
            return translateRepeatStep((RepeatStep) t);
        if (t instanceof TraversalProcessStep) //process
            return translateTraversalProcessStep((TraversalProcessStep) t);

        if (t instanceof CountGlobalStep) //count
            return Collections.singletonList(new TransStep("count"));
        if (t instanceof IsStep) { //is
            IsStep s = (IsStep)t;
            if (s.getPredicate().getBiPredicate() == Compare.eq && s.getPredicate().getOriginalValue().equals(0))
                return Collections.singletonList(new TransStep("is"));
            else
                return Collections.singletonList(new TransStep(s.toString()));
        }
        
        return Collections.singletonList(new TransStep(t.toString())); //others
    }

    public String compile(final TraversalProcessStep s) {
        List<TransStep> trans_steps = translateSteps(s);
        Result res = trans_steps.get(0).translate();
        if (!res.t)
            throw new RuntimeException(res.s);
        return res.s;
    }
}