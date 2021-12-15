package com.alibaba.graphscope.common.intermediate.process;

import com.alibaba.graphscope.common.exception.InterOpIllegalArgException;
import com.alibaba.graphscope.common.exception.InterOpUnsupportedException;
import com.alibaba.graphscope.common.intermediate.InterOpCollection;
import com.alibaba.graphscope.common.intermediate.operator.*;
import com.alibaba.graphscope.common.jna.IrCoreLibrary;
import com.alibaba.graphscope.common.jna.type.FfiNameOrId;
import com.alibaba.graphscope.common.jna.type.FfiProperty;
import com.alibaba.graphscope.common.jna.type.FfiScanOpt;
import com.alibaba.graphscope.common.jna.type.FfiVariable;
import org.javatuples.Pair;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

//get properties required for each intermediate vertex or edge
public class PropertyDetailsProcessor implements InterOpProcessor {
    private static IrCoreLibrary irCoreLib = IrCoreLibrary.INSTANCE;

    public static PropertyDetailsProcessor INSTANCE = new PropertyDetailsProcessor();

    private PropertyDetailsProcessor() {
    }

    public enum PropertyDetailsFactory implements Function<InterOpBase, Object> {
        // the op will create new vertex or edge, such as scan or expand
        CREATE {
            @Override
            public GraphElement apply(InterOpBase interOpBase) {
                if (interOpBase instanceof ScanFusionOp) {
                    ScanFusionOp op = (ScanFusionOp) interOpBase;
                    if (!op.getScanOpt().isPresent()) {
                        throw new InterOpIllegalArgException(op.getClass(), "scanOpt", "not present");
                    }
                    FfiScanOpt opt = (FfiScanOpt) op.getScanOpt().get().getArg();
                    return new GraphElement(opt == FfiScanOpt.Edge);
                } else if (interOpBase instanceof ExpandOp) {
                    ExpandOp op = (ExpandOp) interOpBase;
                    if (!op.getIsEdge().isPresent()) {
                        throw new InterOpIllegalArgException(op.getClass(), "direction", "not present");
                    }
                    Boolean isEdge = (Boolean) op.getIsEdge().get().getArg();
                    return new GraphElement(isEdge);
                } else {
                    throw new InterOpUnsupportedException(interOpBase.getClass(), "unsupported or is not CREATE type");
                }
            }
        },
        // the op require some properties, such as order(..).by('name', order)
        REQUIRE {
            @Override
            public TagRequiredProperties apply(InterOpBase interOpBase) {
                TagRequiredProperties requiredProperties = new TagRequiredProperties();
                if (interOpBase instanceof ProjectOp) {
                    ProjectOp op = (ProjectOp) interOpBase;
                    if (!op.getProjectExprWithAlias().isPresent()) {
                        throw new InterOpIllegalArgException(interOpBase.getClass(), "exprWithAlias", "not present");
                    }
                    List<Pair> projectList = (List<Pair>) op.getProjectExprWithAlias().get().getArg();
                    projectList.forEach(k -> {
                        String expr = (String) k.getValue0();
                        List<Pair<FfiNameOrId.ByValue, FfiProperty.ByValue>> pairList = getTagPropertyPairList(expr);
                        pairList.forEach(p -> {
                            requiredProperties.addTagProperty(p.getValue0(), p.getValue1());
                        });
                    });
                } else if (interOpBase instanceof SelectOp) {
                    SelectOp op = (SelectOp) interOpBase;
                    if (!op.getPredicate().isPresent()) {
                        throw new InterOpIllegalArgException(interOpBase.getClass(), "predicate", "not present");
                    }
                    String expr = (String) op.getPredicate().get().getArg();
                    List<Pair<FfiNameOrId.ByValue, FfiProperty.ByValue>> pairList = getTagPropertyPairList(expr);
                    pairList.forEach(p -> {
                        requiredProperties.addTagProperty(p.getValue0(), p.getValue1());
                    });
                } else if (interOpBase instanceof OrderOp) {
                    OrderOp op = (OrderOp) interOpBase;
                    if (!op.getOrderVarWithOrder().isPresent()) {
                        throw new InterOpIllegalArgException(interOpBase.getClass(), "varWithOrder", "not present");
                    }
                    List<Pair> orderList = (List<Pair>) op.getOrderVarWithOrder().get().getArg();
                    orderList.forEach(k -> {
                        FfiVariable.ByValue var = (FfiVariable.ByValue) k.getValue0();
                        if (!var.property.isNone()) {
                            requiredProperties.addTagProperty(var.tag, var.property);
                        }
                    });
                }
                return requiredProperties;
            }
        },
    }

    @Override
    public void process(InterOpCollection opCollection) {
        // tag -> element or head -> element
        Map<FfiNameOrId.ByValue, GraphElement> elementRecord = new HashMap<>();
        // position of InterOp creating the GraphElement first
        Map<GraphElement, Integer> elementStartPos = new HashMap<>();
        // position of InterOp which has created a new GraphElement -> properties required
        Map<Integer, Set<FfiProperty.ByValue>> opPropertyDetails = new HashMap<>();

        List<InterOpBase> opList = opCollection.unmodifiableCollection();
        Optional<GraphElement> output = Optional.empty();

        for (int i = 0; i < opList.size(); ++i) {
            InterOpBase op = opList.get(i);
            output = outputGraphElementOpt(output, op, elementRecord);
            // set creating point of element
            if (op instanceof ScanFusionOp || op instanceof ExpandOp) {
                elementStartPos.put(output.get(), i);
            }
            // set required properties
            if (op instanceof OrderOp || op instanceof SelectOp || op instanceof ProjectOp) {
                TagRequiredProperties properties = (TagRequiredProperties) PropertyDetailsFactory.REQUIRE.apply(op);
                properties.getTags().forEach(tag -> {
                    Set<FfiProperty.ByValue> required = properties.getTagProperties(tag, true);
                    if (!required.isEmpty()) {
                        GraphElement element = elementRecord.get(tag);
                        if (element != null && elementStartPos.get(element) != null) {
                            int oprIdx = elementStartPos.get(element);
                            opPropertyDetails.computeIfAbsent(oprIdx, k -> new HashSet<>()).addAll(required);
                        }
                    }
                });
            }
            // output of the current op is GraphElement
            if (output.isPresent()) {
                // set head
                elementRecord.put(FfiNameOrId.ByValue.getHead(), output.get());
                // set alias
                if (op.getAlias().isPresent()) {
                    elementRecord.put((FfiNameOrId.ByValue) op.getAlias().get().getArg(), output.get());
                }
            }
        }

        if (!opPropertyDetails.isEmpty()) {
            List<Integer> oprIdxList = new ArrayList<>(opPropertyDetails.keySet());
            Collections.sort(oprIdxList, Collections.reverseOrder());
            oprIdxList.forEach(k -> {
                Set<FfiProperty.ByValue> propertyDetails = opPropertyDetails.get(k);
                if (propertyDetails != null && !propertyDetails.isEmpty()) {
                    // insert get detail after pos k
                    AuxiliaOp op = new AuxiliaOp();
                    Set<FfiNameOrId.ByValue> keys = propertyDetails.stream().map(p -> p.key).collect(Collectors.toSet());
                    op.setPropertyDetails(new OpArg(keys, Function.identity()));
                    opCollection.insertInterOp(k + 1, op);
                }
            });
        }
    }

    private Optional<GraphElement> outputGraphElementOpt(Optional<GraphElement> inputOpt, InterOpBase op,
                                                         Map<FfiNameOrId.ByValue, GraphElement> elementRecord) {
        if (op instanceof ScanFusionOp || op instanceof ExpandOp) {
            GraphElement element = (GraphElement) PropertyDetailsFactory.CREATE.apply(op);
            return Optional.of(element);
        } else if (op instanceof OrderOp || op instanceof SelectOp || op instanceof LimitOp) {
            return inputOpt;
        } else if (op instanceof ProjectOp) {
            return projectOneGraphElementOpt((ProjectOp) op, elementRecord);
        } else {
            throw new InterOpUnsupportedException(op.getClass(), "unsupported yet");
        }
    }

    private Optional<GraphElement> projectOneGraphElementOpt(ProjectOp op, Map<FfiNameOrId.ByValue, GraphElement> elementRecord) {
        List<Pair> projectList = (List<Pair>) op.getProjectExprWithAlias().get().getArg();
        if (projectList.size() != 1) return Optional.empty();
        String expr = (String) projectList.get(0).getValue0();
        int startPos = expr.indexOf('@');
        int endPos = expr.indexOf('.');
        // project self which is a GraphElement is ignored
        if (startPos == -1 || endPos != -1 || startPos + 1 >= expr.length()) {
            return Optional.empty();
        }
        String tag = expr.substring(startPos + 1);
        if (tag.isEmpty()) return Optional.empty();
        return Optional.ofNullable(elementRecord.get(tag));
    }

    public static List<Pair<FfiNameOrId.ByValue, FfiProperty.ByValue>> getTagPropertyPairList(String expr) {
        List<Pair<FfiNameOrId.ByValue, FfiProperty.ByValue>> pairList = new ArrayList<>();
        // parse expr to get List<tag, property>
        for (int i = 0; i < expr.length(); ) {
            int startPos = expr.indexOf('@', i);
            if (startPos == -1) {
                break;
            }
            if (startPos + 1 == expr.length() || expr.charAt(startPos + 1) == ' ') {
                i = startPos + 2;
                continue;
            }
            int propertyPos = expr.indexOf('.', startPos);
            if (propertyPos == -1) {
                break;
            }
            if ((propertyPos + 1) == expr.length() || expr.charAt(propertyPos + 1) == ' ') {
                i = propertyPos + 2;
                continue;
            }
            String tag = expr.substring(startPos + 1, propertyPos);
            int emptyEnd = expr.indexOf(' ', propertyPos);
            int propertyEnd = (emptyEnd == -1) ? expr.length() : emptyEnd;
            i = propertyEnd + 1;
            String property = expr.substring(propertyPos + 1, propertyEnd);
            if (!property.isEmpty()) {
                FfiNameOrId.ByValue ffiNameId = tag.isEmpty() ? FfiNameOrId.ByValue.getHead() : irCoreLib.cstrAsNameOrId(tag);
                pairList.add(Pair.with(ffiNameId, getFfiProperty(property)));
            }
        }
        return pairList;
    }
}

