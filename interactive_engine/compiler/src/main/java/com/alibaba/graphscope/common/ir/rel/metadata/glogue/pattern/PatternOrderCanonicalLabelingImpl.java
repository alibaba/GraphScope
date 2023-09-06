package com.alibaba.graphscope.common.ir.rel.metadata.glogue.pattern;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.jgrapht.Graph;
import org.jgrapht.alg.color.ColorRefinementAlgorithm;
import org.jgrapht.alg.interfaces.VertexColoringAlgorithm.Coloring;
import org.jgrapht.alg.interfaces.VertexColoringAlgorithm.ColoringImpl;

public class PatternOrderCanonicalLabelingImpl extends PatternOrder {

    // the initial mapping of vertices <-> colors, i.e., vertices <-> groups.
    // vertices belongs to the same color means they are structurally equivalent in
    // the patternGraph
    private Coloring<PatternVertex> initColoring;

    // the refined mapping of vertices <-> colors that each vertex is assigned a
    // unique color
    private Coloring<PatternVertex> uniqueColoring;

    // the mapping of vertex types <-> groups, used for pattern matching
    // filtering
    private Map<Integer, List<Integer>> mapTypeToGroup;

    public PatternOrderCanonicalLabelingImpl(Graph<PatternVertex, PatternEdge> patternGraph) {
        // each vertex type initialized with a unique color;
        // if with multiple vertex types, initialize the colors in order of vertex type
        // ids
        Map<PatternVertex, Integer> initialColors = new HashMap<>();
        Map<Integer, Integer> initialMapTypeToColor = new HashMap<>();
        Integer colorId = 0;

        Set<Integer> vertexTypeIds = new TreeSet<>();
        for (PatternVertex vertex : patternGraph.vertexSet()) {
            if (!vertexTypeIds.contains(vertex.getVertexTypeId())) {
                vertexTypeIds.add(vertex.getVertexTypeId());
            }
        }
        for (Integer type : vertexTypeIds) {
            initialMapTypeToColor.put(type, colorId);
            colorId++;
        }

        for (PatternVertex vertex : patternGraph.vertexSet()) {
            Integer vertexTypeId = vertex.getVertexTypeId();
            initialColors.put(vertex, initialMapTypeToColor.get(vertexTypeId));
        }
        ColoringImpl initialColoringImpl = new ColoringImpl<PatternVertex>(initialColors, colorId);
        ColorRefinementAlgorithm<PatternVertex, PatternEdge> colorRefinementAlgorithm = new ColorRefinementAlgorithm(
                patternGraph,
                initialColoringImpl);
        Coloring<PatternVertex> initColoring = colorRefinementAlgorithm.getColoring();
        setTypeGroupMapping(initColoring);
        this.initColoring = initColoring;

        boolean isUniqueColor = checkUniqueColor(initColoring);
        Coloring<PatternVertex> newColor;
        if (!isUniqueColor) {
            newColor = colorRefinementAlgorithm.getColoring();
        } else {
            newColor = initColoring;
        }
        while (!isUniqueColor) {
            newColor = recolor(patternGraph, newColor);
            isUniqueColor = checkUniqueColor(newColor);
        }
        this.uniqueColoring = newColor;
    }

    // check whether the color is uniquely determined.
    private boolean checkUniqueColor(Coloring<PatternVertex> patternColoring) {
        for (Set<PatternVertex> coloredVertices : patternColoring.getColorClasses()) {
            if (coloredVertices.size() > 1) {
                return false;
            }
        }
        return true;
    }

    // recolor if the current color is not unique
    private Coloring<PatternVertex> recolor(Graph<PatternVertex, PatternEdge> patternGraph,
            Coloring<PatternVertex> patternColoring) {
        Map<PatternVertex, Integer> initialColors = patternColoring.getColors();
        int maxColorId = patternColoring.getColorClasses().size();
        for (Set<PatternVertex> coloredVertices : patternColoring.getColorClasses()) {
            if (coloredVertices.size() > 1) {
                // give the structurally-equalled vertex a new color
                initialColors.put(coloredVertices.iterator().next(), maxColorId++);
                break;
            }
        }
        ColoringImpl initialColoringImpl = new ColoringImpl(initialColors, maxColorId);
        ColorRefinementAlgorithm colorRefinementAlgorithm = new ColorRefinementAlgorithm(patternGraph,
                initialColoringImpl);
        Coloring<PatternVertex> newColor = colorRefinementAlgorithm.getColoring();
        return newColor;
    }

    private void setTypeGroupMapping(Coloring<PatternVertex> color) {
        mapTypeToGroup = new TreeMap<>();
        Integer groupId = 0;
        for (Set<PatternVertex> coloredPatternVertices : color.getColorClasses()) {
            Integer vertexTypeId = coloredPatternVertices.iterator().next().getVertexTypeId();
            if (mapTypeToGroup.containsKey(vertexTypeId)) {
                mapTypeToGroup.get(vertexTypeId).add(groupId);
            } else {
                mapTypeToGroup.put(vertexTypeId,
                        new ArrayList<Integer>(Arrays.asList(groupId)));
            }
            groupId++;
        }
    }

    @Override
    public Integer getVertexOrder(PatternVertex vertex) {
        return uniqueColoring.getColors().get(vertex);
    }

    @Override
    public Integer getVertexGroup(PatternVertex vertex) {
        return initColoring.getColors().get(vertex);
    }

    @Override
    public PatternVertex getVertexByOrder(Integer id) {
        return uniqueColoring.getColorClasses().get(id).iterator().next();
    }

    @Override
    public String toString() {
        return "uniqueColoring :" + this.uniqueColoring.toString() + ", groupColoring: "
                + this.initColoring.toString();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof PatternOrderCanonicalLabelingImpl) {
            PatternOrderCanonicalLabelingImpl other = (PatternOrderCanonicalLabelingImpl) obj;
            // should have the same number of types and groups
            if (this.mapTypeToGroup.size() != other.mapTypeToGroup.size()
                    || this.initColoring.getColorClasses().size() != other.initColoring.getColorClasses().size()) {
                System.out.println("In color comparing, numbers of types / colors not equal: "
                        + this.mapTypeToGroup.size() + " vs " + other.mapTypeToGroup.size() + " / "
                        + this.initColoring.getColorClasses().size() + " vs "
                        + other.initColoring.getColorClasses().size());
                return false;
            }
            // each type should have the same number of groups
            else {
                for (Integer type : this.mapTypeToGroup.keySet()) {
                    if (this.mapTypeToGroup.get(type).size() != other.mapTypeToGroup.get(type).size()) {
                        System.out.println("In color comparing, numbers of groups for type " + type + " not equal: "
                                + this.mapTypeToGroup.get(type).size() + " vs "
                                + other.mapTypeToGroup.get(type).size());
                        return false;
                    } else {
                        List<Integer> colors1 = this.mapTypeToGroup.get(type);
                        colors1.sort(Comparator.naturalOrder());
                        List<Integer> colors2 = other.mapTypeToGroup.get(type);
                        colors2.sort(Comparator.naturalOrder());
                        if (!colors1.equals(colors2)) {
                            System.out.println("In groups comparing, groups for type " + type + " not equal: "
                                    + colors1.toString() + " vs " + colors2.toString());
                            return false;
                        }
                    }
                }
            }
        }
        // TODO: more filtering conditions?
        return true;
    }

}
