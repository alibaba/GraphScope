package com.alibaba.graphscope.common.ir.rel.metadata.glogue.pattern;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
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
    // the key is a list of vertex type ids, notice that it is the type of a
    // PatternVertex.
    // the value is a list of group ids
    private Map<List<Integer>, List<Integer>> mapTypeToGroup;

    final static Comparator<List<Integer>> vertexTypeListComparator = (o1, o2) -> {
        if (o1.size() != o2.size()) {
            return o1.size() - o2.size();
        } else {
            o1.sort(Integer::compareTo);
            o2.sort(Integer::compareTo);
            for (int i = 0; i < o1.size(); i++) {
                if (!o1.get(i).equals(o2.get(i))) {
                    return o1.get(i) - o2.get(i);
                }
            }
            return 0;
        }
    };

    final static Comparator<Set<Integer>> vertexTypeSetComparator = (o1, o2) -> {
        if (o1.size() != o2.size()) {
            return o1.size() - o2.size();
        } else {
            TreeSet<Integer> o1TreeSet = new TreeSet<Integer>(o1);
            TreeSet<Integer> o2TreeSet = new TreeSet<Integer>(o2);
            Iterator<Integer> o1Iterator = o1TreeSet.iterator();
            Iterator<Integer> o2Iterator = o2TreeSet.iterator();
            while (o1Iterator.hasNext()) {
                Integer o1Next = o1Iterator.next();
                Integer o2Next = o2Iterator.next();
                if (!o1Next.equals(o2Next)) {
                    return o1Next - o2Next;
                }
            }
            return 0;
        }
    };

    public PatternOrderCanonicalLabelingImpl(Graph<PatternVertex, PatternEdge> patternGraph) {
        // different vertex types are initialized with different colors;
        // if with multiple vertex types, initialize the colors in order of vertex types
        Map<PatternVertex, Integer> initialColors = new HashMap<>();
        Map<List<Integer>, Integer> initialMapTypeToColor = new HashMap<>();
        Integer colorId = 0;

        // first, sort the initial types, and assign the color id in order to each
        // type(s).
        Set<List<Integer>> verticesTypeIds = new TreeSet<>(vertexTypeListComparator);
        for (PatternVertex patternVertex : patternGraph.vertexSet()) {
            List<Integer> vertexTypeIds = patternVertex.getVertexTypeIds();
            if (!verticesTypeIds.contains(vertexTypeIds)) {
                verticesTypeIds.add(vertexTypeIds);
            }
        }
        for (List<Integer> vertexTypeIds : verticesTypeIds) {
            vertexTypeIds.sort(Comparator.naturalOrder());
            initialMapTypeToColor.put(vertexTypeIds, colorId);
            colorId++;
        }

        // then, assign the color id to each vertex
        for (PatternVertex patternVertex : patternGraph.vertexSet()) {
            List<Integer> vertexTypeIds = patternVertex.getVertexTypeIds();
            vertexTypeIds.sort(Comparator.naturalOrder());
            initialColors.put(patternVertex, initialMapTypeToColor.get(vertexTypeIds));
        }
        ColoringImpl<PatternVertex> initialColoringImpl = new ColoringImpl<PatternVertex>(initialColors, colorId);
        ColorRefinementAlgorithm<PatternVertex, PatternEdge> colorRefinementAlgorithm = new ColorRefinementAlgorithm<PatternVertex, PatternEdge>(
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
        mapTypeToGroup = new TreeMap<>(vertexTypeListComparator);
        Integer groupId = 0;
        for (Set<PatternVertex> coloredPatternVertices : color.getColorClasses()) {
            List<Integer> vertexTypeIds = coloredPatternVertices.iterator().next().getVertexTypeIds();
            vertexTypeIds.sort(Comparator.naturalOrder());
            if (mapTypeToGroup.containsKey(vertexTypeIds)) {
                mapTypeToGroup.get(vertexTypeIds).add(groupId);
            } else {
                mapTypeToGroup.put(vertexTypeIds,
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
                // first compare if types are the same
                List<List<Integer>> thisTypeList = new ArrayList<>(this.mapTypeToGroup.keySet());
                List<List<Integer>> otherTypeList = new ArrayList<>(other.mapTypeToGroup.keySet());
                thisTypeList.sort(vertexTypeListComparator);
                otherTypeList.sort(vertexTypeListComparator);
                if (!thisTypeList.equals(otherTypeList)) {
                    return false;
                }
                // then compare the number of groups for each type
                for (List<Integer> vertexTypeIds : this.mapTypeToGroup.keySet()) {
                    if (this.mapTypeToGroup.get(vertexTypeIds).size() != other.mapTypeToGroup.get(vertexTypeIds)
                            .size()) {
                        System.out.println(
                                "In color comparing, numbers of groups for type " + vertexTypeIds + " not equal: "
                                        + this.mapTypeToGroup.get(vertexTypeIds).size() + " vs "
                                        + other.mapTypeToGroup.get(vertexTypeIds).size());
                        return false;
                    } else {
                        List<Integer> colors1 = this.mapTypeToGroup.get(vertexTypeIds);
                        colors1.sort(Comparator.naturalOrder());
                        List<Integer> colors2 = other.mapTypeToGroup.get(vertexTypeIds);
                        colors2.sort(Comparator.naturalOrder());
                        if (!colors1.equals(colors2)) {
                            System.out.println("In groups comparing, groups for type " + vertexTypeIds + " not equal: "
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
