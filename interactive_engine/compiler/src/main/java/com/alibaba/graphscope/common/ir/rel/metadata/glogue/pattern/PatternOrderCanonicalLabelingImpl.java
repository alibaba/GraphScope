/*
 * Copyright 2024 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.graphscope.common.ir.rel.metadata.glogue.pattern;

import org.jgrapht.Graph;
import org.jgrapht.alg.color.ColorRefinementAlgorithm;
import org.jgrapht.alg.interfaces.VertexColoringAlgorithm.Coloring;
import org.jgrapht.alg.interfaces.VertexColoringAlgorithm.ColoringImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

public class PatternOrderCanonicalLabelingImpl extends PatternOrder {

    // the initial mapping of vertices <-> colors, i.e., vertices <-> groups.
    // vertices belongs to the same color means they are structurally equivalent in
    // the patternGraph
    private Coloring<PatternVertex> initColoring;

    // the refined mapping of vertices <-> colors that each vertex is assigned a
    // unique color
    private Coloring<PatternVertex> uniqueColoring;

    // the color class of the uniqueColoring
    private List<PatternVertex> uniqueColorClass;

    // the mapping of vertex types <-> groups, used for pattern matching
    // filtering
    // the key is a IsomorphismChecker (comparator) for vertices, and
    // the value is a list of group ids
    private Map<IsomorphismChecker, List<Integer>> mapCheckerToGroup;

    private static Logger logger = LoggerFactory.getLogger(PatternOrderCanonicalLabelingImpl.class);

    public PatternOrderCanonicalLabelingImpl(Graph<PatternVertex, PatternEdge> patternGraph) {
        // different vertex types are initialized with different colors;
        // if with multiple vertex types, initialize the colors in order of vertex types
        Map<PatternVertex, Integer> initialColors = new HashMap<>();
        Map<IsomorphismChecker, Integer> initialMapCheckerToColor = new HashMap<>();
        Integer colorId = 0;

        // first, sort the initial types, and assign the color id in order to each
        // type(s).
        Set<IsomorphismChecker> checkerSet = new TreeSet();
        for (PatternVertex patternVertex : patternGraph.vertexSet()) {
            checkerSet.add(patternVertex.getIsomorphismChecker());
        }
        for (IsomorphismChecker checker : checkerSet) {
            initialMapCheckerToColor.put(checker, colorId);
            colorId++;
        }

        // then, assign the color id to each vertex
        for (PatternVertex patternVertex : patternGraph.vertexSet()) {
            initialColors.put(
                    patternVertex,
                    initialMapCheckerToColor.get(patternVertex.getIsomorphismChecker()));
        }

        ColoringImpl<PatternVertex> initialColoringImpl =
                new ColoringImpl<PatternVertex>(initialColors, colorId);
        ColorRefinementAlgorithm<PatternVertex, PatternEdge> colorRefinementAlgorithm =
                new ColorRefinementAlgorithm<PatternVertex, PatternEdge>(
                        patternGraph, initialColoringImpl);
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
        this.uniqueColorClass =
                newColor.getColorClasses().stream()
                        .map(set -> set.iterator().next())
                        .collect(Collectors.toList());
    }

    // check whether the color is uniquely determined.
    private boolean checkUniqueColor(Coloring<PatternVertex> patternColoring) {
        List<Set<PatternVertex>> colorClasses = patternColoring.getColorClasses();
        for (Set<PatternVertex> coloredVertices : colorClasses) {
            if (coloredVertices.size() > 1) {
                return false;
            }
        }
        return true;
    }

    // recolor if the current color is not unique
    private Coloring<PatternVertex> recolor(
            Graph<PatternVertex, PatternEdge> patternGraph,
            Coloring<PatternVertex> patternColoring) {
        Map<PatternVertex, Integer> initialColors = patternColoring.getColors();
        List<Set<PatternVertex>> colorClasses = patternColoring.getColorClasses();
        int maxColorId = colorClasses.size();
        for (Set<PatternVertex> coloredVertices : colorClasses) {
            if (coloredVertices.size() > 1) {
                // give the structurally-equalled vertex a new color
                initialColors.put(coloredVertices.iterator().next(), maxColorId++);
                break;
            }
        }
        ColoringImpl initialColoringImpl = new ColoringImpl(initialColors, maxColorId);
        ColorRefinementAlgorithm colorRefinementAlgorithm =
                new ColorRefinementAlgorithm(patternGraph, initialColoringImpl);
        Coloring<PatternVertex> newColor = colorRefinementAlgorithm.getColoring();
        return newColor;
    }

    private void setTypeGroupMapping(Coloring<PatternVertex> color) {
        mapCheckerToGroup = new TreeMap();
        Integer groupId = 0;
        List<Set<PatternVertex>> colorClasses = color.getColorClasses();
        for (Set<PatternVertex> coloredPatternVertices : colorClasses) {
            IsomorphismChecker checker =
                    coloredPatternVertices.iterator().next().getIsomorphismChecker();
            if (mapCheckerToGroup.containsKey(checker)) {
                mapCheckerToGroup.get(checker).add(groupId);
            } else {
                mapCheckerToGroup.put(checker, new ArrayList<Integer>(Arrays.asList(groupId)));
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
        return uniqueColorClass.get(id);
    }

    @Override
    public String toString() {
        return "uniqueColoring :"
                + this.uniqueColoring.toString()
                + ", groupColoring: "
                + this.initColoring.toString();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof PatternOrderCanonicalLabelingImpl) {
            PatternOrderCanonicalLabelingImpl other = (PatternOrderCanonicalLabelingImpl) obj;
            // should have the same number of types and groups
            if (this.mapCheckerToGroup.size() != other.mapCheckerToGroup.size()
                    || this.initColoring.getNumberColors()
                            != other.initColoring.getNumberColors()) {
                return false;
            }
            // each type should have the same number of groups
            else {
                // first compare if types are the same
                List<IsomorphismChecker> thisTypeList =
                        new ArrayList<>(this.mapCheckerToGroup.keySet());
                List<IsomorphismChecker> otherTypeList =
                        new ArrayList<>(other.mapCheckerToGroup.keySet());
                if (!thisTypeList.equals(otherTypeList)) {
                    return false;
                }
                // then compare the number of groups for each type
                for (IsomorphismChecker checker : this.mapCheckerToGroup.keySet()) {
                    if (this.mapCheckerToGroup.get(checker).size()
                            != other.mapCheckerToGroup.get(checker).size()) {
                        logger.debug(
                                "In color comparing, numbers of groups for type "
                                        + checker
                                        + " not equal: "
                                        + this.mapCheckerToGroup.get(checker).size()
                                        + " vs "
                                        + other.mapCheckerToGroup.get(checker).size());
                        return false;
                    } else {
                        List<Integer> colors1 = this.mapCheckerToGroup.get(checker);
                        colors1.sort(Comparator.naturalOrder());
                        List<Integer> colors2 = other.mapCheckerToGroup.get(checker);
                        colors2.sort(Comparator.naturalOrder());
                        if (!colors1.equals(colors2)) {
                            logger.debug(
                                    "In groups comparing, groups for type "
                                            + checker
                                            + " not equal: "
                                            + colors1.toString()
                                            + " vs "
                                            + colors2.toString());
                            return false;
                        }
                    }
                }
            }
        }
        return true;
    }

    @Override
    public int hashCode() {
        return Objects.hash(mapCheckerToGroup);
    }
}
