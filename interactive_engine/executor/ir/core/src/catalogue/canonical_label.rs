//
//! Copyright 2022 Alibaba Group Holding Limited.
//!
//! Licensed under the Apache License, Version 2.0 (the "License");
//! you may not use this file except in compliance with the License.
//! You may obtain a copy of the License at
//!
//! http://www.apache.org/licenses/LICENSE-2.0
//!
//! Unless required by applicable law or agreed to in writing, software
//! distributed under the License is distributed on an "AS IS" BASIS,
//! WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//! See the License for the specific language governing permissions and
//! limitations under the License.

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet, VecDeque};

use crate::catalogue::error::{IrPatternError, IrPatternResult};
use crate::catalogue::pattern::{Adjacency, Pattern, PatternEdge, PatternVertex};
use crate::catalogue::{DynIter, PatternId, PatternLabelId};
use std::convert::TryFrom;

#[derive(Debug, Clone)]
pub(crate) struct CanonicalLabelManager {
    /// Map cloned from pattern
    /// - Key: Vertex ID
    /// - Value: Vector of adjacencies of the given vertex
    ///
    /// As the order of adjacencies only makes sense inside this canonical label manager
    /// We do not need to update the order of adjacencies back to the pattern.
    vertex_adjacencies_map: BTreeMap<PatternId, Vec<Adjacency>>,
    /// Map for Vertex Ranking
    /// - Key: Vertex ID
    /// - Value: Group ID of the given vertex
    vertex_group_map: BTreeMap<PatternId, PatternId>,
    /// Groups of vertices
    /// - Key: Vertex Label + Vertex Group ID
    /// - Value: Vector of vertices belonging to this group
    ///
    /// Since the groups between vertices with different label are independent, we need to use label + group ID as key
    vertex_groups: BTreeMap<(PatternLabelId, PatternId), Vec<PatternId>>,
    /// Indicates whether vertex grouping has been stable currently. If so, we say the vertices have been well grouped.
    has_converged: bool,
    /// Map for Vertex Ranking
    /// - Key: Vertex ID
    /// - Value: Rank of the given vertex
    edge_rank_map: BTreeMap<PatternId, Option<PatternId>>,
    /// Map for edge ranking
    /// - key: Edge ID
    /// - Value: Rank of the given edge
    /// Edge ranks are used for the order of pattern encoding.
    vertex_rank_map: BTreeMap<PatternId, Option<PatternId>>,
}

impl From<&Pattern> for CanonicalLabelManager {
    fn from(pattern: &Pattern) -> Self {
        // Initialize the map from vertex ID to its adjacencies list.
        // Filling the data into the map is delayed after the manager is initialized since the cmp_adjacencies method is needed.
        let mut vertex_adjacencies_map: BTreeMap<PatternId, Vec<Adjacency>> = BTreeMap::new();
        pattern
            .vertices_iter()
            .map(|vertex| vertex.get_id())
            .for_each(|v_id| {
                let vertex_adjacencies: Vec<Adjacency> = pattern
                    .adjacencies_iter(v_id)
                    .cloned()
                    .collect();
                vertex_adjacencies_map.insert(v_id, vertex_adjacencies);
            });

        // Initialize fields for vertex grouping
        let mut vertex_group_map: BTreeMap<PatternId, PatternId> = BTreeMap::new();
        let mut vertex_groups: BTreeMap<(PatternLabelId, PatternId), Vec<PatternId>> = BTreeMap::new();
        let has_converged: bool = false;
        pattern.vertices_iter().for_each(|vertex| {
            let (v_id, v_label) = (vertex.get_id(), vertex.get_label());
            vertex_group_map.insert(v_id, 0);
            vertex_groups
                .entry((v_label, 0))
                .and_modify(|vertices| vertices.push(v_id))
                .or_insert(vec![v_id]);
        });

        // Initialize fields for pattern ranking
        let mut edge_rank_map: BTreeMap<PatternId, Option<PatternId>> = BTreeMap::new();
        pattern.edges_iter().for_each(|edge| {
            edge_rank_map.insert(edge.get_id(), None);
        });
        let mut vertex_rank_map: BTreeMap<PatternId, Option<PatternId>> = BTreeMap::new();
        pattern.vertices_iter().for_each(|vertex| {
            vertex_rank_map.insert(vertex.get_id(), None);
        });

        // Initialize the manager with all the previous fields
        let mut manager = CanonicalLabelManager {
            vertex_adjacencies_map,
            vertex_group_map,
            vertex_groups,
            has_converged,
            edge_rank_map,
            vertex_rank_map,
        };
        // Sort the adjacencies for each vertex and fill the data into the vertex adjacency map
        manager.update_vertex_adjacencies_order();

        manager
    }
}

impl CanonicalLabelManager {
    /// Iterate the vertex-group map to update group information in pattern
    pub fn vertex_groups_iter(&self) -> DynIter<(PatternId, PatternId)> {
        Box::new(
            self.vertex_group_map
                .iter()
                .map(|(v_id, v_group)| (*v_id, *v_group)),
        )
    }

    /// Iterate the vertex-rank map to update vertex ranks in pattern
    pub fn vertex_ranks_iter(&self) -> DynIter<(PatternId, Option<PatternId>)> {
        Box::new(
            self.vertex_rank_map
                .iter()
                .map(|(v_id, v_rank)| (*v_id, *v_rank)),
        )
    }

    /// Iterate the edge-rank map to update edge ranks in pattern
    pub fn edge_ranks_iter(&self) -> DynIter<(PatternId, Option<PatternId>)> {
        Box::new(
            self.edge_rank_map
                .iter()
                .map(|(e_id, e_rank)| (*e_id, *e_rank)),
        )
    }

    /// Given vertex ID, return the group ID of the given vertex
    pub fn get_vertex_group(&self, vertex_id: PatternId) -> Option<PatternId> {
        self.vertex_group_map.get(&vertex_id).cloned()
    }

    /// Given vertex ID, return the rank of the given vertex
    pub fn get_vertex_rank(&self, vertex_id: PatternId) -> Option<PatternId> {
        *self
            .vertex_rank_map
            .get(&vertex_id)
            .unwrap_or(&None)
    }
}

/// Methods for Vertex Grouping
impl CanonicalLabelManager {
    /// Group vertices that are identical in graph structure together.
    ///
    /// The idea of vertex groups is very similar to the ordered partition in canonical labeling.
    ///
    /// Basic Idea: All vertices with the same label are initially in the same group, and iteratively refine the groups with updated grouping information until the grouping is stable.
    pub fn vertex_grouping(&mut self, pattern: &Pattern) -> IrPatternResult<()> {
        while !self.has_converged {
            self.refine_vertex_groups(pattern)?;
        }
        Ok(())
    }

    /// Refine all the vertex groups with the information about themselves as well as their adjacencies.
    fn refine_vertex_groups(&mut self, pattern: &Pattern) -> IrPatternResult<()> {
        // The updated version of vertex group map and vertex groups.
        // The updated data are temporarily stored here and finally moved to the VertexGroupManager.
        let mut updated_vertex_group_map: BTreeMap<PatternId, PatternId> = BTreeMap::new();
        let mut updated_vertex_groups: BTreeMap<(PatternLabelId, PatternId), Vec<PatternId>> =
            BTreeMap::new();
        let mut has_converged = true;
        for (&(v_label, initial_group), vertex_group) in self.vertex_groups.iter() {
            // Temporarily record the group for each vertex
            let mut vertex_group_tmp_vec: Vec<PatternId> = vec![initial_group; vertex_group.len()];
            // To find out the exact group of a vertex, compare it with all vertices with the same label
            for i in 0..vertex_group.len() {
                let current_v_id: PatternId = vertex_group[i];
                for j in (i + 1)..vertex_group.len() {
                    match self.cmp_vertices(pattern, current_v_id, vertex_group[j])? {
                        Ordering::Greater => vertex_group_tmp_vec[i] += 1,
                        Ordering::Less => vertex_group_tmp_vec[j] += 1,
                        Ordering::Equal => (),
                    }
                }

                let v_group: PatternId = vertex_group_tmp_vec[i];
                if v_group != initial_group {
                    has_converged = false;
                }

                updated_vertex_group_map.insert(current_v_id, v_group);
                updated_vertex_groups
                    .entry((v_label, v_group))
                    .and_modify(|vertex_group| vertex_group.push(current_v_id))
                    .or_insert(vec![current_v_id]);
            }
        }

        // Update vertex group manager
        self.vertex_group_map = updated_vertex_group_map;
        self.vertex_groups = updated_vertex_groups;
        self.has_converged = has_converged;

        // Update the order of vertex adjacencies
        self.update_vertex_adjacencies_order();
        Ok(())
    }
}

impl CanonicalLabelManager {
    /// Set unique ranks to each vertex and edge
    pub fn pattern_ranking(&mut self, pattern: &mut Pattern) -> IrPatternResult<()> {
        let start_v_id: PatternId = self.get_pattern_ranking_start_vertex(pattern)?;
        let mut next_free_vertex_rank: PatternId = 0;
        let mut next_free_edge_rank: PatternId = 0;
        self.vertex_rank_map
            .insert(start_v_id, Some(next_free_vertex_rank));
        next_free_vertex_rank += 1;
        let mut visited_edges: BTreeSet<PatternId> = BTreeSet::new();
        // Initialize Stack for adjacencies
        let mut adjacency_stack: VecDeque<Adjacency> =
            self.init_adjacencies_stack(start_v_id, &self.vertex_adjacencies_map)?;
        // Perform DFS on adjacencies
        while let Some(adjacency) = adjacency_stack.pop_back() {
            // Insert edge to dfs sequence if it has not been visited
            let adj_edge_id: PatternId = adjacency.get_edge_id();
            if visited_edges.contains(&adj_edge_id) {
                continue;
            }
            visited_edges.insert(adj_edge_id);
            self.edge_rank_map
                .insert(adj_edge_id, Some(next_free_edge_rank));
            next_free_edge_rank += 1;

            // Set dfs id to the vertex if it has not been set before
            let current_v_id: PatternId = adjacency.get_adj_vertex().get_id();
            let is_vertex_visited = self
                .vertex_rank_map
                .get(&current_v_id)
                .unwrap_or(&None)
                .is_some();
            if !is_vertex_visited {
                self.vertex_rank_map
                    .insert(current_v_id, Some(next_free_vertex_rank));
                next_free_vertex_rank += 1;
            }

            // Update the order of vertex adjacencies with the updated ranks
            self.update_vertex_adjacencies_order();
            // Push adjacencies of the current vertex into the stack for later DFS
            let adjacencies_to_extend = self
                .vertex_adjacencies_map
                .get(&current_v_id)
                .ok_or(IrPatternError::CanonicalLabelError(format!(
                    "vertex not exist in CanonicalLabelManager, the vertex is {:?}",
                    current_v_id
                )))?;
            adjacencies_to_extend
                .iter()
                .rev()
                .filter(|adj| !visited_edges.contains(&adj.get_edge_id()))
                .for_each(|adj| adjacency_stack.push_back(*adj));
        }
        Ok(())
    }

    /// Return the ID of the starting vertex of pattern ranking.
    ///
    /// In our case, it's the vertex with the smallest label and group ID
    fn get_pattern_ranking_start_vertex(&self, pattern: &Pattern) -> IrPatternResult<PatternId> {
        let min_v_label = pattern
            .get_min_vertex_label()
            .ok_or(IrPatternError::InvalidPattern("min_vertex_label not exist in pattern".to_string()))?;
        pattern
            .vertices_iter_by_label(min_v_label)
            .map(|vertex| vertex.get_id())
            .min_by(|&v1_id, &v2_id| {
                let v1_group = self.get_vertex_group(v1_id).unwrap();
                let v2_group = self.get_vertex_group(v2_id).unwrap();
                v1_group.cmp(&v2_group)
            })
            .ok_or(IrPatternError::InvalidPattern(format!(
                "vertices of min_vertex_label not exist in pattern, the label is {:?}",
                min_v_label
            )))
    }

    /// Pattern Ranking adopts DFS and the stack for DFS stores vertex adjacencies
    fn init_adjacencies_stack(
        &self, start_v_id: PatternId, vertex_adjacencies_map: &BTreeMap<PatternId, Vec<Adjacency>>,
    ) -> IrPatternResult<VecDeque<Adjacency>> {
        let mut adjacency_stack: VecDeque<Adjacency> = VecDeque::new();
        let adjacencies_of_start_vertex =
            vertex_adjacencies_map
                .get(&start_v_id)
                .ok_or(IrPatternError::InvalidPattern(format!(
                    "Get adjacency's of vertex failed, vertex is {:?}",
                    start_v_id
                )))?;
        // Use rev() so that we can pop out adjacencies in normal order
        adjacencies_of_start_vertex
            .iter()
            .rev()
            .for_each(|adjacency| adjacency_stack.push_back(*adjacency));

        Ok(adjacency_stack)
    }
}

impl CanonicalLabelManager {
    /// Compare two adjacencies in the pattern.
    /// The following data are taken into consideration:
    /// - Data of Adjacency Itself: (Edge Direction, End Vertex Label and Edge Label)
    /// - Group ID of end vertex
    /// - Rank of end vertex
    fn cmp_adjacencies(&self, adj1: &Adjacency, adj2: &Adjacency) -> Ordering {
        // Compare the information stored inside adjacencies: label and edge direction
        let adj1_info_tuple =
            (adj1.get_direction(), adj1.get_adj_vertex().get_label(), adj1.get_edge_label());
        let adj2_info_tuple =
            (adj2.get_direction(), adj2.get_adj_vertex().get_label(), adj2.get_edge_label());
        match adj1_info_tuple.cmp(&adj2_info_tuple) {
            Ordering::Less => return Ordering::Less,
            Ordering::Greater => return Ordering::Greater,
            Ordering::Equal => (),
        }

        let adj1_v_id: PatternId = adj1.get_adj_vertex().get_id();
        let adj2_v_id: PatternId = adj2.get_adj_vertex().get_id();
        // Compare vertex groups
        let adj1_v_group = self.get_vertex_group(adj1_v_id);
        let adj2_v_group = self.get_vertex_group(adj2_v_id);
        if adj1_v_group.is_some() && adj2_v_group.is_some() {
            match adj1_v_group
                .unwrap()
                .cmp(&adj2_v_group.unwrap())
            {
                Ordering::Less => return Ordering::Less,
                Ordering::Greater => return Ordering::Greater,
                Ordering::Equal => (),
            }
        }

        // Compare vertex ranks
        // Adjacency will be given high priority if its adjacent vertex has no or smaller rank
        // Since vertices in the same pattern will never be given the same rank, two adjacencies cannot be equal.
        let adj1_v_rank = self.get_vertex_rank(adj1_v_id);
        let adj2_v_rank = self.get_vertex_rank(adj2_v_id);
        if adj1_v_rank.is_none() && adj2_v_rank.is_none() {
            return Ordering::Equal;
        } else if adj1_v_rank.is_none() {
            return Ordering::Less;
        } else if adj2_v_rank.is_none() {
            return Ordering::Greater;
        } else {
            adj1_v_rank.unwrap().cmp(&adj2_v_rank.unwrap())
        }
    }

    /// Compare the ranks of two PatternVertices
    ///
    /// Consider labels and out/in degrees only
    ///
    /// Called when setting initial ranks
    fn cmp_vertices(
        &self, pattern: &Pattern, v1_id: PatternId, v2_id: PatternId,
    ) -> IrPatternResult<Ordering> {
        // Compare Label
        let v1_label = pattern
            .get_vertex(v1_id)
            .ok_or(IrPatternError::MissingPatternVertex(v1_id))?
            .get_label();
        let v2_label = pattern
            .get_vertex(v2_id)
            .ok_or(IrPatternError::MissingPatternVertex(v2_id))?
            .get_label();
        match v1_label.cmp(&v2_label) {
            Ordering::Less => return Ok(Ordering::Less),
            Ordering::Greater => return Ok(Ordering::Greater),
            Ordering::Equal => (),
        }

        // Compare Out Degree
        let v1_out_degree = pattern.get_vertex_out_degree(v1_id);
        let v2_out_degree = pattern.get_vertex_out_degree(v2_id);
        match v1_out_degree.cmp(&v2_out_degree) {
            Ordering::Less => return Ok(Ordering::Less),
            Ordering::Greater => return Ok(Ordering::Greater),
            Ordering::Equal => (),
        }

        // Compare In Degree
        let v1_in_degree = pattern.get_vertex_in_degree(v1_id);
        let v2_in_degree = pattern.get_vertex_in_degree(v2_id);
        match v1_in_degree.cmp(&v2_in_degree) {
            Ordering::Less => return Ok(Ordering::Less),
            Ordering::Greater => return Ok(Ordering::Greater),
            Ordering::Equal => (),
        }

        // Compare Adjacencies
        let mut v1_adjacencies_iter = pattern.adjacencies_iter(v1_id);
        let mut v2_adjacencies_iter = pattern.adjacencies_iter(v2_id);
        loop {
            let v1_adjacency = v1_adjacencies_iter.next();
            let v2_adjacency = v2_adjacencies_iter.next();
            if v1_adjacency.is_none() || v2_adjacency.is_none() {
                break;
            }

            // Compare direction and labels
            match self.cmp_adjacencies(v1_adjacency.unwrap(), v2_adjacency.unwrap()) {
                Ordering::Less => return Ok(Ordering::Less),
                Ordering::Greater => return Ok(Ordering::Greater),
                Ordering::Equal => (),
            }
        }

        // Return Equal if Still Cannot Distinguish
        Ok(Ordering::Equal)
    }

    /// Update the order of each record in vertex adjacency map
    ///
    /// The criteria for sorting is the same as function `cmp_adjacencies`
    fn update_vertex_adjacencies_order(&mut self) {
        // Take two maps out as immutable reference
        let vertex_group_map = &self.vertex_group_map;
        let vertex_rank_map = &self.vertex_rank_map;
        self.vertex_adjacencies_map
            .values_mut()
            .for_each(|adjacencies| {
                adjacencies.sort_by(|adj1, adj2| {
                    // Compare the information stored inside adjacencies: label and edge direction
                    let adj1_info_tuple =
                        (adj1.get_direction(), adj1.get_adj_vertex().get_label(), adj1.get_edge_label());
                    let adj2_info_tuple =
                        (adj2.get_direction(), adj2.get_adj_vertex().get_label(), adj2.get_edge_label());
                    match adj1_info_tuple.cmp(&adj2_info_tuple) {
                        Ordering::Less => return Ordering::Less,
                        Ordering::Greater => return Ordering::Greater,
                        Ordering::Equal => (),
                    }

                    let adj1_v_id: PatternId = adj1.get_adj_vertex().get_id();
                    let adj2_v_id: PatternId = adj2.get_adj_vertex().get_id();
                    // Compare vertex groups
                    let adj1_v_group = vertex_group_map.get(&adj1_v_id);
                    let adj2_v_group = vertex_group_map.get(&adj2_v_id);
                    if adj1_v_group.is_some() && adj2_v_group.is_some() {
                        match adj1_v_group
                            .unwrap()
                            .cmp(&adj2_v_group.unwrap())
                        {
                            Ordering::Less => return Ordering::Less,
                            Ordering::Greater => return Ordering::Greater,
                            Ordering::Equal => (),
                        }
                    }

                    // Compare vertex ranks
                    // Adjacency will be given high priority if its adjacent vertex has no or smaller rank
                    // Since vertices in the same pattern will never be given the same rank, two adjacencies cannot be equal.
                    let adj1_v_rank = vertex_rank_map.get(&adj1_v_id);
                    let adj2_v_rank = vertex_rank_map.get(&adj2_v_id);
                    if adj1_v_rank.is_none() && adj2_v_rank.is_none() {
                        return Ordering::Equal;
                    } else if adj1_v_rank.is_none() {
                        return Ordering::Less;
                    } else if adj2_v_rank.is_none() {
                        return Ordering::Greater;
                    } else {
                        adj1_v_rank.unwrap().cmp(&adj2_v_rank.unwrap())
                    }
                });
            });
    }
}

impl Pattern {
    pub fn encode_to(&self) -> Vec<u8> {
        if self.get_edges_num() > 0 {
            let mut edge_ids: Vec<PatternId> = self
                .edges_iter()
                .map(|edge| edge.get_id())
                .collect();
            edge_ids.sort_by(|e1_id, e2_id| {
                let e1_rank = self.get_edge_rank(*e1_id).unwrap();
                let e2_rank = self.get_edge_rank(*e2_id).unwrap();
                e1_rank.cmp(&e2_rank)
            });
            let mut pattern_code = Vec::with_capacity(edge_ids.len() * 20);
            for edge_id in edge_ids {
                let edge = self.get_edge(edge_id).unwrap();
                let edge_label = edge.get_label();
                let start_vertex = edge.get_start_vertex();
                let start_vertex_rank = self
                    .get_vertex_rank(start_vertex.get_id())
                    .unwrap();
                let start_vertex_label = start_vertex.get_label();
                let end_vertex = edge.get_end_vertex();
                let end_vertex_rank = self
                    .get_vertex_rank(end_vertex.get_id())
                    .unwrap();
                let end_vertex_label = end_vertex.get_label();
                pattern_code.extend_from_slice(&label_to_u8_array(edge_label));
                pattern_code.extend_from_slice(&id_to_u8_array(start_vertex_rank));
                pattern_code.extend_from_slice(&label_to_u8_array(start_vertex_label));
                pattern_code.extend_from_slice(&id_to_u8_array(end_vertex_rank));
                pattern_code.extend_from_slice(&label_to_u8_array(end_vertex_label));
            }
            pattern_code
        } else if self.get_vertices_num() == 1 {
            Vec::from(label_to_u8_array(self.get_max_vertex_label().unwrap()))
        } else {
            vec![]
        }
    }

    pub fn decode_from(code: &[u8]) -> Option<Pattern> {
        if code.len() == 0 {
            None
        } else if code.len() % 20 == 0 {
            let mut pattern_edges = Vec::with_capacity(code.len() / 20);
            for i in 0..(code.len() / 20) {
                let edge_id = i;
                let k = i * 20;
                let edge_label = u8_array_to_label(&code[k..k + 4]);
                let start_vertex = PatternVertex::new(
                    u8_array_to_id(&code[k + 4..k + 8]),
                    u8_array_to_label(&code[k + 8..k + 12]),
                );
                let end_vertex = PatternVertex::new(
                    u8_array_to_id(&code[k + 12..k + 16]),
                    u8_array_to_label(&code[k + 16..k + 20]),
                );
                pattern_edges.push(PatternEdge::new(edge_id, edge_label, start_vertex, end_vertex));
            }
            Some(Pattern::try_from(pattern_edges).unwrap())
        } else if code.len() == 4 {
            let pattern_label = u8_array_to_label(code);
            Some(Pattern::from(PatternVertex::new(0, pattern_label)))
        } else {
            None
        }
    }
}

fn label_to_u8_array(label: PatternLabelId) -> [u8; 4] {
    u32_to_u8_array(label as u32)
}

fn id_to_u8_array(id: PatternId) -> [u8; 4] {
    u32_to_u8_array(id as u32)
}

fn u32_to_u8_array(num: u32) -> [u8; 4] {
    let first_u8 = ((num & 0xff000000) >> 24) as u8;
    let second_u8 = ((num & 0xff0000) >> 16) as u8;
    let third_u8 = ((num & 0xff00) >> 8) as u8;
    let fourth_u8 = (num & 0xff) as u8;
    [first_u8, second_u8, third_u8, fourth_u8]
}

fn u8_array_to_label(u8_array: &[u8]) -> PatternLabelId {
    u8_array_to_u32(u8_array) as PatternLabelId
}

fn u8_array_to_id(u8_array: &[u8]) -> PatternId {
    u8_array_to_u32(u8_array) as PatternId
}

fn u8_array_to_u32(u8_array: &[u8]) -> u32 {
    assert_eq!(u8_array.len(), 4);
    u8_array[3] as u32
        + ((u8_array[2] as u32) << 8)
        + ((u8_array[1] as u32) << 16)
        + ((u8_array[0] as u32) << 24)
}
