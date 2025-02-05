#include "MutableVertexPartition.h"

#ifdef DEBUG
  using std::cerr;
  using std::endl;
#endif

/****************************************************************************
  Create a new vertex partition.

  Parameters:
    graph            -- The igraph.Graph on which this partition is defined.
    membership=None  -- The membership vector of this partition, i.e. an
                        community number for each node. So membership[i] = c
                        implies that node i is in community c. If None, it is
                        initialised with each node in its own community.
    weight_attr=None -- What edge attribute should be used as a weight for the
                        edges? If None, the weight defaults to 1.
    size_attr=None   -- What node attribute should be used for keeping track
                        of the size of the node? In some methods (e.g. CPM or
                        Significance), we need to keep track of the total
                        size of the community. So when we aggregate/collapse
                        the graph, we should know how many nodes were in a
                        community. If None, the size of a node defaults to 1.
    self_weight_attr=None
                     -- What node attribute should be used for the self
                        weight? If None, the self_weight is
                        recalculated each time."""
*****************************************************************************/

MutableVertexPartition::MutableVertexPartition(Graph* graph,
      vector<size_t> const& membership)
{
  this->destructor_delete_graph = false;
  this->graph = graph;
  if (membership.size() != graph->vcount())
  {
    throw Exception("Membership vector has incorrect size.");
  }
  this->_membership = membership;
  this->init_admin();
}

MutableVertexPartition::MutableVertexPartition(Graph* graph)
{
  this->destructor_delete_graph = false;
  this->graph = graph;
  this->_membership = range(graph->vcount());
  this->init_admin();
}

MutableVertexPartition* MutableVertexPartition::create(Graph* graph)
{
  return new MutableVertexPartition(graph);
}

MutableVertexPartition* MutableVertexPartition::create(Graph* graph, vector<size_t> const& membership)
{
  return new MutableVertexPartition(graph, membership);
}


MutableVertexPartition::~MutableVertexPartition()
{
  this->clean_mem();
  if (this->destructor_delete_graph)
    delete this->graph;
}

void MutableVertexPartition::clean_mem()
{

}

double MutableVertexPartition::csize(size_t comm)
{
  if (comm < this->_csize.size())
    return this->_csize[comm];
  else
    return 0;
}

size_t MutableVertexPartition::cnodes(size_t comm)
{
  if (comm < this->_cnodes.size())
    return this->_cnodes[comm];
  else
    return 0;
}

vector<size_t> MutableVertexPartition::get_community(size_t comm)
{
  vector<size_t> community;
  community.reserve(this->_cnodes[comm]);
  for (size_t i = 0; i < this->graph->vcount(); i++)
    if (this->_membership[i] == comm)
      community.push_back(i);
  return community;
}

vector< vector<size_t> > MutableVertexPartition::get_communities()
{
  vector< vector<size_t> > communities(this->_n_communities);

  for (size_t c = 0; c < this->_n_communities; c++)
  {
    size_t cn = this->_cnodes[c];
    communities[c].reserve(cn);
  }

  for (size_t i = 0; i < this->graph->vcount(); i++)
      communities[this->_membership[i]].push_back(i);

  return communities;
}

size_t MutableVertexPartition::n_communities()
{
  return this->_n_communities;
}

/****************************************************************************
  Initialise all the administration based on the membership vector.
*****************************************************************************/

void MutableVertexPartition::init_admin()
{
  #ifdef DEBUG
    cerr << "void MutableVertexPartition::init_admin()" << endl;
  #endif
  size_t n = this->graph->vcount();

  // First determine number of communities (assuming they are consecutively numbered
  this->update_n_communities();

  // Reset administration
  this->_total_weight_in_comm.clear();
  this->_total_weight_in_comm.resize(this->_n_communities);
  this->_total_weight_from_comm.clear();
  this->_total_weight_from_comm.resize(this->_n_communities);
  this->_total_weight_to_comm.clear();
  this->_total_weight_to_comm.resize(this->_n_communities);
  this->_csize.clear();
  this->_csize.resize(this->_n_communities);
  this->_cnodes.clear();
  this->_cnodes.resize(this->_n_communities);

  this->_current_node_cache_community_from = n + 1; this->_cached_weight_from_community.resize(this->_n_communities, 0);
  this->_current_node_cache_community_to = n + 1;   this->_cached_weight_to_community.resize(this->_n_communities, 0);
  this->_current_node_cache_community_all = n + 1;  this->_cached_weight_all_community.resize(this->_n_communities, 0);
  this->_cached_neigh_comms_all.resize(n);

  if (this->get_graph()->is_directed()) {
    this->_cached_neigh_comms_from.resize(n);
    this->_cached_neigh_comms_to.resize(n);
  }

  this->_empty_communities.clear();

  this->_total_weight_in_all_comms = 0.0;
  for (size_t v = 0; v < n; v++)
  {
    size_t v_comm = this->_membership[v];
    // Update the community size
    this->_csize[v_comm] += this->graph->node_size(v);
    // Update the community size
    this->_cnodes[v_comm] += 1;
  }

  size_t m = graph->ecount();
  for (size_t e = 0; e < m; e++)
  {
    size_t v, u;
    this->graph->edge(e, v, u);

    size_t v_comm = this->_membership[v];
    size_t u_comm = this->_membership[u];

    // Get the weight of the edge
    double w = this->graph->edge_weight(e);
    // Add weight to the outgoing weight of community of v
    this->_total_weight_from_comm[v_comm] += w;
    #ifdef DEBUG
      cerr << "\t" << "Add (" << v << ", " << u << ") weight " << w << " to from_comm " << v_comm <<  "." << endl;
    #endif
    // Add weight to the incoming weight of community of u
    this->_total_weight_to_comm[u_comm] += w;
    #ifdef DEBUG
      cerr << "\t" << "Add (" << v << ", " << u << ") weight " << w << " to to_comm " << u_comm << "." << endl;
    #endif
    if (!this->graph->is_directed())
    {
      #ifdef DEBUG
        cerr << "\t" << "Add (" << u << ", " << v << ") weight " << w << " to from_comm " << u_comm <<  "." << endl;
      #endif
      this->_total_weight_from_comm[u_comm] += w;
      #ifdef DEBUG
        cerr << "\t" << "Add (" << u << ", " << v << ") weight " << w << " to to_comm " << v_comm << "." << endl;
      #endif
      this->_total_weight_to_comm[v_comm] += w;
    }
    // If it is an edge within a community
    if (v_comm == u_comm)
    {
      this->_total_weight_in_comm[v_comm] += w;
      this->_total_weight_in_all_comms += w;
      #ifdef DEBUG
        cerr << "\t" << "Add (" << v << ", " << u << ") weight " << w << " to in_comm " << v_comm << "." << endl;
      #endif
    }
  }

  this->_total_possible_edges_in_all_comms = 0;
  for (size_t c = 0; c < this->_n_communities; c++)
  {
    double n_c = this->csize(c);
    double possible_edges = this->graph->possible_edges(n_c);

    #ifdef DEBUG
      cerr << "\t" << "c=" << c << ", n_c=" << n_c << ", possible_edges=" << possible_edges << endl;
    #endif

    this->_total_possible_edges_in_all_comms += possible_edges;

    // It is possible that some community have a zero size (if the order
    // is for example not consecutive. We add those communities to the empty
    // communities vector for consistency.
    if (this->_cnodes[c] == 0)
      this->_empty_communities.push_back(c);
  }

  #ifdef DEBUG
    cerr << "exit MutableVertexPartition::init_admin()" << endl << endl;
  #endif

}

void MutableVertexPartition::update_n_communities()
{
  this->_n_communities = 0;
  for (size_t i = 0; i < this->graph->vcount(); i++)
    if (this->_membership[i] >= this->_n_communities)
      this->_n_communities = this->_membership[i] + 1;
}

/****************************************************************************
 Renumber the communities so that they are numbered 0,...,q-1 where q is
 the number of communities. This also removes any empty communities, as they
 will not be given a new number.
*****************************************************************************/
void MutableVertexPartition::renumber_communities()
{
  vector<MutableVertexPartition*> partitions(1);
  partitions[0] = this;
  vector<size_t> new_comm_id = MutableVertexPartition::rank_order_communities(partitions);
  this->relabel_communities(new_comm_id);
}

/****************************************************************************
 Renumber the communities according to the new labels in new_comm_id.

 This adjusts the internal bookkeeping as required, avoiding the more costly
 setup required in init_admin(). In particular, this avoids recomputation of
 weights in/from/to each community by simply assigning the previously
 computed values to the new, relabeled communities.

 For instance, a new_comm_id of <1, 2, 0> will change the labels such that
 community 0 becomes 1, community 1 becomes 2, and community 2 becomes 0.
*****************************************************************************/
void MutableVertexPartition::relabel_communities(vector<size_t> const& new_comm_id) {
  if (this->_n_communities != new_comm_id.size()) {
    throw Exception("Problem swapping community labels. Mismatch between n_communities and new_comm_id vector.");
  }

  size_t n = this->graph->vcount();

  for (size_t i = 0; i < n; i++)
    this->_membership[i] = new_comm_id[this->_membership[i]];

  this->update_n_communities();
  size_t nbcomms = this->n_communities();

  vector<double> new_total_weight_in_comm(nbcomms, 0.0);
  vector<double> new_total_weight_from_comm(nbcomms, 0.0);
  vector<double> new_total_weight_to_comm(nbcomms, 0.0);
  vector<double> new_csize(nbcomms, 0);
  vector<size_t> new_cnodes(nbcomms, 0);

  // Relabel community admin
  for (size_t c = 0; c < new_comm_id.size(); c++) {
    size_t new_c = new_comm_id[c];
    if (this->_cnodes[c] > 0) {
      new_total_weight_in_comm[new_c] = this->_total_weight_in_comm[c];
      new_total_weight_from_comm[new_c] = this->_total_weight_from_comm[c];
      new_total_weight_to_comm[new_c] = this->_total_weight_to_comm[c];
      new_csize[new_c] = this->_csize[c];
      new_cnodes[new_c] = this->_cnodes[c];
    }
  }

  this->_total_weight_in_comm = new_total_weight_in_comm;
  this->_total_weight_from_comm = new_total_weight_from_comm;
  this->_total_weight_to_comm = new_total_weight_to_comm;
  this->_csize = new_csize;
  this->_cnodes = new_cnodes;

  this->_empty_communities.clear();
  for (size_t c = 0; c < nbcomms; c++) {
    if (this->_cnodes[c] == 0) {
      this->_empty_communities.push_back(c);
    }
  }

  // invalidate cached weight vectors
  for (size_t c : this->_cached_neigh_comms_from)
    this->_cached_weight_from_community[c] = 0;
  this->_cached_neigh_comms_from.clear();
  this->_cached_weight_from_community.resize(nbcomms, 0);
  this->_current_node_cache_community_from = n + 1;

  for (size_t c : this->_cached_neigh_comms_to)
    this->_cached_weight_to_community[c] = 0;
  this->_cached_neigh_comms_to.clear();
  this->_cached_weight_to_community.resize(nbcomms, 0);
  this->_current_node_cache_community_to = n + 1;

  for (size_t c : this->_cached_neigh_comms_all)
    this->_cached_weight_all_community[c] = 0;
  this->_cached_neigh_comms_all.clear();
  this->_cached_weight_all_community.resize(nbcomms, 0);
  this->_current_node_cache_community_all = n + 1;

  #ifdef DEBUG
    if (this->_csize.size() < this->_n_communities ||
        this->_cnodes.size() < this->_n_communities ||
        this->_total_weight_in_comm.size() < this->_n_communities ||
        this->_total_weight_to_comm.size() < this->_n_communities ||
        this->_total_weight_from_comm.size() < this->_n_communities ||
        this->_cached_weight_from_community.size() < this->_n_communities ||
        this->_cached_weight_to_community.size() < this->_n_communities ||
        this->_cached_weight_all_community.size() < this->_n_communities) {
      cerr << "ERROR: MutableVertexPartition bookkeeping is too small after rearrange_community_labels." << endl;
    }

    this->init_admin();

    for (size_t c = 0; c < this->_n_communities; c++) {
      if (fabs(new_total_weight_in_comm[c] - this->_total_weight_in_comm[c]) > 1e-6 ||
          fabs(new_total_weight_from_comm[c] - this->_total_weight_from_comm[c]) > 1e-6 ||
          fabs(new_total_weight_to_comm[c] - this->_total_weight_to_comm[c]) > 1e-6 ||
          new_csize[c] != this->_csize[c] ||
          new_cnodes[c] != this->_cnodes[c]) {
        cerr << "ERROR: MutableVertexPartition bookkeeping is incorrect after rearrange_community_labels." << endl;
        cerr << "Community c has " << endl
             << "total_weight_in_comm=" << new_total_weight_in_comm[c]
             << " (should be " << this->_total_weight_in_comm[c] << ")" << endl
             << "total_weight_from_comm=" << new_total_weight_from_comm[c]
             << " (should be " << this->_total_weight_from_comm[c] << ")" << endl
             << "total_weight_to_comm=" << new_total_weight_to_comm[c]
             << " (should be " << this->_total_weight_to_comm[c] << ")" << endl
             << "csize=" << new_csize[c]
             << " (should be " << this->_csize[c] << ")" << endl
             << "cnodes=" << new_cnodes[c]
             << " (should be " << this->_cnodes[c] << ")" << endl;
      }
    }
  #endif
}

vector<size_t> MutableVertexPartition::rank_order_communities(vector<MutableVertexPartition*> partitions)
{
  size_t nb_layers = partitions.size();
  size_t nb_comms = partitions[0]->n_communities();

  #ifdef DEBUG
    size_t n = partitions[0]->graph->vcount();
    for (size_t layer = 0; layer < nb_layers; layer++)
    {
      for (size_t v = 0; v < n; v++)
      {
        if (partitions[0]->membership(v) != partitions[layer]->membership(v))
          cerr << "Membership of all partitions are not equal";
      }
    }
  #endif
  // First sort the communities by size
  // Csizes
  // first - community
  // second - csize
  // third - number of nodes (may be aggregate nodes), to account for communities with zero weight.
  vector<size_t*> csizes;
  for (size_t i = 0; i < nb_comms; i++)
  {
      double csize = 0;
      for (size_t layer = 0; layer < nb_layers; layer++)
        csize += partitions[layer]->csize(i);

      size_t* row = new size_t[3];
      row[0] = i;
      row[1] = csize;
      row[2] = partitions[0]->cnodes(i);
      csizes.push_back(row);
  }
  sort(csizes.begin(), csizes.end(), orderCSize);

  // Then use the sort order to assign new communities,
  // such that the largest community gets the lowest index.
  vector<size_t> new_comm_id(nb_comms, 0);
  for (size_t i = 0; i < nb_comms; i++)
  {
    size_t comm = csizes[i][0];
    new_comm_id[comm] = i;
    delete[] csizes[i];
  }

  return new_comm_id;
}


/****************************************************************************
 Renumber the communities using the original fixed membership vector. Notice
 that this doesn't ensure any property of the community numbers.
*****************************************************************************/
void MutableVertexPartition::renumber_communities(vector<size_t> const& fixed_nodes, vector<size_t> const& fixed_membership)
{

  #ifdef DEBUG
    cerr << "void MutableVertexPartition::renumber_communities(" << &fixed_nodes << ", " << &fixed_membership << ")" << endl;
  #endif

  // Skip whole thing if there are no fixed nodes for efficiency
  if (fixed_nodes.size() == 0)
    return;

  // The number of communities does not depend on whether some are fixed
  size_t nb_comms = n_communities();

  // Fill the community map with the original communities
  vector<size_t> new_comm_id(nb_comms);
  vector<bool> comm_assigned_bool(nb_comms);
  priority_queue<size_t, vector<size_t>, std::greater<size_t> > new_comm_assigned;
  for (size_t v : fixed_nodes) {
    if (!comm_assigned_bool[_membership[v]])
    {
      size_t fixed_comm_v = fixed_membership[v];
      #ifdef DEBUG
        cerr << "Setting map for fixed community " << fixed_comm_v << endl;
      #endif
      new_comm_id[_membership[v]] = fixed_comm_v;
      comm_assigned_bool[_membership[v]] = true;
      new_comm_assigned.push(fixed_comm_v);
    }
  }

  // Index of the most recently added community
  size_t cc = 0;
  for (size_t c = 0; c != nb_comms; c++) {
    if(!comm_assigned_bool[c]) {
      // Look for the first free integer
      while (!new_comm_assigned.empty() && cc == new_comm_assigned.top())
      {
          new_comm_assigned.pop();
          cc++;
      }
      // Assign the community
      #ifdef DEBUG
        cerr << "Setting map for free community " << cc << endl;
      #endif
      new_comm_id[c] = cc++;
    }
  }

  this->relabel_communities(new_comm_id);
}

void MutableVertexPartition::renumber_communities(vector<size_t> const& membership)
{
  cerr << "This function is deprecated, use MutableVertexPartition::set_membership(vector<size_t> const& membership)" << endl;
  this->set_membership(membership);
}

size_t MutableVertexPartition::get_empty_community()
{
  if (this->_empty_communities.empty())
  {
    // If there was no empty community yet,
    // we will create a new one.
    add_empty_community();
  }

  return this->_empty_communities.back();
}

void MutableVertexPartition::set_membership(vector<size_t> const& membership)
{
  #ifdef DEBUG
    cerr << "void MutableVertexPartition::set_membership(" << &membership << ")" << endl;
  #endif
  this->_membership = membership;

  this->clean_mem();
  this->init_admin();
  #ifdef DEBUG
    cerr << "exit MutableVertexPartition::set_membership(" << &membership << ")" << endl;
  #endif
}

size_t MutableVertexPartition::add_empty_community()
{
  this->_n_communities = this->_n_communities + 1;

  if (this->_n_communities > this->graph->vcount())
    throw Exception("There cannot be more communities than nodes, so there must already be an empty community.");

  size_t new_comm = this->_n_communities - 1;

  this->_csize.resize(this->_n_communities);                  this->_csize[new_comm] = 0;
  this->_cnodes.resize(this->_n_communities);                 this->_cnodes[new_comm] = 0;
  this->_total_weight_in_comm.resize(this->_n_communities);   this->_total_weight_in_comm[new_comm] = 0;
  this->_total_weight_from_comm.resize(this->_n_communities); this->_total_weight_from_comm[new_comm] = 0;
  this->_total_weight_to_comm.resize(this->_n_communities);   this->_total_weight_to_comm[new_comm] = 0;

  this->_cached_weight_all_community.resize(this->_n_communities);
  this->_cached_weight_from_community.resize(this->_n_communities);
  this->_cached_weight_to_community.resize(this->_n_communities);

  this->_empty_communities.push_back(new_comm);
  #ifdef DEBUG
    cerr << "Added empty community " << new_comm << endl;
  #endif
  return new_comm;
}

/****************************************************************************
  Move a node to a new community and update the administration.
  Parameters:
    v        -- Node to move.
    new_comm -- To which community should it move.
*****************************************************************************/
void MutableVertexPartition::move_node(size_t v,size_t new_comm)
{
  #ifdef DEBUG
    cerr << "void MutableVertexPartition::move_node(" << v << ", " << new_comm << ")" << endl;
    if (new_comm >= this->n_communities())
      cerr << "ERROR: New community (" << new_comm << ") larger than total number of communities (" << this->n_communities() << ")." << endl;
  #endif
  // Move node and update internal administration
  if (new_comm >= this->_n_communities)
  {
    if (new_comm < this->graph->vcount())
    {
      while (new_comm >= this->_n_communities)
        this->add_empty_community();
    }
    else
    {
      throw Exception("Cannot add new communities beyond the number of nodes.");
    }
  }

  // Keep track of all possible edges in all communities;
  double node_size = this->graph->node_size(v);
  size_t old_comm = this->_membership[v];
  #ifdef DEBUG
    cerr << "Node size: " << node_size << ", old comm: " << old_comm << ", new comm: " << new_comm << endl;
  #endif
  // Incidentally, this is independent of whether we take into account self-loops or not
  // (i.e. whether we count as n_c^2 or as n_c(n_c - 1). Be careful to do this before the
  // adaptation of the community sizes, otherwise the calculations are incorrect.
  if (new_comm != old_comm)
  {
    double delta_possible_edges_in_comms = 2.0*node_size*(ptrdiff_t)(this->_csize[new_comm] - this->_csize[old_comm] + node_size)/(2.0 - this->graph->is_directed());
    _total_possible_edges_in_all_comms += delta_possible_edges_in_comms;
    #ifdef DEBUG
      cerr << "Change in possible edges in all comms: " << delta_possible_edges_in_comms << endl;
    #endif
  }

  // Remove from old community
  #ifdef DEBUG
    cerr << "Removing from old community " << old_comm << ", community size: " << this->_csize[old_comm] << endl;
  #endif
  this->_cnodes[old_comm] -= 1;
  this->_csize[old_comm] -= node_size;
  #ifdef DEBUG
    cerr << "Removed from old community." << endl;
  #endif

  // We have to use the size of the set of nodes rather than the csize
  // to account for nodes that have a zero size (i.e. community may not be empty, but
  // may have zero size).
  if (this->_cnodes[old_comm] == 0)
  {
    #ifdef DEBUG
      cerr << "Adding community " << old_comm << " to empty communities." << endl;
    #endif
    this->_empty_communities.push_back(old_comm);
    #ifdef DEBUG
      cerr << "Added community " << old_comm << " to empty communities." << endl;
    #endif
  }

  if (this->_cnodes[new_comm] == 0)
  {
    #ifdef DEBUG
      cerr << "Removing from empty communities (number of empty communities is " << this->_empty_communities.size() << ")." << endl;
    #endif
    vector<size_t>::reverse_iterator it_comm = this->_empty_communities.rbegin();
    while (it_comm != this->_empty_communities.rend() && *it_comm != new_comm)
    {
      #ifdef DEBUG
        cerr << "Empty community " << *it_comm << " != new community " << new_comm << endl;
      #endif
      it_comm++;
    }
    #ifdef DEBUG
      cerr << "Erasing empty community " << *it_comm << endl;
      if (it_comm == this->_empty_communities.rend())
        cerr << "ERROR: empty community does not exist." << endl;
    #endif
    if (it_comm != this->_empty_communities.rend())
      this->_empty_communities.erase( (++it_comm).base() );
  }

  #ifdef DEBUG
    cerr << "Adding to new community " << new_comm << ", community size: " << this->_csize[new_comm] << endl;
  #endif
  // Add to new community
  this->_cnodes[new_comm] += 1;
  this->_csize[new_comm] += this->graph->node_size(v);

  // Switch outgoing links
  #ifdef DEBUG
    cerr << "Added to new community." << endl;
  #endif

  // Use set for incident edges, because self loop appears twice
  igraph_neimode_t modes[2] = {IGRAPH_OUT, IGRAPH_IN};
  for (size_t mode_i = 0; mode_i < 2; mode_i++)
  {
    igraph_neimode_t mode = modes[mode_i];

    // Loop over all incident edges
    vector<size_t> const& neighbours = this->graph->get_neighbours(v, mode);
    vector<size_t> const& neighbour_edges = this->graph->get_neighbour_edges(v, mode);

    size_t degree = neighbours.size();

    #ifdef DEBUG
      if (mode == IGRAPH_OUT)
        cerr << "\t" << "Looping over outgoing links." << endl;
      else if (mode == IGRAPH_IN)
        cerr << "\t" << "Looping over incoming links." << endl;
      else
        cerr << "\t" << "Looping over unknown mode." << endl;
    #endif

    for (size_t idx = 0; idx < degree; idx++)
    {
      size_t u = neighbours[idx];
      size_t e = neighbour_edges[idx];

      size_t u_comm = this->_membership[u];
      // Get the weight of the edge
      double w = this->graph->edge_weight(e);
      if (mode == IGRAPH_OUT)
      {
        // Remove the weight from the outgoing weights of the old community
        this->_total_weight_from_comm[old_comm] -= w;
        // Add the weight to the outgoing weights of the new community
        this->_total_weight_from_comm[new_comm] += w;
        #ifdef DEBUG
          cerr << "\t" << "Moving link (" << v << "-" << u << ") "
               << "outgoing weight " << w
               << " from " << old_comm << " to " << new_comm
               << "." << endl;
        #endif
      }
      else if (mode == IGRAPH_IN)
      {
        // Remove the weight from the outgoing weights of the old community
        this->_total_weight_to_comm[old_comm] -= w;
        // Add the weight to the outgoing weights of the new community
        this->_total_weight_to_comm[new_comm] += w;
        #ifdef DEBUG
          cerr << "\t" << "Moving link (" << v << "-" << u << ") "
               << "incoming weight " << w
               << " from " << old_comm << " to " << new_comm
               << "." << endl;
        #endif
      }
      else
        throw Exception("Incorrect mode for updating the admin.");
      // Get internal weight (if it is an internal edge)
      double int_weight = w/(this->graph->is_directed() ? 1.0 : 2.0)/( u == v ? 2.0 : 1.0);
      // If it is an internal edge in the old community
      if (old_comm == u_comm)
      {
        // Remove the internal weight
        this->_total_weight_in_comm[old_comm] -= int_weight;
        this->_total_weight_in_all_comms -= int_weight;
        #ifdef DEBUG
          cerr << "\t" << "From link (" << v << "-" << u << ") "
               << "remove internal weight " << int_weight
               << " from " << old_comm << "." << endl;
        #endif
      }
      // If it is an internal edge in the new community
      // i.e. if u is in the new community, or if it is a self loop
      if ((new_comm == u_comm) || (u == v))
      {
        // Add the internal weight
        this->_total_weight_in_comm[new_comm] += int_weight;
        this->_total_weight_in_all_comms += int_weight;
        #ifdef DEBUG
          cerr << "\t" << "From link (" << v << "-" << u << ") "
               << "add internal weight " << int_weight
               << " to " << new_comm << "." << endl;
        #endif
      }
    }
  }
  #ifdef DEBUG
    // Check this->_total_weight_in_all_comms
    double check_total_weight_in_all_comms = 0.0;
    for (size_t c = 0; c < this->n_communities(); c++)
      check_total_weight_in_all_comms += this->total_weight_in_comm(c);
    cerr << "Internal _total_weight_in_all_comms=" << this->_total_weight_in_all_comms
         << ", calculated check_total_weight_in_all_comms=" << check_total_weight_in_all_comms << endl;
  #endif
  // Update the membership vector
  this->_membership[v] = new_comm;
  #ifdef DEBUG
    cerr << "exit MutableVertexPartition::move_node(" << v << ", " << new_comm << ")" << endl << endl;
  #endif
}


/****************************************************************************
 Read new communities from coarser partition assuming that the community
 represents a node in the coarser partition (with the same index as the
 community number).
****************************************************************************/
void MutableVertexPartition::from_coarse_partition(vector<size_t> const& coarse_partition_membership)
{
  this->from_coarse_partition(coarse_partition_membership, this->_membership);
}

void MutableVertexPartition::from_coarse_partition(MutableVertexPartition* coarse_partition)
{
  this->from_coarse_partition(coarse_partition, this->_membership);
}

void MutableVertexPartition::from_coarse_partition(MutableVertexPartition* coarse_partition, vector<size_t> const& coarse_node)
{
  this->from_coarse_partition(coarse_partition->membership(), coarse_node);
}

/****************************************************************************
 Set the current community of all nodes to the community specified in the partition
 assuming that the coarser partition is created using the membership as specified
 by coarser_membership. In other words node i becomes node coarse_node[i] in
 the coarser partition and thus has community coarse_partition_membership[coarse_node[i]].
****************************************************************************/
void MutableVertexPartition::from_coarse_partition(vector<size_t> const& coarse_partition_membership, vector<size_t> const& coarse_node)
{
  // Read the coarser partition
  for (size_t v = 0; v < this->graph->vcount(); v++)
  {
    // In the coarser partition, the node should have the community id
    // as represented by the coarser_membership vector
    size_t v_level2 = coarse_node[v];

    // In the coarser partition, this node is represented by v_level2
    size_t v_comm_level2 = coarse_partition_membership[v_level2];

    // Set local membership to community found for node at second level
    this->_membership[v] = v_comm_level2;
  }

  this->clean_mem();
  this->init_admin();
}


/****************************************************************************
 Read new partition from another partition.
****************************************************************************/
void MutableVertexPartition::from_partition(MutableVertexPartition* partition)
{
  // Assign the membership of every node in the supplied partition
  // to the one in this partition
  for (size_t v = 0; v < this->graph->vcount(); v++)
    this->_membership[v] = partition->membership(v);
  this->clean_mem();
  this->init_admin();
}

void MutableVertexPartition::cache_neigh_communities(size_t v, igraph_neimode_t mode)
{
  // TODO: We can probably calculate at once the IN, OUT and ALL
  // rather than this being called multiple times.

  // Weight between vertex and community
  #ifdef DEBUG
    cerr << "double MutableVertexPartition::cache_neigh_communities(" << v << ", " << mode << ")." << endl;
  #endif
  vector<double>* _cached_weight_tofrom_community = NULL;
  vector<size_t>* _cached_neighs_comms = NULL;
  switch (mode)
  {
    case IGRAPH_IN:
      _cached_weight_tofrom_community = &(this->_cached_weight_from_community);
      _cached_neighs_comms = &(this->_cached_neigh_comms_from);
      break;
    case IGRAPH_OUT:
      _cached_weight_tofrom_community = &(this->_cached_weight_to_community);
      _cached_neighs_comms = &(this->_cached_neigh_comms_to);
      break;
    case IGRAPH_ALL:
      _cached_weight_tofrom_community = &(this->_cached_weight_all_community);
      _cached_neighs_comms = &(this->_cached_neigh_comms_all);
      break;
  }

  // Reset cached communities
  for (size_t c : *_cached_neighs_comms)
       (*_cached_weight_tofrom_community)[c] = 0;

  // Loop over all incident edges
  vector<size_t> const& neighbours = this->graph->get_neighbours(v, mode);
  vector<size_t> const& neighbour_edges = this->graph->get_neighbour_edges(v, mode);

  size_t degree = neighbours.size();

  // Reset cached neighbours
  _cached_neighs_comms->clear();
  for (size_t idx = 0; idx < degree; idx++)
  {
    size_t u = neighbours[idx];
    size_t e = neighbour_edges[idx];

    // If it is an edge to the requested community
    #ifdef DEBUG
      size_t v_comm = this->_membership[v];
    #endif
    size_t comm = this->_membership[u];
    // Get the weight of the edge
    double w = this->graph->edge_weight(e);
    // Self loops appear twice here if the graph is undirected, so divide by 2.0 in that case.
    if (u == v && !this->graph->is_directed())
        w /= 2.0;
    #ifdef DEBUG
      cerr << "\t" << "Edge (" << v << "-" << u << "), Comm (" << v_comm << "-" << comm << ") weight: " << w << "." << endl;
    #endif
    (*_cached_weight_tofrom_community)[comm] += w;
    // REMARK: Notice in the rare case of negative weights, being exactly equal
    // for a certain community, that this community may then potentially be added multiple
    // times to the _cached_neighs. However, I don' believe this causes any further issue,
    // so that's why I leave this here as is.
    if ((*_cached_weight_tofrom_community)[comm] != 0)
      _cached_neighs_comms->push_back(comm);
  }
  #ifdef DEBUG
    cerr << "exit Graph::cache_neigh_communities(" << v << ", " << mode << ")." << endl;
  #endif
}

vector<size_t> const& MutableVertexPartition::get_neigh_comms(size_t v, igraph_neimode_t mode)
{
  if (!this->get_graph()->is_directed())
    mode = IGRAPH_ALL; // igraph ignores mode for undirected graphs

  switch (mode)
  {
    case IGRAPH_IN:
      if (this->_current_node_cache_community_from != v)
      {
        cache_neigh_communities(v, mode);
        this->_current_node_cache_community_from = v;
      }
      return this->_cached_neigh_comms_from;
    case IGRAPH_OUT:
      if (this->_current_node_cache_community_to != v)
      {
        cache_neigh_communities(v, mode);
        this->_current_node_cache_community_to = v;
      }
      return this->_cached_neigh_comms_to;
    case IGRAPH_ALL:
      if (this->_current_node_cache_community_all != v)
      {
        cache_neigh_communities(v, mode);
        this->_current_node_cache_community_all = v;
      }
      return this->_cached_neigh_comms_all;
  }
  throw Exception("Problem obtaining neighbour communities, invalid mode.");
}

vector<size_t> MutableVertexPartition::get_neigh_comms(size_t v, igraph_neimode_t mode, vector<size_t> const& constrained_membership)
{
  vector<size_t> neigh_comms;
  vector<bool> comm_added(this->n_communities(), false);
  for (size_t u : this->graph->get_neighbours(v, mode))
  {
    if (constrained_membership[v] == constrained_membership[u])
    {
      size_t comm = this->membership(u);
      if (!comm_added[comm])
      {
        neigh_comms.push_back(comm);
        comm_added[comm];
      }
    }
  }
  return neigh_comms;
}
