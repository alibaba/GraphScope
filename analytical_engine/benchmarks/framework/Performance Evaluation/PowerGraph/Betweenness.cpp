#include <vector>
#include <graphlab.hpp>

struct vertex_data {
  int shortest_paths_len;
  int shortest_paths_num;
  float centrality;
  float delta;
  int pred_siz;
  bool finish;

  vertex_data() : centrality(0), shortest_paths_len(0), shortest_paths_num(0), delta(0), pred_siz(0), finish(false) {}

  void save(graphlab::oarchive& oarc) const {
    oarc << centrality << shortest_paths_len << shortest_paths_num << delta << pred_siz << finish;
  }

  void load(graphlab::iarchive& iarc) {
    iarc >> centrality >> shortest_paths_len >> shortest_paths_num >> delta >> pred_siz >> finish;
  }
};

typedef graphlab::distributed_graph<vertex_data, graphlab::empty> graph_type;
struct msg_type : graphlab::IS_POD_TYPE {
  int msg_len, msg_num, msg_siz;
  msg_type() : msg_len(0), msg_num(0), msg_siz(0) {}
  msg_type(int _1, int _2, int _3) : msg_len(_1), msg_num(_2), msg_siz(_3) {}
  msg_type& operator+=(const msg_type& rhs) {
    if (msg_len == rhs.msg_len) {
      msg_num += rhs.msg_num;
      msg_siz += rhs.msg_siz;
    } else if (msg_len > rhs.msg_len) {
      msg_len = rhs.msg_len;
      msg_num = rhs.msg_num;
      msg_siz = rhs.msg_siz;
    }
    return *this;
  }
};
struct msg2_type : graphlab::IS_POD_TYPE {
  float delta;
  int num;
  msg2_type() : delta(0.0), num(0) {}
  msg2_type(float _1, int _2) : delta(_1), num(_2) {}
  msg2_type& operator+=(const msg2_type& rhs) {
    delta += rhs.delta;
    num += rhs.num;
    return *this;
  }
};


class BFS : public graphlab::ivertex_program<graph_type, graphlab::empty, msg_type> , public graphlab::IS_POD_TYPE {
  bool active;
  int local_len;
  int local_num;
  int local_siz;

public:

  void init(icontext_type& context, const vertex_type& vertex, const msg_type& msg) {
    // std::cout << "init  " << vertex.id() << "  msg.len=" << msg.msg_num << " msg.num=" << msg.msg_num << std::endl;
    active = true;
    local_len = msg.msg_len;
    local_num = msg.msg_num;
    local_siz = msg.msg_siz;
  }

  edge_dir_type gather_edges(icontext_type& context, const vertex_type& vertex) const { 
    return graphlab::NO_EDGES;
  };

  void apply(icontext_type& context, vertex_type& vertex, const graphlab::empty& empty) {
    if (active == true) {
      active = false;
      if (vertex.data().shortest_paths_len == 0) {
        // std::cout << "apply active  " << vertex.id() << "   pred_size=" << local_siz << "   degree=" << vertex.num_in_edges() + vertex.num_out_edges() << std::endl;
        active = true;
        vertex.data().shortest_paths_len = local_len;
        vertex.data().shortest_paths_num = local_num;
        vertex.data().pred_siz = local_siz;
      } else {
        vertex.data().pred_siz += local_siz;
      }
    }
  }

  edge_dir_type scatter_edges(icontext_type& context, const vertex_type& vertex) const {
    if (active) {
      // std::cout << "scatter edges active  " << vertex.id() << std::endl;
      return graphlab::ALL_EDGES;
    } else {
      return graphlab::NO_EDGES;
    }
  };

  void scatter(icontext_type& context, const vertex_type& vertex, edge_type& edge) const {
    const vertex_type other = vertex.id() == edge.source().id()? edge.target() : edge.source();
    // std::cout << "scatter signal  " << vertex.id() << " " << other.id() << std::endl;
    if (other.data().shortest_paths_len == 0) {
      const msg_type msg(vertex.data().shortest_paths_len + 1, vertex.data().shortest_paths_num, 1);
      context.signal(other, msg);
    } else if (vertex.data().shortest_paths_len == other.data().shortest_paths_len) {
      const msg_type msg(vertex.data().shortest_paths_len, 0, 1);
      context.signal(other, msg);
    }
  }
};



class Betweenness : public graphlab::ivertex_program<graph_type, graphlab::empty, msg2_type> , public graphlab::IS_POD_TYPE {
  bool active;
  float local_delta;
  int local_num;

public:

  void init(icontext_type& context, const vertex_type& vertex, const msg2_type& msg) {
    // std::cout << "init  " << vertex.id() << "  msg=" << msg.delta << " " << msg.num << std::endl;
    local_delta = msg.delta;
    local_num = msg.num;
  }

  edge_dir_type gather_edges(icontext_type& context, const vertex_type& vertex) const { 
    return graphlab::NO_EDGES;
  };

  void apply(icontext_type& context, vertex_type& vertex, const graphlab::empty& empty) {
    active = false;
    if (vertex.data().pred_siz != vertex.num_in_edges() + vertex.num_out_edges()) {
      vertex.data().delta += local_delta;
      vertex.data().pred_siz += local_num;
      // std::cout << "apply inactive  " << vertex.id() << "    pred=" << vertex.data().pred_siz << std::endl;
    }
    if (vertex.data().pred_siz == vertex.num_in_edges() + vertex.num_out_edges() && vertex.data().finish == false) {
        vertex.data().finish = true;
        active = true;
        vertex.data().delta *= vertex.data().shortest_paths_num;
        // std::cout << "apply active  " << vertex.id() << "    pred=" << vertex.data().pred_siz << std::endl;

      }
  }

  edge_dir_type scatter_edges(icontext_type& context, const vertex_type& vertex) const {
    if (active) {
      // std::cout << "scatter edges active  " << vertex.id() << std::endl;
      return graphlab::ALL_EDGES;
    } else {
      return graphlab::NO_EDGES;
    }
  };

  void scatter(icontext_type& context, const vertex_type& vertex, edge_type& edge) const {
    const vertex_type other = vertex.id() == edge.source().id()? edge.target() : edge.source();
    if (other.data().shortest_paths_len == vertex.data().shortest_paths_len - 1) {
      const msg2_type msg(1.0 / vertex.data().shortest_paths_num * (1 + vertex.data().delta), 1);
      // std::cout << "scatter signal  " << vertex.id() << " -> " << other.id() << "  msg=" << msg.delta << " " << msg.num << std::endl;
      context.signal(other, msg);
    }
  }
};


void initialize_vertex(graph_type::vertex_type& vertex) {
  vertex.data().centrality = 0;
  vertex.data().shortest_paths_len = 0;
  vertex.data().shortest_paths_num = 0;
  vertex.data().delta = 0;
  vertex.data().pred_siz = 0;
  vertex.data().finish = false;
}

/*
 * Loads graphs in the form 'id (id edge_strength)*'
 *
 */
bool line_parser(graph_type& graph, const std::string& filename, const std::string& textline) {
  std::stringstream strm(textline);
  graphlab::vertex_id_type vid;
  // first entry in the line is a vertex ID
  strm >> vid;
  vertex_data node;
  // insert this vertex with its label
  graph.add_vertex(vid, node);
  // while there are elements in the line, continue to read until we fail
  // double edge_val=1.0;
  while(1){
    graphlab::vertex_id_type other_vid;
    strm >> other_vid;
    // strm >> edge_val;
    if (strm.fail())
      break;
    // graph.add_edge(vid, other_vid, edge_val);
    graph.add_edge(vid, other_vid);
  }

  return true;
}


// output
struct betweenness_writer {
  std::string save_vertex(const graph_type::vertex_type& vtx) {
    std::stringstream strm;
    strm << "vertex " << vtx.id() << "  shortest path length=" << vtx.data().shortest_paths_len << "    shortest path number=" << vtx.data().shortest_paths_num << "   pred_size=" << vtx.data().pred_siz <<  "     delta=" << vtx.data().delta << "\n";
    return strm.str();
  }
  std::string save_edge(graph_type::edge_type e) { return ""; }
};

// find last layer
bool depth_is_maximum(const graph_type::vertex_type& vtx) {
  return vtx.data().pred_siz == vtx.num_in_edges() + vtx.num_out_edges();
}

int main(int argc, char** argv) {
  graphlab::mpi_tools::init(argc, argv);
  graphlab::distributed_control dc;
  global_logger().set_log_level(LOG_INFO);

// Parse command line options -----------------------------------------------
  graphlab::command_line_options clopts("Betweeness Algorithm.");
  std::string graph_dir;
  clopts.attach_option("graph", graph_dir, "The graph file. Required ");
  clopts.add_positional("graph");

  std::string saveprefix;
  clopts.attach_option("saveprefix", saveprefix,
                        "If set, will save the resultant betweness score to a "
                        "sequence of files with prefix saveprefix");

  if(!clopts.parse(argc, argv)) {
    dc.cout() << "Error in parsing command line arguments." << std::endl;
    return EXIT_FAILURE;
  }
  if (graph_dir == "") {
    dc.cout() << "Graph not specified. Cannot continue";
    return EXIT_FAILURE;
  }

  clopts.get_engine_args().set_option("type", "synchronous");



// Build the graph ----------------------------------------------------------
  graph_type graph(dc);
  dc.cout() << "Loading graph using line parser" << std::endl;
  graph.load(graph_dir, line_parser);
  graph.finalize();

  graph.transform_vertices(initialize_vertex);

  graphlab::omni_engine<BFS> engine(dc, graph, "synchronous");
  const msg_type msg(1, 1, 0);
  engine.signal(0, msg);
  engine.start();

  if (saveprefix != "") {
    graph.save(saveprefix+"0", betweenness_writer(),
               false,    // do not gzip
               true,     // save vertices
               false);   // do not save edges
  }

  graphlab::vertex_set last_layer = graph.select(depth_is_maximum);
  graphlab::omni_engine<Betweenness> engine2(dc, graph, "synchronous");
  engine2.signal_vset(last_layer);
  engine2.start();
  const float runtime = engine.elapsed_seconds();
  dc.cout() << "Finished Running engine in " << runtime
            << " seconds." << std::endl;


  if (saveprefix != "") {
    graph.save(saveprefix, betweenness_writer(),
               false,    // do not gzip
               true,     // save vertices
               false);   // do not save edges
  }

  graphlab::mpi_tools::finalize();
  return 0;
}
