# Tutorial: Develop your Algorithm in C++ with PIE Model

In this tutorial, we will demonstrate how to develop an algorithm using the PIE model in C++ programming language.

## Prerequisites:
- Familiarity with the [PIE programming model](https://graphscope.io/docs/latest/analytical_engine/programming_model_pie.html) 
- Install GraphScope by using the command `pip3 install graphscope`

Then, We will guide you through developing a simple PIE algorithm that computes the degree for each vertex in the graph. The full API can be found in [Analytical Engine API Doc](https://graphscope.io/docs/latest/reference/analytical_engine_index.html) and [libgrape-lite API](https://alibaba.github.io/libgrape-lite).

## Step 1: Define the context class

First, we create a context class that inherits from `grape::VertexDataContext`. This class will store and manage algorithm-specific data and parameters.

```cpp
template <typename FRAG_T>
class MyAppContext : public grape::VertexDataContext<FRAG_T, uint64_t> {
  using oid_t = typename FRAG_T::oid_t;
  using vid_t = typename FRAG_T::vid_t;
  using vertex_t = typename FRAG_T::vertex_t;

 public:
  explicit MyAppContext(const FRAG_T& fragment)
      : grape::VertexDataContext<FRAG_T, uint64_t>(fragment, true),
        result(this->data()) {}

  void Init(grape::ParallelMessageManager& messages, int param1) {
    this->step = 0;
    this->param1 = param1;
    result.SetValue(0);
  }

  int step = 0;
  int param1 = 0;
  typename FRAG_T::template vertex_array_t<uint64_t>& result;
};
```

As shown in the code, the MyAppContext class defines two member variables called `step` and `param1` to store the current superstep and algorithm-specific parameter, respectively. And we also define a member variable named `result` with `uint64_t` type to store the the degree for each vertex in the fragment. The `Init` method is used to initialize the context of the computation. In current example, we initialize the `step` and `param1` variables to zero and the algorithm-specific parameter. We also set the result to zero for each vertex.

## Step 2: Define the Algorithm class

Next, we define the `MyApp` class, which is responsible for implementing the algorithm by using the `MyAppContext` class.

```cpp
template <typename FRAG_T>
class MyApp : public grape::ParallelAppBase<FRAG_T, MyAppContext<FRAG_T>>,
              public grape::ParallelEngine,
              public grape::Communicator {
 public:
  INSTALL_PARALLEL_WORKER(MyApp<FRAG_T>, MyAppContext<FRAG_T>, FRAG_T)
  static constexpr grape::MessageStrategy message_strategy =
      grape::MessageStrategy::kSyncOnOuterVertex;
  static constexpr grape::LoadStrategy load_strategy =
      grape::LoadStrategy::kBothOutIn;
  using vertex_t = typename fragment_t::vertex_t;

  void PEval(const fragment_t& fragment, context_t& context,
             message_manager_t& messages) {
    messages.InitChannels(thread_num());
    // We put all compute logic in IncEval phase, thus do nothing but force continue.
    messages.ForceContinue();
  }

  void IncEval(const fragment_t& fragment, context_t& context,
               message_manager_t& messages) {
    // superstep
    ++context.step;

    // Process received messages sent by other fragment here.
    {
      messages.ParallelProcess<fragment_t, double>(
          thread_num(), fragment,
          [&context](int tid, vertex_t u, const double& msg) {
            // Implement your logic here
          });
    }

    // Compute the degree for each vertex, set the result in context
    auto inner_vertices = fragment.InnerVertices();
    ForEach(inner_vertices.begin(), inner_vertices.end(),
            [&context, &fragment](int tid, vertex_t u) {
              context.result[u] =
                  static_cast<uint64_t>(fragment.GetOutgoingAdjList(u).Size() +
                                        fragment.GetIncomingAdjList(u).Size());
            });
  }
};
```

The `MyApp` class inherits from the `grape::ParallelAppBase`, which provides the basic functionality for implementing a parallel graph algorithm. It also inherits from the `grape::ParallelEngine` and `grape::Communicator` classes, which provide the communication and parallel processing capabilities. The MyApp class defines two static constexpr variables called `message_strategy` and `load_strategy`, these variables specify the message strategy and load strategy used in the computation. For more information please refer to the [libgrape-lite Doc](https://alibaba.github.io/libgrape-lite).

The `PEval` method is used to implement the partial evaluation phase of the computation. In current example, we initialize the communication channels and do nothing else, instead, we put the computing logic into `IncEval` method.

The `IncEval` method is used to implement the incremental evaluation phase of the computation. In this method, we increment the superstep counter in the context, process received messages sent by other fragments, and compute the degree for each vertex in the graph. The `ForEach` method is used to iterate over the inner vertices of the fragment, and the `GetOutgoingAdjList` and `GetIncomingAdjList` methods are used to get the outgoing and incoming adjacency lists of each vertex. The result for each vertex then set in the context.


## Step 3: Package the Algorithm

To make the algorithm runnable on the GraphScope, we need to package it as a `.gar` file. The package should include the above C++ files and a configuration file named `.gs_conf.yaml` with the following content:

```yaml
app:
- algo: my_app
  type: cpp_pie
  class_name: gs::MyApp
  src: my_app.h
  context_type: vertex_data
  compatible_graph:
    - gs::ArrowProjectedFragment
    - gs::DynamicProjectedFragment
    - vineyard::ArrowFragment
```

the codebase structure is as follows:

```
.
├── my_app.h ➝ algorithm logics
└── my_app_context.h ➝ context with auxiliary data for the algorithm
└── .gs_conf.yaml ➝ configuration file 
```

then, we package the algorithm by comand: ` zip -jr 'my_app.gar' '*.h' ''.gs_conf.yaml'`

## Step 4: Run the .gar file on GraphScope

Use the following Python code to run the algorithm on GraphScope.

```python
import graphscope

from graphscope.framework.app import load_app
from graphscope.dataset import load_p2p_network

sess = graphscope.session()
simple_graph = load_p2p_network(sess)._project_to_simple()

my_app = load_app('<path_to_your_gar_resource>')
result = my_app(simple_graph, 10)  # pass 10 as param1 defined in 'MyAppContext.h'

print(result.to_numpy('r'))
```

## GraphScope C++ SDK with Github Template

To help you develop your algorithms more efficiently, we provide a [C++ template library](https://github.com/GraphScope/cpp-template) to help you get started with your algorithm development. It includes examples and best practices for implementing PIE algorithms in C++.

