// #include "ligra.h"
// #include <vector>
// #include <algorithm>
// #include <unordered_map>
// #include <atomic>

// // 定义标签更新结构体
// template <class vertex>
// struct LPA_Vertex_F {
//   int* labels;      // 当前标签数组
//   int* new_labels;  // 新标签数组
//   vertex* V;        // 图的顶点数组

//   // 构造函数，初始化成员变量
//   LPA_Vertex_F(int* _labels, int* _new_labels, vertex* _V) : 
//     labels(_labels), new_labels(_new_labels), V(_V) {}

//   // 重载括号操作符，用于 vertexMap
//   inline bool operator()(uintE i) {
//     // 使用临时数组存储邻居标签
//     std::vector<int> neighbor_labels;
//     neighbor_labels.reserve(V[i].getOutDegree());

//     // 收集所有邻居的标签
//     for(uintT j = 0; j < V[i].getOutDegree(); j++) {
//       uintE neighbor = V[i].getOutNeighbors()[j];
//       neighbor_labels.push_back(labels[neighbor]);
//     }

//     if (neighbor_labels.empty()) {
//       // 如果没有邻居，保持原标签
//       new_labels[i] = labels[i];
//       return false;
//     }

//     // 统计标签频率
//     std::unordered_map<int, int> label_count;
//     for(auto label : neighbor_labels) {
//       label_count[label]++;
//     }

//     // 找到频率最高的标签
//     int max_count = -1;
//     int best_label = labels[i]; // 默认保留原标签
//     for(auto &p : label_count) {
//       if(p.second > max_count || (p.second == max_count && p.first < best_label)) {
//         max_count = p.second;
//         best_label = p.first;
//       }
//     }

//     // 更新标签
//     new_labels[i] = best_label;

//     // 返回标签是否发生变化
//     return (labels[i] != new_labels[i]);
//   }
// };

// // 定义标签重置结构体
// // struct LPA_Vertex_Reset {
// //   int* new_labels;

// //   // 构造函数，初始化成员变量
// //   LPA_Vertex_Reset(int* _new_labels) : new_labels(_new_labels) {}

// //   // 重载括号操作符，用于 vertexMap
// //   inline bool operator () (uintE i) {
// //     new_labels[i] = 0; // 重置新标签数组（可选，根据需要调整）
// //     return 1;
// //   }
// // };

// // 主计算函数
// template <class vertex>
// void Compute(graph<vertex>& GA, commandLine P) {
//   long maxIters = P.getOptionLongValue("-maxiters", 100);
//   const int n = GA.n;

//   // 初始化标签数组
//   int* labels = newA(int, n);
//   int* new_labels = newA(int, n);
  
//   // 初始化标签，每个节点的初始标签为其自身 ID
//   parallel_for(long i = 0; i < n; i++) {
//     labels[i] = i;
//     new_labels[i] = i;
//   }

//   bool changed = true;
//   long iter = 0;

//   while (changed && iter < maxIters) {
//     iter++;
//     changed = false;

//     // 定义 Frontier，这里所有节点都参与更新
//     bool* frontier = newA(bool, n);
//     parallel_for(long i = 0; i < n; i++) frontier[i] = true;
//     vertexSubset Frontier(n, n, frontier);

//     // 使用 vertexMap 更新标签
//     vertexMap(Frontier, LPA_Vertex_F<vertex>(labels, new_labels, GA.V));

//     // 删除 Frontier
//     Frontier.del();

//     // 检查是否有标签发生变化
//     // 使用一个原子变量来检测是否有变化
//     std::atomic<bool> any_changed(false);
//     parallel_for(long i = 0; i < n; i++) {
//       if(labels[i] != new_labels[i]) {
//         any_changed.store(true, std::memory_order_relaxed);
//         labels[i] = new_labels[i];
//       }
//     }
//     changed = any_changed.load();

//     // 重置 new_labels 数组
//     // vertexSubset ResetFrontier(n, n, nullptr);
//     // parallel_for(long i = 0; i < n; i++) {
//     //   new_labels[i] = 0;
//     // }

//     std::cout << "Iteration " << iter << " completed." << std::endl;
//   }

//   std::cout << "LPA completed in " << iter << " iterations." << std::endl;

//   // 输出每个节点的标签（社区）
//   for(long i = 0; i < n; i++) {
//     std::cout << "Node " << i << " is in community " << labels[i] << std::endl;
//   }

//   // 释放内存
//   free(labels);
//   free(new_labels);
// }


#include "ligra.h"
#include "math.h"
#include <vector>
#include <unordered_map>


template <class vertex>
struct PR_F {
  double* p_curr, *p_next;
  
  PR_F(double* _p_curr, double* _p_next) : 
    p_curr(_p_curr), p_next(_p_next) {}
  inline bool update(uintE s, uintE d){ //update function applies PageRank equation
    p_next[d] = p_curr[s];
    return 1;
  }
  inline bool updateAtomic (uintE s, uintE d) { //atomic Update
    // writeAdd(&p_next[d],p_curr[s]);
    // return (CAS(&p_next[d],UINT_E_MAX,p_curr[s]));
    return 1;
  }
  inline bool cond (intT d) { return cond_true(d); }};

//vertex map function to update its p value according to PageRank equation
template <class vertex>
struct PR_Vertex_F {
  double* p_curr;
  double* p_next;
  vertex* V;
  PR_Vertex_F(double* _p_curr, double* _p_next, vertex* _V) :
    p_curr(_p_curr), p_next(_p_next), V(_V){}
  inline bool operator () (uintE i) {
    std::vector<int> neighbor_labels;
    neighbor_labels.reserve(V[i].getOutDegree());

    // 收集所有邻居的标签
    for(uintT j = 0; j < V[i].getOutDegree(); j++) {
      uintE neighbor = V[i].getOutNeighbors()[j];
      neighbor_labels.push_back(p_curr[neighbor]);
    }

    if (neighbor_labels.empty()) {
      // 如果没有邻居，保持原标签
      p_next[i] = p_curr[i];
      return false;
    }

    // 统计标签频率
    std::unordered_map<int, int> label_count;
    for(auto label : neighbor_labels) {
      label_count[label]++;
    }

    // 找到频率最高的标签
    int max_count = -1;
    int best_label = p_curr[i]; // 默认保留原标签
    for(auto &p : label_count) {
      if(p.second > max_count || (p.second == max_count && p.first < best_label)) {
        max_count = p.second;
        best_label = p.first;
      }
    }

    // 更新标签
    p_next[i] = best_label;

    
    return 1;
  }
};

//resets p
struct PR_Vertex_Reset {
  double* p_curr;
  PR_Vertex_Reset(double* _p_curr) :
    p_curr(_p_curr) {}
  inline bool operator () (uintE i) {
    p_curr[i] = 0.0;
    return 1;
  }
}; 

template <class vertex>
void Compute(graph<vertex>& GA, commandLine P) {
  setWorkers(32);
  long maxIters = P.getOptionLongValue("-maxiters",100);
  const intE n = GA.n;
  const double damping = 0.85, epsilon = 0.0000001;
  
  double one_over_n = 1/(double)n;
  double* p_curr = newA(double,n);
  {parallel_for(long i=0;i<n;i++) p_curr[i] = i;}
  double* p_next = newA(double,n);
  {parallel_for(long i=0;i<n;i++) p_next[i] = -1;} //-1 if unchanged
  bool* frontier = newA(bool,n);
  {parallel_for(long i=0;i<n;i++) frontier[i] = 1;}

  vertexSubset Frontier(n,n,frontier);
  
  long iter = 0;
  while(iter++ < maxIters) {
    edgeMap(GA,Frontier,PR_F<vertex>(p_curr,p_next),0, no_output);
    vertexMap(Frontier,PR_Vertex_F<vertex>(p_curr,p_next,GA.V));
    //compute L1-norm between p_curr and p_next
    // {parallel_for(long i=0;i<n;i++) {
    //   p_curr[i] = fabs(p_curr[i]-p_next[i]);
    //   }}
    // double L1_norm = sequence::plusReduce(p_curr,n);
    // if(L1_norm < epsilon) break;
    //reset p_curr
    // vertexMap(Frontier,PR_Vertex_Reset(p_curr));
    swap(p_curr,p_next);
  }
  Frontier.del(); free(p_curr); free(p_next); 
}

