# GIE Codegen

This Project contains the runtime code generation module for `GraphScope` Interactive Engine.
Given a cypher query, GIE compiler compiles it into a physical plan consisting of GAIA IRs,
and codegen module is able to generating native code to run the physical plan on GIE engine.

The generated C++ code contains  4 parts
- Headers
- Expression classes
- Query class
- extern C APIs.
```c++
//0. headers
#include "flex/engines/hqps_db/core/sync_engine.h"
#include "flex/engines/hqps_db/app/cypher_app_base.h"
#include "flex/storages/mutable_csr/mutable_csr_interface.h"

namespace gs {

//1. Expressions
struct Expression0{
};

struct Expression1{
};

//2. Query class

class Query0 : public AppBase {
 public:
  bool Query(Decoder& input, Encoder& output) override {
    //...
  }
};
}  // namespace gs

// 3. Create and delete handler for query
extern "C" {
void* CreateApp(gs::GraphDBSession& db) {
  Query0* app = new Query0(db);
  return static_cast<void*>(app);
}

void DeleteApp(void* app) {
  if (app){
    Query0* casted = static_cast<Query0*>(app);
    delete casted;
  }
}
}
```

