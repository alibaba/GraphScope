
#include "flex/engines/hqps/ds/collection.h"
#include "flex/engines/hqps/ds/multi_vertex_set/row_vertex_set.h"

int main(int argc, char** argv) {
  {
    gs::CollectionBuilder<int32_t> builder;
    builder.Insert(1);
    builder.Insert(2);
    auto res = builder.Build();
    CHECK(res.Size() == 2);
  }

  {
    gs::RowVertexSetBuilder<std::string, int32_t, grape::EmptyType> builder(
        "0");
    builder.Insert(1);
    builder.Insert(2);
    auto res = builder.Build();
    CHECK(res.Size() == 2);
  }
}