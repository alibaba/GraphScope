#ifndef GRAPHSCOPE_GRAPH_INDEXERS_H_
#define GRAPHSCOPE_GRAPH_INDEXERS_H_

#include "flex/utils/id_indexer.h"
#include "flex/utils/pt_indexer.h"

namespace gs {

#ifndef USE_PTHASH
using IndexerType = LFIndexer<vid_t>;

template <typename KEY_T>
using IndexerBuilderType = IdIndexer<KEY_T, vid_t>;
#else
using IndexerType = PTIndexer<vid_t>;

template <typename KEY_T>
using IndexerBuilderType = PTIndexerBuilder<KEY_T, vid_t>;
#endif

}  // namespace gs

#endif  // GRAPHSCOPE_GRAPH_INDEXERS_H_
