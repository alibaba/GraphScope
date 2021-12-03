/**
 * Copyright 2021 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "lgraph/db/edge.h"
#include "lgraph/db/store_ffi/store_ffi.h"

namespace LGRAPH_NAMESPACE {
namespace db {

Edge::~Edge() {
  if (handle_ != nullptr) {
    ffi::ReleaseEdgeHandle(handle_);
  }
}

EdgeId Edge::GetEdgeId() {
  return ffi::GetEdgeId(handle_);
}

EdgeRelation Edge::GetEdgeRelation() {
  return ffi::GetEdgeRelation(handle_);
}

Property Edge::GetPropertyBy(PropertyId prop_id) {
  return Property(ffi::GetEdgeProperty(handle_, prop_id));
}

PropertyIterator Edge::GetPropertyIterator() {
  return PropertyIterator(ffi::GetEdgePropertyIterator(handle_));
}

EdgeIterator::~EdgeIterator() {
  if (handle_ != nullptr) {
    ffi::ReleaseEdgeIteratorHandle(handle_);
  }
}

Result<Edge, Error> EdgeIterator::Next() {
  ErrorHandle err_hdl = nullptr;
  EdgeHandle edge_hdl = ffi::EdgeIteratorNext(handle_, &err_hdl);
  if (err_hdl == nullptr) {
    return Result<Edge, Error>(Ok(Edge(edge_hdl)));
  }
  return Result<Edge, Error>(Err(Error(err_hdl)));
}

}
}
