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

#include "lgraph/db/property.h"
#include "lgraph/db/store_ffi/store_ffi.h"

namespace LGRAPH_NAMESPACE {
namespace db {

Property::~Property() {
  if (handle_ != nullptr) {
    ffi::ReleasePropertyHandle(handle_);
  }
}

PropertyId Property::GetPropertyId() {
  return ffi::GetPropertyId(handle_);
}

Result<int32_t, Error> Property::GetAsInt32() {
  ErrorHandle e = nullptr;
  int32_t val = ffi::GetPropertyAsInt32(handle_, &e);
  if (e == nullptr) {
    return Result<int32_t, Error>(Ok(val));
  }
  return Result<int32_t, Error>(Err(Error(e)));
}

Result<int64_t, Error> Property::GetAsInt64() {
  ErrorHandle e = nullptr;
  int64_t val = ffi::GetPropertyAsInt64(handle_, &e);
  if (e == nullptr) {
    return Result<int64_t, Error>(Ok(val));
  }
  return Result<int64_t, Error>(Err(Error(e)));
}

Result<float, Error> Property::GetAsFloat() {
  ErrorHandle e = nullptr;
  float val = ffi::GetPropertyAsFloat(handle_, &e);
  if (e == nullptr) {
    return Result<float, Error>(Ok(val));
  }
  return Result<float, Error>(Err(Error(e)));
}

Result<double, Error> Property::GetAsDouble() {
  ErrorHandle e = nullptr;
  double val = ffi::GetPropertyAsDouble(handle_, &e);
  if (e == nullptr) {
    return Result<double, Error>(Ok(val));
  }
  return Result<double, Error>(Err(Error(e)));
}

Result<StringSlice, Error> Property::GetAsStr() {
  ErrorHandle e = nullptr;
  StringSlice val = ffi::GetPropertyAsString(handle_, &e);
  if (e == nullptr) {
    return Result<StringSlice, Error>(Ok(val));
  }
  return Result<StringSlice, Error>(Err(Error(e)));
}


PropertyIterator::~PropertyIterator() {
  if (handle_ != nullptr) {
    ffi::ReleasePropertyIteratorHandle(handle_);
  }
}

Result<Property, Error> PropertyIterator::Next() {
  ErrorHandle err_hdl = nullptr;
  PropertyHandle prop_hdl = ffi::PropertyIteratorNext(handle_, &err_hdl);
  if (err_hdl == nullptr) {
    return Result<Property, Error>(Ok(Property(prop_hdl)));
  }
  return Result<Property, Error>(Err(Error(err_hdl)));
}

}
}
