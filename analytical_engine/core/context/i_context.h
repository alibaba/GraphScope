/** Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef ANALYTICAL_ENGINE_CORE_CONTEXT_I_CONTEXT_H_
#define ANALYTICAL_ENGINE_CORE_CONTEXT_I_CONTEXT_H_

#include <cstdint>
#include <iostream>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "boost/leaf/result.hpp"
#include "vineyard/common/util/uuid.h"
#include "vineyard/graph/fragment/property_graph_types.h"

#include "core/error.h"
#include "core/object/gs_object.h"

#ifdef ENABLE_JAVA_SDK
#define CONTEXT_TYPE_JAVA_PIE_PROPERTY "java_pie_property"
#define CONTEXT_TYPE_JAVA_PIE_PROJECTED "java_pie_projected"
#endif

namespace bl = boost::leaf;

namespace arrow {
class Array;
}
namespace grape {
class CommSpec;
class InArchive;
}  // namespace grape
namespace vineyard {
class Client;
}

namespace gs {
class IFragmentWrapper;
class LabeledSelector;
class Selector;
namespace rpc {
class GSParams;
}

/**
 * @brief IContextWrapper is the base class for any kind of ContextWrapper.
 * The ContextWrapper provides a series of methods to serialize the data hold by
 * the context. A specific ContextWrapper class can only be instantiated by the
 * CtxWrapperBuilder.
 */
class IContextWrapper : public GSObject {
 public:
  explicit IContextWrapper(const std::string& id)
      : GSObject(id, ObjectType::kContextWrapper) {}

  virtual std::string context_type() = 0;

  // Return the schema of context, in human readable format.
  // This is consistent with the syntax of selector.
  // For simplicity, only return those parts that cannot be known
  // from client.
  // Those context who need it may override this method.
  virtual std::string schema() { return ""; }

  virtual std::shared_ptr<IFragmentWrapper> fragment_wrapper() = 0;
};

/**
 * @brief A base class for VertexDataContextWrapper.
 */
class IVertexDataContextWrapper : public IContextWrapper {
 public:
  explicit IVertexDataContextWrapper(const std::string& id)
      : IContextWrapper(id) {}

  virtual bl::result<std::unique_ptr<grape::InArchive>> ToNdArray(
      const grape::CommSpec& comm_spec, const Selector& selector,
      const std::pair<std::string, std::string>& range) = 0;

  virtual bl::result<std::unique_ptr<grape::InArchive>> ToDataframe(
      const grape::CommSpec& comm_spec,
      const std::vector<std::pair<std::string, Selector>>& selectors,
      const std::pair<std::string, std::string>& range) = 0;

  virtual bl::result<vineyard::ObjectID> ToVineyardTensor(
      const grape::CommSpec& comm_spec, vineyard::Client& client,
      const Selector& selector,
      const std::pair<std::string, std::string>& range) = 0;

  virtual bl::result<vineyard::ObjectID> ToVineyardDataframe(
      const grape::CommSpec& comm_spec, vineyard::Client& client,
      const std::vector<std::pair<std::string, Selector>>& selectors,
      const std::pair<std::string, std::string>& range) = 0;

  virtual bl::result<
      std::vector<std::pair<std::string, std::shared_ptr<arrow::Array>>>>
  ToArrowArrays(
      const grape::CommSpec& comm_spec,
      const std::vector<std::pair<std::string, Selector>>& selectors) = 0;

  virtual bl::result<std::string> GetContextData(const rpc::GSParams& params) {
    RETURN_GS_ERROR(vineyard::ErrorCode::kInvalidOperationError,
                    "Not implemented operation: GetContextData");
  }
};

/**
 * @brief A base class for LabeledVertexDataContext. The data in the context are
 * group by the label.
 */
class ILabeledVertexDataContextWrapper : public IContextWrapper {
  using label_id_t = vineyard::property_graph_types::LABEL_ID_TYPE;

 public:
  explicit ILabeledVertexDataContextWrapper(const std::string& id)
      : IContextWrapper(id) {}

  virtual bl::result<std::unique_ptr<grape::InArchive>> ToNdArray(
      const grape::CommSpec& comm_spec, const LabeledSelector& selector,
      const std::pair<std::string, std::string>& range) = 0;

  virtual bl::result<std::unique_ptr<grape::InArchive>> ToDataframe(
      const grape::CommSpec& comm_spec,
      const std::vector<std::pair<std::string, LabeledSelector>>& selectors,
      const std::pair<std::string, std::string>& range) = 0;

  virtual bl::result<vineyard::ObjectID> ToVineyardTensor(
      const grape::CommSpec& comm_spec, vineyard::Client& client,
      const LabeledSelector& selector,
      const std::pair<std::string, std::string>& range) = 0;

  virtual bl::result<vineyard::ObjectID> ToVineyardDataframe(
      const grape::CommSpec& comm_spec, vineyard::Client& client,
      const std::vector<std::pair<std::string, LabeledSelector>>& selectors,
      const std::pair<std::string, std::string>& range) = 0;

  /**
   * @brief Output the data in the context to ArrowArray.
   *
   * @param comm_spec
   * @param selectors example: {"col1_label0": "v:label0.id", "col1_label1":
   * "v:label1.id", "col2_result": "r:label0"}
   * @return
   */
  virtual bl::result<std::map<
      label_id_t,
      std::vector<std::pair<std::string, std::shared_ptr<arrow::Array>>>>>
  ToArrowArrays(const grape::CommSpec& comm_spec,
                const std::vector<std::pair<std::string, LabeledSelector>>&
                    selectors) = 0;
};

/**
 * @brief A base class for vertex property context. Differs from
 * ILabeledVertexDataContextWrapper, its columns can be added at runtime.
 */
class IVertexPropertyContextWrapper : public IContextWrapper {
 public:
  explicit IVertexPropertyContextWrapper(const std::string& id)
      : IContextWrapper(id) {}

  virtual bl::result<std::unique_ptr<grape::InArchive>> ToNdArray(
      const grape::CommSpec& comm_spec, const Selector& selector,
      const std::pair<std::string, std::string>& range) = 0;

  virtual bl::result<std::unique_ptr<grape::InArchive>> ToDataframe(
      const grape::CommSpec& comm_spec,
      const std::vector<std::pair<std::string, Selector>>& selectors,
      const std::pair<std::string, std::string>& range) = 0;

  virtual bl::result<vineyard::ObjectID> ToVineyardTensor(
      const grape::CommSpec& comm_spec, vineyard::Client& client,
      const Selector& selector,
      const std::pair<std::string, std::string>& range) = 0;

  virtual bl::result<vineyard::ObjectID> ToVineyardDataframe(
      const grape::CommSpec& comm_spec, vineyard::Client& client,
      const std::vector<std::pair<std::string, Selector>>& selectors,
      const std::pair<std::string, std::string>& range) = 0;

  virtual bl::result<
      std::vector<std::pair<std::string, std::shared_ptr<arrow::Array>>>>
  ToArrowArrays(
      const grape::CommSpec& comm_spec,
      const std::vector<std::pair<std::string, Selector>>& selectors) = 0;
};

/**
 * @brief A base class for LabeledVertexPropertyContext. Compared with
 * ILabeledVertexDataContextWrapper, columns can be added at runtime.
 */
class ILabeledVertexPropertyContextWrapper : public IContextWrapper {
  using label_id_t = vineyard::property_graph_types::LABEL_ID_TYPE;

 public:
  explicit ILabeledVertexPropertyContextWrapper(const std::string& id)
      : IContextWrapper(id) {}

  virtual bl::result<std::unique_ptr<grape::InArchive>> ToNdArray(
      const grape::CommSpec& comm_spec, const LabeledSelector& selector,
      const std::pair<std::string, std::string>& range) = 0;

  virtual bl::result<std::unique_ptr<grape::InArchive>> ToDataframe(
      const grape::CommSpec& comm_spec,
      const std::vector<std::pair<std::string, LabeledSelector>>& selectors,
      const std::pair<std::string, std::string>& range) = 0;

  virtual bl::result<vineyard::ObjectID> ToVineyardTensor(
      const grape::CommSpec& comm_spec, vineyard::Client& client,
      const LabeledSelector& selector,
      const std::pair<std::string, std::string>& range) = 0;

  virtual bl::result<vineyard::ObjectID> ToVineyardDataframe(
      const grape::CommSpec& comm_spec, vineyard::Client& client,
      const std::vector<std::pair<std::string, LabeledSelector>>& selectors,
      const std::pair<std::string, std::string>& range) = 0;

  virtual bl::result<std::map<
      label_id_t,
      std::vector<std::pair<std::string, std::shared_ptr<arrow::Array>>>>>
  ToArrowArrays(const grape::CommSpec& comm_spec,
                const std::vector<std::pair<std::string, LabeledSelector>>&
                    selectors) = 0;
};

#ifdef ENABLE_JAVA_SDK

/**
 * @brief A base class for JavaPropertyDefaultContext. It holds an inner
 * ctxWrapper, and redirect function calls to the inner ctxWrapper.
 */
class IJavaPIEPropertyContextWrapper : public IContextWrapper {
  using label_id_t = vineyard::property_graph_types::LABEL_ID_TYPE;

 public:
  explicit IJavaPIEPropertyContextWrapper(const std::string& id)
      : IContextWrapper(id) {}

  virtual bl::result<std::unique_ptr<grape::InArchive>> ToNdArray(
      const grape::CommSpec& comm_spec, const LabeledSelector& selector,
      const std::pair<std::string, std::string>& range) = 0;

  virtual bl::result<std::unique_ptr<grape::InArchive>> ToDataframe(
      const grape::CommSpec& comm_spec,
      const std::vector<std::pair<std::string, LabeledSelector>>& selectors,
      const std::pair<std::string, std::string>& range) = 0;

  virtual bl::result<vineyard::ObjectID> ToVineyardTensor(
      const grape::CommSpec& comm_spec, vineyard::Client& client,
      const LabeledSelector& selector,
      const std::pair<std::string, std::string>& range) = 0;

  virtual bl::result<vineyard::ObjectID> ToVineyardDataframe(
      const grape::CommSpec& comm_spec, vineyard::Client& client,
      const std::vector<std::pair<std::string, LabeledSelector>>& selectors,
      const std::pair<std::string, std::string>& range) = 0;

  virtual bl::result<std::map<
      label_id_t,
      std::vector<std::pair<std::string, std::shared_ptr<arrow::Array>>>>>
  ToArrowArrays(const grape::CommSpec& comm_spec,
                const std::vector<std::pair<std::string, LabeledSelector>>&
                    selectors) = 0;
};

/**
 * @brief A base class for JavaProjectedDefaultContext. It holds an inner
 * ctxWrapper,and redirect function calls to the inner ctxWrapper.
 */
class IJavaPIEProjectedContextWrapper : public IContextWrapper {
 public:
  explicit IJavaPIEProjectedContextWrapper(const std::string& id)
      : IContextWrapper(id) {}

  virtual bl::result<std::unique_ptr<grape::InArchive>> ToNdArray(
      const grape::CommSpec& comm_spec, const Selector& selector,
      const std::pair<std::string, std::string>& range) = 0;

  virtual bl::result<std::unique_ptr<grape::InArchive>> ToDataframe(
      const grape::CommSpec& comm_spec,
      const std::vector<std::pair<std::string, Selector>>& selectors,
      const std::pair<std::string, std::string>& range) = 0;

  virtual bl::result<vineyard::ObjectID> ToVineyardTensor(
      const grape::CommSpec& comm_spec, vineyard::Client& client,
      const Selector& selector,
      const std::pair<std::string, std::string>& range) = 0;

  virtual bl::result<vineyard::ObjectID> ToVineyardDataframe(
      const grape::CommSpec& comm_spec, vineyard::Client& client,
      const std::vector<std::pair<std::string, Selector>>& selectors,
      const std::pair<std::string, std::string>& range) = 0;

  virtual bl::result<
      std::vector<std::pair<std::string, std::shared_ptr<arrow::Array>>>>
  ToArrowArrays(
      const grape::CommSpec& comm_spec,
      const std::vector<std::pair<std::string, Selector>>& selectors) = 0;
};
#endif

/**
 * @brief An abstract ContextWrapper for the data not assigned to vertex/edges.
 */
class ITensorContextWrapper : public IContextWrapper {
 public:
  explicit ITensorContextWrapper(const std::string& id) : IContextWrapper(id) {}

  virtual bl::result<std::unique_ptr<grape::InArchive>> ToNdArray(
      const grape::CommSpec& comm_spec, uint32_t axis) = 0;

  virtual bl::result<std::unique_ptr<grape::InArchive>> ToDataframe(
      const grape::CommSpec& comm_spec) = 0;

  virtual bl::result<vineyard::ObjectID> ToVineyardTensor(
      const grape::CommSpec& comm_spec, vineyard::Client& client,
      uint32_t axis) = 0;

  virtual bl::result<vineyard::ObjectID> ToVineyardDataframe(
      const grape::CommSpec& comm_spec, vineyard::Client& client) = 0;

  virtual bl::result<
      std::vector<std::pair<std::string, std::shared_ptr<arrow::Array>>>>
  ToArrowArrays(
      const grape::CommSpec& comm_spec,
      const std::vector<std::pair<std::string, Selector>>& selectors) = 0;
};

}  // namespace gs

#endif  // ANALYTICAL_ENGINE_CORE_CONTEXT_I_CONTEXT_H_
