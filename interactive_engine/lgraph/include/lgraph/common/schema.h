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

#pragma once

#include <fstream>
#include "lgraph/common/check.h"
#include "lgraph/common/types.h"
#include "lgraph/proto/model.pb.h"

namespace LGRAPH_NAMESPACE {

class PropertyDef {
public:
  PropertyDef(PropertyId prop_id, const std::string &prop_name, DataType data_type,
              const std::string &default_value_bytes, const std::string &comment);
  ~PropertyDef() = default;

  PropertyDef(const PropertyDef &) = default;
  PropertyDef &operator=(const PropertyDef &) = default;
  PropertyDef(PropertyDef &&) = default;
  PropertyDef &operator=(PropertyDef &&) = default;

  PropertyId GetPropId() const {
    return prop_id_;
  }

  const std::string &GetPropName() const {
    return prop_name_;
  }

  DataType GetDataType() const {
    return data_type_;
  }

  const std::string &GetDefaultValueBytes() const {
    return default_value_bytes_;
  }

  const std::string &GetComment() const {
    return comment_;
  }

private:
  PropertyId prop_id_;
  std::string prop_name_;
  DataType data_type_;
  std::string default_value_bytes_;
  std::string comment_;
};

class TypeDef {
public:
  TypeDef(LabelId label_id, const std::string &label_name, EntityType entity_type,
          std::vector<PropertyId> &&property_ids);
  ~TypeDef() = default;

  TypeDef(const TypeDef &) = default;
  TypeDef &operator=(const TypeDef &) = default;
  TypeDef(TypeDef &&) = default;
  TypeDef &operator=(TypeDef &&) = default;

  static TypeDef FromProto(const TypeDefPb &proto);

  LabelId GetLabelId() const {
    return label_id_;
  }

  const std::string &GetLabelName() const {
    return label_name_;
  }

  EntityType GetEntityType() const {
    return entity_type_;
  }

  const std::vector<PropertyId> &GetPropIds() const {
    return property_ids_;
  }

private:
  LabelId label_id_;
  std::string label_name_;
  EntityType entity_type_;
  std::vector<PropertyId> property_ids_;
};

class Schema {
public:
  Schema(std::unordered_map<LabelId, TypeDef> &&label_to_typedefs, std::vector<EdgeRelation> &&edge_relations,
         std::unordered_map<PropertyId, PropertyDef> &&property_defs);
  ~Schema() = default;

  Schema(const Schema &) = default;
  Schema &operator=(const Schema &) = default;
  Schema(Schema &&) = default;
  Schema &operator=(Schema &&) = default;

  static Schema FromProtoFile(const char *schema_proto_bytes_file);
  static Schema FromProtoBytes(const void *proto_data, size_t size);
  static Schema FromProto(const GraphDefPb &proto);

  const TypeDef &GetTypeDef(LabelId label_id) const {
    return label_to_typedefs_.at(label_id);
  }

  const std::unordered_map<LabelId, TypeDef> &GetTypeDefMap() const {
    return label_to_typedefs_;
  }

  const std::vector<EdgeRelation> &GetEdgeRelations() const {
    return edge_relations_;
  }

  const PropertyDef &GetPropDef(PropertyId property_id) const {
    return property_defs_.at(property_id);
  }

  const std::unordered_map<PropertyId, PropertyDef> &GetPropDefMap() const {
    return property_defs_;
  }

private:
  std::unordered_map<LabelId, TypeDef> label_to_typedefs_;
  std::vector<EdgeRelation> edge_relations_;
  std::unordered_map<PropertyId, PropertyDef> property_defs_;

  static void AddPropertyDefs(const TypeDefPb &proto, std::unordered_map<PropertyId, PropertyDef> *property_defs);
};

inline PropertyDef::PropertyDef(PropertyId prop_id, const std::string &prop_name, DataType data_type,
                                const std::string &default_value_bytes, const std::string &comment)
    : prop_id_(prop_id), prop_name_(prop_name), data_type_(data_type), default_value_bytes_(default_value_bytes),
      comment_(comment) {}

inline TypeDef::TypeDef(LabelId label_id, const std::string &label_name, EntityType entity_type,
                        std::vector<PropertyId> &&property_ids)
    : label_id_(label_id), label_name_(label_name), entity_type_(entity_type), property_ids_(std::move(property_ids)) {}

inline TypeDef TypeDef::FromProto(const TypeDefPb &proto) {
  std::vector<PropertyId> property_ids;
  auto &pb_prop_defs = proto.props();
  property_ids.reserve(pb_prop_defs.size());
  for (auto &prop_def : pb_prop_defs) {
    property_ids.push_back(static_cast<PropertyId>(prop_def.id()));
  }
  return {static_cast<LabelId>(proto.labelid().id()), proto.label(),
          static_cast<EntityType>(proto.typeenum()), std::move(property_ids)};
}

inline Schema::Schema(std::unordered_map<LabelId, TypeDef> &&label_to_typedefs,
               std::vector<EdgeRelation> &&edge_relations,
               std::unordered_map<PropertyId, PropertyDef> &&property_defs)
    : label_to_typedefs_(std::move(label_to_typedefs)), edge_relations_(std::move(edge_relations)),
      property_defs_(property_defs) {}

inline Schema Schema::FromProtoFile(const char *schema_proto_bytes_file) {
  std::ifstream infile(schema_proto_bytes_file);
  std::vector<char> buffer;
  infile.seekg(0, infile.end);
  long length = infile.tellg();
  Check(length > 0, "Loading empty schema file!");
  buffer.resize(length);
  infile.seekg(0, infile.beg);
  infile.read(&buffer[0], length);
  infile.close();
  return Schema::FromProtoBytes(buffer.data(), buffer.size());
}
inline Schema Schema::FromProtoBytes(const void *proto_data, size_t size) {
  GraphDefPb pb;
  Check(pb.ParseFromArray(proto_data, static_cast<int>(size)), "Parse GraphDefPb Failed!");
  return Schema::FromProto(pb);
}

inline Schema Schema::FromProto(const GraphDefPb &proto) {
  std::unordered_map<LabelId, TypeDef> label_to_typedefs;
  std::vector<EdgeRelation> edge_relations;
  std::unordered_map<PropertyId, PropertyDef> property_defs;

  auto &pb_typedefs = proto.typedefs();
  label_to_typedefs.reserve(pb_typedefs.size());
  for (auto &def : pb_typedefs) {
    AddPropertyDefs(def, &property_defs);
    label_to_typedefs.emplace(static_cast<LabelId>(def.labelid().id()), std::move(TypeDef::FromProto(def)));
  }
  Check(proto.propertynametoid_size() == property_defs.size(), "schema error in property defs!");
  auto &pb_edge_kinds = proto.edgekinds();
  edge_relations.reserve(pb_edge_kinds.size());
  for (auto &kind : pb_edge_kinds) {
    edge_relations.emplace_back(static_cast<LabelId>(kind.edgelabelid().id()),
                                static_cast<LabelId>(kind.srcvertexlabelid().id()),
                                static_cast<LabelId>(kind.dstvertexlabelid().id()));
  }
  return {std::move(label_to_typedefs), std::move(edge_relations), std::move(property_defs)};
}

inline void Schema::AddPropertyDefs(const TypeDefPb &proto, std::unordered_map<PropertyId, PropertyDef> *property_defs) {
  for (auto &prop_def : proto.props()) {
    auto prop_id = static_cast<PropertyId>(prop_def.id());
    if (property_defs->count(prop_id) == 0) {
      property_defs->emplace(prop_id, std::move(
          PropertyDef{prop_id, prop_def.name(), static_cast<DataType>(prop_def.datatype()),
                      prop_def.defaultvalue().val(), prop_def.comment()}));
    }
  }
}

}
