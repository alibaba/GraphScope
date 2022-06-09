/** Copyright 2020 Alibaba Group Holding Limited.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

#include <gtest/gtest.h>

#include "../graph_schema.h"
#include "../graph_builder_ffi.h"

using std::string;

namespace vineyard {

class GraphSchemaTest : public ::testing::Test {
 protected:
  void SetUp() override {
    schema_ = create_schema_builder();
  }

  void TearDown() override {
    free_schema(schema_);
  }

  Schema schema_;
};

TEST_F(GraphSchemaTest, BuildVertexTypeAndProperty) {
  const char* person_label = "person";
  LabelId person_label_id = 1;

  // since commit "Add a general project operator (#211)", property id
  // is expected to be ordinal of the property and used as index into
  // valid_properties flags, so must start at 0
  PropertyId id_prop_id = 0;
  const char* id_prop = "id";

  PropertyId name_prop_id = 1;
  const char* name_prop = "name";

  auto personType = build_vertex_type(schema_, person_label_id, person_label);
  ASSERT_TRUE(personType != nullptr);
  build_vertex_property(personType, id_prop_id, id_prop, INT);
  build_vertex_property(personType, name_prop_id, name_prop, STRING);
  finish_build_vertex(personType);
  finish_build_schema(schema_);

  LabelId person_id_in_schema = -1;
  ASSERT_EQ(0, get_label_id(schema_, person_label, &person_id_in_schema));
  EXPECT_EQ(person_id_in_schema, person_label_id);

  const char* person_label_in_schema = nullptr;
  ASSERT_EQ(0,
            get_label_name(schema_, person_label_id, &person_label_in_schema));
  EXPECT_STREQ(person_label_in_schema, person_label);
  free_string(const_cast<char*>(person_label_in_schema));

  PropertyId id_prop_id_in_schema = -1;
  ASSERT_EQ(0,
            get_property_id(schema_, id_prop, &id_prop_id_in_schema));
  EXPECT_EQ(id_prop_id_in_schema, id_prop_id);

  const char* id_prop_in_schema = nullptr;
  ASSERT_EQ(0,
            get_property_name(schema_, id_prop_id, &id_prop_in_schema));
  EXPECT_STREQ(id_prop_in_schema, id_prop);
  free_string(const_cast<char*>(id_prop_in_schema));

  ::PropertyType id_prop_type_in_schema = INVALID;
  ASSERT_EQ(0,
            get_property_type(schema_, person_label_id,
                              id_prop_id, &id_prop_type_in_schema));
  EXPECT_EQ(id_prop_type_in_schema, INT);

  PropertyId name_prop_id_in_schema = -1;
  ASSERT_EQ(0,
            get_property_id(schema_, name_prop, &name_prop_id_in_schema));
  EXPECT_EQ(name_prop_id_in_schema, name_prop_id);

  const char* name_prop_in_schema = nullptr;
  ASSERT_EQ(0,
            get_property_name(schema_, name_prop_id, &name_prop_in_schema));
  EXPECT_STREQ(name_prop_in_schema, name_prop);
  free_string(const_cast<char*>(name_prop_in_schema));

  ::PropertyType name_prop_type_in_schema = INVALID;
  ASSERT_EQ(0,
            get_property_type(schema_, person_label_id,
                              name_prop_id, &name_prop_type_in_schema));
  EXPECT_EQ(name_prop_type_in_schema, STRING);
}


}
