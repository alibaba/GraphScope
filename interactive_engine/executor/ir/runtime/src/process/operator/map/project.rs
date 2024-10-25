//
//! Copyright 2021 Alibaba Group Holding Limited.
//!
//! Licensed under the Apache License, Version 2.0 (the "License");
//! you may not use this file except in compliance with the License.
//! You may obtain a copy of the License at
//!
//! http://www.apache.org/licenses/LICENSE-2.0
//!
//! Unless required by applicable law or agreed to in writing, software
//! distributed under the License is distributed on an "AS IS" BASIS,
//! WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//! See the License for the specific language governing permissions and
//! limitations under the License.

use std::collections::BTreeMap;
use std::convert::{TryFrom, TryInto};

use common_pb::path_concat::Endpoint;
use dyn_type::Object;
use graph_proxy::apis::PropKey;
use graph_proxy::utils::expr::eval::{Evaluate, Evaluator};
use ir_common::error::ParsePbError;
use ir_common::generated::common as common_pb;
use ir_common::generated::physical as pb;
use ir_common::KeyId;
use pegasus::api::function::{FilterMapFunction, FnResult};

use crate::error::FnExecError;
use crate::error::{FnExecResult, FnGenResult};
use crate::process::entry::DynEntry;
use crate::process::entry::Entry;
use crate::process::entry::PairEntry;
use crate::process::entry::{CollectionEntry, EntryType};
use crate::process::operator::map::FilterMapFuncGen;
use crate::process::operator::TagKey;
use crate::process::record::Record;

#[derive(Debug)]
enum Projector {
    ExprProjector(Evaluator),
    GraphElementProjector(TagKey),
    /// VecProjector will output a collection entry, which is a collection of projected graph elements (computed via TagKey).
    VecProjector(Vec<TagKey>),
    /// MapProjector will output a collection entry, which is a collection of key-value pairs. The key is a Object (preserve the user-given key), and the value is a projected graph element (computed via TagKey).
    /// Besides, MapProjector supports nested map.
    MapProjector(VariableKeyValues),
    /// A simple concatenation of two paths.
    PathConcatProjector((TagKey, Endpoint), (TagKey, Endpoint)),
    /// PathValueProjector will output a collection of projected properties of the element in the path.
    PathValueProjector(PathTagKeyValues),
}

/// Project entries with specified tags or further their properties.
/// Notice that when projecting a single column, if the result is a None-Entry,
/// Caused by either the given `tag` or the required properties do not exist, the record will be filtered.
#[derive(Debug)]
struct ProjectOperator {
    is_append: bool,
    projected_columns: Vec<(Projector, Option<KeyId>)>,
}

#[derive(Debug)]
enum VariableValue {
    Value(TagKey),
    PathFunc(PathTagKeyValues),
    Nest(VariableKeyValues),
}

#[derive(Debug)]
struct VariableKeyValue {
    key: Object,
    value: VariableValue,
}

#[derive(Debug)]
struct VariableKeyValues {
    key_vals: Vec<VariableKeyValue>,
}

impl VariableKeyValues {
    fn exec_projector(&self, input: &Record) -> FnExecResult<DynEntry> {
        let mut map_collection = Vec::with_capacity(self.key_vals.len());
        for kv in self.key_vals.iter() {
            let key = kv.key.clone();
            let value = match &kv.value {
                VariableValue::Value(tag_key) => tag_key.get_arc_entry(input)?,
                VariableValue::Nest(nest) => nest.exec_projector(input)?,
                VariableValue::PathFunc(path_func) => path_func.exec_projector(input)?,
            };
            map_collection.push(PairEntry::new(key.into(), value).into());
        }
        Ok(DynEntry::new(CollectionEntry { inner: map_collection }))
    }
}

#[derive(Debug)]
enum PathKey {
    Property(PropKey),
    Vec(Vec<PropKey>),
    Map(Vec<(Object, PropKey)>),
}

impl PathKey {
    // get the properties of the elements in path according to the path key.
    // The properties are returned as a Object::Vector, e.g., project a.name where a is a path, the result is a vector of names.
    fn get_key(&self, entry: &DynEntry) -> FnExecResult<Object> {
        match self {
            PathKey::Property(prop_key) => {
                let props = self.get_path_props(entry, prop_key)?;
                Ok(Object::Vector(props))
            }
            PathKey::Vec(vec) => {
                let prop_num = vec.len();
                if prop_num == 0 {
                    warn!("Empty Path Properties in PathKey::Vec");
                    return Ok(Object::Vector(vec![]));
                }
                let prob_props = self.get_path_props(entry, &vec[0])?;
                let mut prop_collection: Vec<Vec<Object>> = prob_props
                    .into_iter()
                    .map(|prop| {
                        let mut inner_vec = Vec::with_capacity(prop_num);
                        inner_vec.push(prop);
                        inner_vec.extend((1..prop_num).map(|_| Object::None));
                        inner_vec
                    })
                    .collect();
                for (prop_idx, prop_key) in vec.into_iter().enumerate().skip(1) {
                    let props = self.get_path_props(entry, &prop_key)?;
                    for (path_idx, prop) in props.into_iter().enumerate() {
                        prop_collection[path_idx][prop_idx] = prop;
                    }
                }
                Ok(Object::Vector(
                    prop_collection
                        .into_iter()
                        .map(|vec| Object::Vector(vec))
                        .collect(),
                ))
            }
            PathKey::Map(map) => {
                let prop_num = map.len();
                if prop_num == 0 {
                    warn!("Empty Path Properties in PathKey::Vec");
                    return Ok(Object::Vector(vec![]));
                }
                let prob_key = &map[0].0;
                let prob_props = self.get_path_props(entry, &map[0].1)?;
                let mut prop_collection = Vec::with_capacity(prob_props.len());
                for prop_val in prob_props.into_iter() {
                    let mut btree_map = BTreeMap::new();
                    btree_map.insert(prob_key.clone(), prop_val);
                    prop_collection.push(btree_map);
                }
                for (key_name, prop_key) in map.into_iter().skip(1) {
                    let props = self.get_path_props(entry, &prop_key)?;
                    for (path_idx, prop) in props.into_iter().enumerate() {
                        prop_collection[path_idx].insert(key_name.clone(), prop);
                    }
                }
                Ok(Object::Vector(
                    prop_collection
                        .into_iter()
                        .map(|map| Object::KV(map))
                        .collect(),
                ))
            }
        }
    }

    fn get_path_props(&self, entry: &DynEntry, prop_key: &PropKey) -> FnExecResult<Vec<Object>> {
        if PropKey::Id.eq(prop_key) {
            Ok(entry
                .as_graph_path()
                .ok_or_else(|| {
                    FnExecError::unexpected_data_error("Apply PathKey::Property on a non-Path entry")
                })?
                .get_elem_ids()
                .into_iter()
                .map(|id| id.into())
                .collect())
        } else if PropKey::Label.eq(prop_key) {
            Ok(entry
                .as_graph_path()
                .ok_or_else(|| {
                    FnExecError::unexpected_data_error("Apply PathKey::Property on a non-Path entry")
                })?
                .get_elem_labels()
                .into_iter()
                .map(|label| {
                    label
                        .map(|label| label.into())
                        .unwrap_or(Object::None)
                })
                .collect())
        } else {
            Ok(prop_key
                .get_key(entry)?
                .take_vector()
                .map_err(|e| FnExecError::ExprEvalError(e.into()))?)
        }
    }
}

#[derive(Debug)]
struct PathTagKeyValues {
    tag: Option<KeyId>,
    val: PathKey,
    // TODO: support function options.
    // Currently, if the path is ALLV, return the properties of vertices; if the path is ALLVE, return the properties of both vertices and edges.
    _opt: common_pb::path_function::FuncOpt,
}

impl PathTagKeyValues {
    fn exec_projector(&self, input: &Record) -> FnExecResult<DynEntry> {
        if let Some(entry) = input.get(self.tag) {
            if EntryType::Path != entry.get_type() {
                Err(FnExecError::unexpected_data_error("Apply PathTagKeyValues on a non-Path entry"))
            } else {
                let projected_properties_obj = self.val.get_key(&entry)?;
                Ok(projected_properties_obj.into())
            }
        } else {
            Ok(DynEntry::new(Object::Vector(vec![])))
        }
    }
}

// TODO:
// 1. Currently, the behavior of filtering none-entry is identical to e.g., `g.V().values('name')`,
//    but differs to `g.V().valueMap('name')`, which will output the none-entry.
//    To support both cases, we may further need a flag to identify whether to filter or not.
//    BTW, if it is necessary to output none-entry,
//    we may need to further distinguish the cases of none-exist tags (filtering case) and none-exist properties (output none-entry).
// 2. When projecting multiple columns, even all projected columns are none-entry, the record won't be filtered for now.
//    This seems ambiguous. But multi-column project always appears in the end of the query. Can modify this logic if necessary.
fn exec_projector(input: &Record, projector: &Projector) -> FnExecResult<DynEntry> {
    let entry = match projector {
        Projector::ExprProjector(evaluator) => {
            let projected_result = evaluator.eval::<DynEntry, Record>(Some(&input))?;
            DynEntry::new(projected_result)
        }
        Projector::GraphElementProjector(tag_key) => tag_key.get_arc_entry(input)?,
        Projector::VecProjector(vec) => {
            let mut collection = Vec::with_capacity(vec.len());
            for tag_key in vec.iter() {
                let entry = tag_key.get_arc_entry(input)?;
                collection.push(entry);
            }
            DynEntry::new(CollectionEntry { inner: collection })
        }
        Projector::MapProjector(map) => map.exec_projector(input)?,
        Projector::PathConcatProjector((left, left_endpoint), (right, right_endpoint)) => {
            let mut left_path = left
                .get_arc_entry(input)?
                .as_graph_path()
                .ok_or_else(|| FnExecError::unsupported_error("Left entry is not Path in PathConcat"))?
                .clone();
            let mut right_path = right
                .get_arc_entry(input)?
                .as_graph_path()
                .ok_or_else(|| FnExecError::unsupported_error("Right entry is not Path in PathConcat"))?
                .clone();

            let mut invalid = false;
            let mut concat_success = false;
            match (left_endpoint, right_endpoint) {
                // e.g., concat [3,2,1], [3,4,5] => [1,2,3,4,5]
                (Endpoint::Start, Endpoint::Start) => {
                    if left_path.get_path_start() != right_path.get_path_start() {
                        invalid = true;
                    } else {
                        left_path.reverse();
                        left_path.pop();
                        concat_success = left_path.append_path(right_path);
                    }
                }
                (Endpoint::Start, Endpoint::End) => {
                    // e.g., concat [3,2,1], [5,4,3] => [1,2,3,4,5]
                    if left_path.get_path_start().is_none()
                        || (left_path.get_path_start().unwrap() != right_path.get_path_end())
                    {
                        invalid = true;
                    } else {
                        left_path.reverse();
                        left_path.pop();
                        right_path.reverse();
                        concat_success = left_path.append_path(right_path);
                    }
                }
                (Endpoint::End, Endpoint::Start) => {
                    // e.g., concat [1,2,3], [3,4,5] => [1,2,3,4,5]
                    if right_path.get_path_start().is_none()
                        || (right_path.get_path_start().unwrap() != left_path.get_path_end())
                    {
                        invalid = true;
                    } else {
                        left_path.pop();
                        concat_success = left_path.append_path(right_path);
                    }
                }
                (Endpoint::End, Endpoint::End) => {
                    // e.g., concat [1,2,3], [5,4,3] => [1,2,3,4,5]
                    if left_path.get_path_end() != right_path.get_path_end() {
                        invalid = true;
                    } else {
                        left_path.pop();
                        right_path.reverse();
                        concat_success = left_path.append_path(right_path);
                    }
                }
            }

            if invalid {
                Err(FnExecError::unexpected_data_error(&format!(
                    "Concat vertices are not the same in PathConcat"
                )))?
            } else if !concat_success {
                return Ok(Object::None.into());
            } else {
                DynEntry::new(left_path)
            }
        }
        Projector::PathValueProjector(path) => path.exec_projector(input)?,
    };
    Ok(entry)
}

impl FilterMapFunction<Record, Record> for ProjectOperator {
    fn exec(&self, mut input: Record) -> FnResult<Option<Record>> {
        if self.is_append {
            if self.projected_columns.len() == 1 {
                let (projector, alias) = self.projected_columns.get(0).unwrap();
                let entry = exec_projector(&input, &projector)?;
                if entry.is_none() {
                    Ok(None)
                } else {
                    input.append_arc_entry(entry, alias.clone());
                    Ok(Some(input))
                }
            } else {
                for (projector, alias) in self.projected_columns.iter() {
                    let entry = exec_projector(&input, projector)?;
                    // Notice that if multiple columns, alias cannot be None
                    if let Some(alias) = alias {
                        let columns = input.get_columns_mut();
                        columns.insert(*alias as usize, entry);
                    }
                }
                // set head as None when the last column is appended
                input.set_curr_entry(None);
                Ok(Some(input))
            }
        } else {
            let mut new_record = Record::default();
            if self.projected_columns.len() == 1 {
                let (projector, alias) = self.projected_columns.get(0).unwrap();
                let entry = exec_projector(&input, &projector)?;
                if entry.is_none() {
                    Ok(None)
                } else {
                    new_record.append_arc_entry(entry, alias.clone());
                    Ok(Some(new_record))
                }
            } else {
                for (projector, alias) in self.projected_columns.iter() {
                    let entry = exec_projector(&input, &projector)?;
                    // Notice that if multiple columns, alias cannot be None
                    if let Some(alias) = alias {
                        let columns = new_record.get_columns_mut();
                        columns.insert(*alias as usize, entry);
                    }
                }
                Ok(Some(new_record))
            }
        }
    }
}

impl FilterMapFuncGen for pb::Project {
    fn gen_filter_map(self) -> FnGenResult<Box<dyn FilterMapFunction<Record, Record>>> {
        let mut projected_columns = Vec::with_capacity(self.mappings.len());
        for expr_alias in self.mappings.into_iter() {
            let expr = expr_alias
                .expr
                .ok_or_else(|| ParsePbError::from("expr eval is missing in project"))?;
            let projector = if expr.operators.len() == 1 {
                match expr.operators.get(0).unwrap() {
                    common_pb::ExprOpr { item: Some(common_pb::expr_opr::Item::Var(var)), .. } => {
                        let tag_key = TagKey::try_from(var.clone())?;
                        Projector::GraphElementProjector(tag_key)
                    }
                    common_pb::ExprOpr { item: Some(common_pb::expr_opr::Item::Vars(vars)), .. } => {
                        let tag_keys = vars
                            .keys
                            .iter()
                            .map(|var| TagKey::try_from(var.clone()))
                            .collect::<Result<Vec<TagKey>, _>>()?;
                        Projector::VecProjector(tag_keys)
                    }
                    common_pb::ExprOpr { item: Some(common_pb::expr_opr::Item::Map(key_vals)), .. } => {
                        let variable_key_values = VariableKeyValues::try_from(key_vals.clone())?;
                        Projector::MapProjector(variable_key_values)
                    }
                    common_pb::ExprOpr {
                        item: Some(common_pb::expr_opr::Item::PathConcat(concat_vars)),
                        ..
                    } => {
                        let left = concat_vars.left.as_ref().ok_or_else(|| {
                            ParsePbError::EmptyFieldError(format!(
                                "left in PathConcat Expr {:?}",
                                concat_vars
                            ))
                        })?;
                        let left_path_tag = left.path_tag.clone().ok_or_else(|| {
                            ParsePbError::EmptyFieldError(format!(
                                "path_tag in PathConcat Expr {:?}",
                                concat_vars
                            ))
                        })?;
                        let left_endpoint: common_pb::path_concat::Endpoint =
                            unsafe { std::mem::transmute(left.endpoint) };
                        let right = concat_vars.right.as_ref().ok_or_else(|| {
                            ParsePbError::EmptyFieldError(format!(
                                "right in PathConcat Expr {:?}",
                                concat_vars
                            ))
                        })?;

                        let right_path_tag = right.path_tag.clone().ok_or_else(|| {
                            ParsePbError::EmptyFieldError(format!(
                                "path_tag in PathConcat Expr {:?}",
                                concat_vars
                            ))
                        })?;
                        let right_endpoint: common_pb::path_concat::Endpoint =
                            unsafe { std::mem::transmute(right.endpoint) };
                        Projector::PathConcatProjector(
                            (TagKey::try_from(left_path_tag)?, left_endpoint),
                            (TagKey::try_from(right_path_tag)?, right_endpoint),
                        )
                    }
                    common_pb::ExprOpr {
                        item: Some(common_pb::expr_opr::Item::PathFunc(path_func)),
                        ..
                    } => {
                        let path_key_values = PathTagKeyValues::try_from(path_func.clone())?;
                        Projector::PathValueProjector(path_key_values)
                    }
                    _ => {
                        let evaluator = Evaluator::try_from(expr)?;
                        Projector::ExprProjector(evaluator)
                    }
                }
            } else {
                let evaluator = Evaluator::try_from(expr)?;
                Projector::ExprProjector(evaluator)
            };
            projected_columns.push((projector, expr_alias.alias));
        }
        let project_operator = ProjectOperator { is_append: self.is_append, projected_columns };
        if log_enabled!(log::Level::Debug) && pegasus::get_current_worker().index == 0 {
            debug!("Runtime project operator {:?}", project_operator);
        }
        Ok(Box::new(project_operator))
    }
}

impl TryFrom<common_pb::VariableKeyValues> for VariableKeyValues {
    type Error = ParsePbError;

    fn try_from(key_vals: common_pb::VariableKeyValues) -> Result<Self, Self::Error> {
        let mut vec = Vec::with_capacity(key_vals.key_vals.len());
        for key_val in key_vals.key_vals {
            let (_key, _value) = (key_val.key, key_val.value);
            let key = if let Some(key) = _key {
                Object::try_from(key.clone())?
            } else {
                return Err(ParsePbError::from("empty key provided in Map"));
            };
            let value = if let Some(val) = _value {
                match val {
                    common_pb::variable_key_value::Value::Val(val) => {
                        VariableValue::Value(TagKey::try_from(val.clone())?)
                    }
                    common_pb::variable_key_value::Value::Nested(nested_vals) => {
                        let nested = VariableKeyValues::try_from(nested_vals)?;
                        VariableValue::Nest(nested)
                    }
                    common_pb::variable_key_value::Value::PathFunc(path_func) => {
                        let path_key_values = path_func.try_into()?;
                        VariableValue::PathFunc(path_key_values)
                    }
                }
            } else {
                return Err(ParsePbError::from("empty value provided in Map"));
            };
            vec.push(VariableKeyValue { key, value });
        }
        Ok(VariableKeyValues { key_vals: vec })
    }
}

impl TryFrom<common_pb::PathFunction> for PathTagKeyValues {
    type Error = ParsePbError;

    fn try_from(path_func: common_pb::PathFunction) -> Result<Self, Self::Error> {
        let path_key = path_func
            .path_key
            .ok_or_else(|| ParsePbError::from("empty path key"))?;
        let func_opt = unsafe { std::mem::transmute(path_func.opt) };
        let path_key_values = PathTagKeyValues {
            tag: path_func
                .tag
                .map(|tag| KeyId::try_from(tag))
                .transpose()?,
            val: match path_key {
                common_pb::path_function::PathKey::Property(prop) => {
                    PathKey::Property(PropKey::try_from(prop)?)
                }
                common_pb::path_function::PathKey::Vars(vars) => PathKey::Vec(
                    vars.keys
                        .into_iter()
                        .map(|prop| PropKey::try_from(prop))
                        .collect::<Result<Vec<PropKey>, _>>()?,
                ),
                common_pb::path_function::PathKey::Map(map) => PathKey::Map(
                    map.key_vals
                        .into_iter()
                        .map(|key_val| {
                            let key = Object::try_from(key_val.key.unwrap());
                            let value = PropKey::try_from(key_val.val.unwrap());
                            if key.is_ok() && value.is_ok() {
                                Ok((key.unwrap(), value.unwrap()))
                            } else {
                                Err(ParsePbError::from("invalid key-value pair in Map"))
                            }
                        })
                        .collect::<Result<Vec<(Object, PropKey)>, _>>()?,
                ),
            },
            _opt: func_opt,
        };
        Ok(path_key_values)
    }
}

#[cfg(test)]
mod tests {
    use std::vec;

    use ahash::HashMap;
    use dyn_type::Object;
    use graph_proxy::apis::{DynDetails, Edge, GraphElement, GraphPath, Vertex};
    use ir_common::expr_parse::str_to_expr_pb;
    use ir_common::generated::{common as common_pb, physical as pb};
    use ir_common::NameOrId;
    use pegasus::api::{Map, Sink};
    use pegasus::result::ResultStream;
    use pegasus::JobConf;
    use pegasus_common::downcast::AsAny;

    use crate::process::entry::{CollectionEntry, Entry, PairEntry};
    use crate::process::operator::map::FilterMapFuncGen;
    use crate::process::operator::tests::{
        init_source, init_source_with_multi_tags, init_source_with_tag, init_vertex1, init_vertex2,
        to_expr_map_pb, to_expr_var_pb, to_expr_vars_pb, to_prop_pb, to_var_pb, PERSON_LABEL, TAG_A, TAG_B,
        TAG_C, TAG_D, TAG_E, TAG_F, TAG_G,
    };
    use crate::process::record::Record;

    fn project_test(source: Vec<Record>, project_opr_pb: pb::Project) -> ResultStream<Record> {
        let conf = JobConf::new("project_test");
        let result = pegasus::run(conf, || {
            let source = source.clone();
            let project_opr_pb = project_opr_pb.clone();
            |input, output| {
                let mut stream = input.input_from(source.into_iter())?;
                let project_func = project_opr_pb.gen_filter_map().unwrap();
                stream = stream.filter_map(move |i| project_func.exec(i))?;
                stream.sink_into(output)
            }
        })
        .expect("build job failure");

        result
    }

    // g.V().valueMap("id")
    #[test]
    fn project_single_mapping_test() {
        let project_opr_pb = pb::Project {
            mappings: vec![pb::project::ExprAlias {
                expr: Some(str_to_expr_pb("@.id".to_string()).unwrap()),
                alias: None,
            }],
            is_append: false,
        };
        let mut result = project_test(init_source(), project_opr_pb);
        let mut object_result = vec![];
        while let Some(Ok(res)) = result.next() {
            object_result.push(
                res.get(None)
                    .unwrap()
                    .as_object()
                    .unwrap()
                    .clone(),
            );
        }
        let expected_result = vec![object!(1), object!(2)];
        assert_eq!(object_result, expected_result);
    }

    // g.V().as("a").select("a").by("name").as("b")
    #[test]
    fn project_tag_single_mapping_test() {
        let project_opr_pb = pb::Project {
            mappings: vec![pb::project::ExprAlias {
                expr: Some(to_expr_var_pb(Some(TAG_A.into()), Some("name".into()))),
                alias: Some(TAG_B.into()),
            }],
            is_append: false,
        };
        let mut result = project_test(init_source_with_tag(), project_opr_pb);

        let mut object_result = vec![];
        while let Some(Ok(res)) = result.next() {
            let a_entry = res.get(Some(TAG_A));
            assert_eq!(a_entry, None);
            object_result.push(
                res.get(Some(TAG_B))
                    .unwrap()
                    .as_object()
                    .unwrap()
                    .clone(),
            );
        }
        let expected_result = vec![object!("marko"), object!("vadas")];
        assert_eq!(object_result, expected_result);
    }

    // g.V().as("a").valueMap("id")
    #[test]
    fn project_none_tag_single_mapping_test() {
        let project_opr_pb = pb::Project {
            mappings: vec![pb::project::ExprAlias {
                expr: Some(str_to_expr_pb("@.id".to_string()).unwrap()),
                alias: None,
            }],
            is_append: false,
        };
        let mut result = project_test(init_source_with_tag(), project_opr_pb);
        let mut object_result = vec![];
        while let Some(Ok(res)) = result.next() {
            object_result.push(
                res.get(None)
                    .unwrap()
                    .as_object()
                    .unwrap()
                    .clone(),
            );
        }
        let expected_result = vec![object!(1), object!(2)];
        assert_eq!(object_result, expected_result);
    }

    // g.V().valueMap('age', 'name') with alias of 'age' as 'b' and 'name' as 'c'
    #[test]
    fn project_multi_mapping_test() {
        let project_opr_pb = pb::Project {
            mappings: vec![
                pb::project::ExprAlias {
                    expr: Some(str_to_expr_pb("@.age".to_string()).unwrap()),
                    alias: Some(TAG_B.into()),
                },
                pb::project::ExprAlias {
                    expr: Some(str_to_expr_pb("@.name".to_string()).unwrap()),
                    alias: Some(TAG_C.into()),
                },
            ],
            is_append: false,
        };
        let mut result = project_test(init_source(), project_opr_pb);
        let mut object_result = vec![];
        while let Some(Ok(res)) = result.next() {
            // head should be None
            assert_eq!(res.get(None), None);
            let age_val = res
                .get(Some(TAG_B))
                .unwrap()
                .as_object()
                .unwrap();
            let name_val = res
                .get(Some(TAG_C))
                .unwrap()
                .as_object()
                .unwrap();
            object_result.push((age_val.clone(), name_val.clone()));
        }
        let expected_result = vec![(object!(29), object!("marko")), (object!(27), object!("vadas"))];
        assert_eq!(object_result, expected_result);
    }

    // g.V().as('a').select('a').by(valueMap('age', 'name')) with alias of 'age' as 'b' and 'name' as 'c'
    #[test]
    fn project_tag_multi_mapping_test() {
        let project_opr_pb = pb::Project {
            mappings: vec![
                pb::project::ExprAlias {
                    expr: Some(to_expr_var_pb(Some(TAG_A.into()), Some("age".into()))),
                    alias: Some(TAG_B.into()),
                },
                pb::project::ExprAlias {
                    expr: Some(to_expr_var_pb(Some(TAG_A.into()), Some("name".into()))),
                    alias: Some(TAG_C.into()),
                },
            ],
            is_append: false,
        };
        let mut result = project_test(init_source_with_tag(), project_opr_pb);

        let mut object_result = vec![];
        while let Some(Ok(res)) = result.next() {
            let age = res
                .get(Some(TAG_B))
                .unwrap()
                .as_object()
                .unwrap();
            let name = res
                .get(Some(TAG_C))
                .unwrap()
                .as_object()
                .unwrap();
            object_result.push((age.clone(), name.clone()));
        }
        let expected_result = vec![(object!(29), object!("marko")), (object!(27), object!("vadas"))];
        assert_eq!(object_result, expected_result);
    }

    // g.V().as('a').out().as('b').select('a', 'b').by(valueMap('age', 'name')).by('name'),
    // with alias of 'a.age' as 'c', 'a.name' as 'd' and 'b.name' as 'e'
    #[test]
    fn project_multi_tag_multi_mapping() {
        // 1->3
        // 2->3
        let v1 = init_vertex1();
        let v2 = init_vertex2();
        let map3: HashMap<NameOrId, Object> =
            vec![("id".into(), object!(3)), ("age".into(), object!(32)), ("name".into(), object!("josh"))]
                .into_iter()
                .collect();
        let v3 = Vertex::new(3, Some(PERSON_LABEL), DynDetails::new(map3));
        let mut r1 = Record::new(v1, Some(TAG_A.into()));
        r1.append(v3.clone(), Some(TAG_B.into()));
        let mut r2 = Record::new(v2, Some(TAG_A.into()));
        r2.append(v3.clone(), Some(TAG_B.into()));
        let source = vec![r1, r2];

        let project_opr_pb = pb::Project {
            mappings: vec![
                pb::project::ExprAlias {
                    expr: Some(to_expr_var_pb(Some(TAG_A.into()), Some("age".into()))),
                    alias: Some(TAG_C.into()),
                },
                pb::project::ExprAlias {
                    expr: Some(to_expr_var_pb(Some(TAG_A.into()), Some("name".into()))),
                    alias: Some(TAG_D.into()),
                },
                pb::project::ExprAlias {
                    expr: Some(to_expr_var_pb(Some(TAG_B.into()), Some("name".into()))),
                    alias: Some(TAG_E.into()),
                },
            ],
            is_append: false,
        };
        let mut result = project_test(source, project_opr_pb);

        let mut object_result = vec![];
        while let Some(Ok(res)) = result.next() {
            let a_age = res
                .get(Some(TAG_C))
                .unwrap()
                .as_object()
                .unwrap();
            let a_name = res
                .get(Some(TAG_D))
                .unwrap()
                .as_object()
                .unwrap();
            let b_name = res
                .get(Some(TAG_E))
                .unwrap()
                .as_object()
                .unwrap();
            object_result.push((a_age.clone(), a_name.clone(), b_name.clone()));
        }
        let expected_result = vec![
            (object!(29), object!("marko"), object!("josh")),
            (object!(27), object!("vadas"), object!("josh")),
        ];
        assert_eq!(object_result, expected_result);
    }

    // g.V().as('a').select('a').by(valueMap('age')) with 'age' as 'b' and append 'b'
    #[test]
    fn project_single_mapping_appended_test() {
        let project_opr_pb = pb::Project {
            mappings: vec![pb::project::ExprAlias {
                expr: Some(to_expr_var_pb(Some(TAG_A.into()), Some("age".into()))),
                alias: Some(TAG_B.into()),
            }],
            is_append: true,
        };
        let mut result = project_test(init_source_with_tag(), project_opr_pb);
        let mut a_results = vec![];
        let mut b_results = vec![];
        while let Some(Ok(res)) = result.next() {
            let v = res
                .get(Some(TAG_A))
                .unwrap()
                .as_vertex()
                .unwrap();
            a_results.push(v.id());
            let b_val = res
                .get(Some(TAG_B))
                .unwrap()
                .as_object()
                .unwrap();
            b_results.push(b_val.clone());
        }
        let expected_a_result = vec![1, 2];
        let expected_b_result = vec![object!(29), object!(27)];
        assert_eq!(a_results, expected_a_result);
        assert_eq!(b_results, expected_b_result);
    }

    // g.V().valueMap('age', 'name') with alias of 'age' as 'b' and 'name' as 'c'
    #[test]
    fn project_multi_mapping_appended_test() {
        let project_opr_pb = pb::Project {
            mappings: vec![
                pb::project::ExprAlias {
                    expr: Some(str_to_expr_pb("@.age".to_string()).unwrap()),
                    alias: Some(TAG_B.into()),
                },
                pb::project::ExprAlias {
                    expr: Some(str_to_expr_pb("@.name".to_string()).unwrap()),
                    alias: Some(TAG_C.into()),
                },
            ],
            is_append: true,
        };
        let mut result = project_test(init_source(), project_opr_pb);
        let mut object_result = vec![];
        while let Some(Ok(res)) = result.next() {
            let age = res
                .get(Some(TAG_B))
                .unwrap()
                .as_object()
                .unwrap();
            let name = res
                .get(Some(TAG_C))
                .unwrap()
                .as_object()
                .unwrap();
            object_result.push((age.clone(), name.clone()));
        }
        let expected_result = vec![(object!(29), object!("marko")), (object!(27), object!("vadas"))];
        assert_eq!(object_result, expected_result);
    }

    // None expr is not allowed
    #[test]
    fn project_empty_mapping_expr_test() {
        let project_opr_pb = pb::Project {
            mappings: vec![pb::project::ExprAlias { expr: None, alias: None }],
            is_append: false,
        };
        let project_func = project_opr_pb.gen_filter_map();
        if let Err(_) = project_func {
            assert!(true)
        }
    }

    // None alias is not allowed.
    #[test]
    fn project_empty_mapping_alias_test() {
        let project_opr_pb = pb::Project {
            mappings: vec![pb::project::ExprAlias {
                expr: Some(str_to_expr_pb("@.id".to_string()).unwrap()),
                alias: None,
            }],
            is_append: true,
        };
        let project_func = project_opr_pb.gen_filter_map();
        if let Err(_) = project_func {
            assert!(true)
        }
    }

    // g.V().valueMap("age", "name") // by vec
    #[test]
    fn project_vec_mapping_test() {
        let project_opr_pb = pb::Project {
            mappings: vec![pb::project::ExprAlias {
                expr: Some(str_to_expr_pb("[@.age,@.name]".to_string()).unwrap()),
                alias: None,
            }],
            is_append: false,
        };
        let mut result = project_test(init_source(), project_opr_pb);
        let mut collection_result: Vec<Vec<Object>> = vec![];
        while let Some(Ok(res)) = result.next() {
            let collection_entry = res
                .get(None)
                .unwrap()
                .as_any_ref()
                .downcast_ref::<CollectionEntry>()
                .unwrap()
                .inner
                .clone()
                .into_iter()
                .map(|entry| entry.as_object().unwrap().clone())
                .collect();
            collection_result.push(collection_entry);
        }
        let expected_result =
            vec![vec![object!(29), object!("marko")], vec![object!(27), object!("vadas")]];
        assert_eq!(collection_result, expected_result);
    }

    // g.V().valueMap("age", "name") // by varmap
    #[test]
    fn project_varmap_mapping_test() {
        let project_opr_pb = pb::Project {
            mappings: vec![pb::project::ExprAlias {
                expr: Some(str_to_expr_pb("{@.age,@.name}".to_string()).unwrap()),
                alias: None,
            }],
            is_append: false,
        };
        let mut result = project_test(init_source(), project_opr_pb);
        let mut object_result = vec![];
        while let Some(Ok(res)) = result.next() {
            object_result.push(
                res.get(None)
                    .unwrap()
                    .as_object()
                    .unwrap()
                    .clone(),
            );
        }
        let expected_result = vec![
            Object::KV(
                vec![
                    (object!(vec![Object::None, object!("age")]), object!(29)),
                    (object!(vec![Object::None, object!("name")]), object!("marko")),
                ]
                .into_iter()
                .collect(),
            ),
            Object::KV(
                vec![
                    (object!(vec![Object::None, object!("age")]), object!(27)),
                    (object!(vec![Object::None, object!("name")]), object!("vadas")),
                ]
                .into_iter()
                .collect(),
            ),
        ];
        assert_eq!(object_result, expected_result);
    }

    // g.V().valueMap('age', 'name').isNull, with alias of 'age' as 'newAge' and 'name' as 'newName' (by map)
    // this is projected by ExprProjector
    #[test]
    fn project_map_mapping_test() {
        let mut expr = to_expr_map_pb(vec![
            ("newAge".to_string(), (None, Some("age".into()))),
            ("newName".to_string(), (None, Some("name".into()))),
        ]);
        expr.operators.push(common_pb::ExprOpr {
            item: Some(common_pb::expr_opr::Item::Logical(13)), // isNull
            node_type: None,
        });
        let project_opr_pb = pb::Project {
            mappings: vec![pb::project::ExprAlias { expr: Some(expr), alias: None }],
            is_append: false,
        };
        let mut result = project_test(init_source(), project_opr_pb);
        let mut object_result = vec![];
        while let Some(Ok(res)) = result.next() {
            let value = res.get(None).unwrap().as_object().unwrap();
            object_result.push(value.clone());
        }
        let expected_result = vec![object!(false), object!(false)];
        assert_eq!(object_result, expected_result);
    }

    // g.V().valueMap('age', 'name') with alias of 'age' as 'newAge' and 'name' as 'newName', by map
    // this is projected by MapProjector
    #[test]
    fn simple_project_map_mapping_test() {
        let project_opr_pb = pb::Project {
            mappings: vec![pb::project::ExprAlias {
                expr: Some(to_expr_map_pb(vec![
                    ("newAge".to_string(), (None, Some("age".into()))),
                    ("newName".to_string(), (None, Some("name".into()))),
                ])),
                alias: None,
            }],
            is_append: false,
        };
        let mut result = project_test(init_source(), project_opr_pb);
        let mut object_result = vec![];
        while let Some(Ok(res)) = result.next() {
            let collection = res
                .get(None)
                .unwrap()
                .as_any_ref()
                .downcast_ref::<CollectionEntry>()
                .unwrap();
            let mut result = vec![];
            for entry in collection.inner.iter() {
                let pair_entry = entry
                    .as_any_ref()
                    .downcast_ref::<PairEntry>()
                    .unwrap();
                let key = pair_entry
                    .get_left()
                    .as_any_ref()
                    .downcast_ref::<Object>()
                    .unwrap();
                let value = pair_entry
                    .get_right()
                    .as_any_ref()
                    .downcast_ref::<Object>()
                    .unwrap();
                result.push((key.clone(), value.clone()));
            }
            object_result.push(result);
        }
        let expected_result = vec![
            vec![(object!("newAge"), object!(29)), (object!("newName"), object!("marko"))],
            vec![(object!("newAge"), object!(27)), (object!("newName"), object!("vadas"))],
        ];
        assert_eq!(object_result, expected_result);
    }

    // g.V().as("a").select("a").by(valueMap("age", "name")) // by map
    #[test]
    fn project_tag_varmap_mapping_test() {
        let project_opr_pb = pb::Project {
            mappings: vec![pb::project::ExprAlias {
                expr: Some(to_expr_vars_pb(
                    vec![
                        (Some(TAG_A.into()), Some("age".into())),
                        (Some(TAG_A.into()), Some("name".into())),
                    ],
                    true,
                )),
                alias: None,
            }],
            is_append: false,
        };
        let mut result = project_test(init_source_with_tag(), project_opr_pb);
        let mut object_result = vec![];
        while let Some(Ok(res)) = result.next() {
            object_result.push(
                res.get(None)
                    .unwrap()
                    .as_object()
                    .unwrap()
                    .clone(),
            );
        }

        let expected_result = vec![
            Object::KV(
                vec![
                    (object!(vec![object!(TAG_A), object!("age")]), object!(29)),
                    (object!(vec![object!(TAG_A), object!("name")]), object!("marko")),
                ]
                .into_iter()
                .collect(),
            ),
            Object::KV(
                vec![
                    (object!(vec![object!(TAG_A), object!("age")]), object!(27)),
                    (object!(vec![object!(TAG_A), object!("name")]), object!("vadas")),
                ]
                .into_iter()
                .collect(),
            ),
        ];
        assert_eq!(object_result, expected_result);
    }

    // g.V().as("a").select("a").by(valueMap("age", "name")),with alias of 'a.age' as 'newAge' and 'a.name' as 'newName', by map
    // this is projected by ExprProjector
    #[test]
    fn project_tag_map_mapping_test() {
        let mut expr = to_expr_map_pb(vec![
            ("newAge".to_string(), (Some(TAG_A.into()), Some("age".into()))),
            ("newName".to_string(), (Some(TAG_A.into()), Some("name".into()))),
        ]);
        expr.operators.push(common_pb::ExprOpr {
            item: Some(common_pb::expr_opr::Item::Logical(13)), // isNull
            node_type: None,
        });
        let project_opr_pb = pb::Project {
            mappings: vec![pb::project::ExprAlias { expr: Some(expr), alias: None }],
            is_append: false,
        };
        let mut result = project_test(init_source_with_tag(), project_opr_pb);
        let mut object_result = vec![];
        while let Some(Ok(res)) = result.next() {
            object_result.push(
                res.get(None)
                    .unwrap()
                    .as_object()
                    .unwrap()
                    .clone(),
            );
        }

        let expected_result = vec![object!(false), object!(false)];
        assert_eq!(object_result, expected_result);
    }

    // g.V().as("a").select("a").by(valueMap("age", "name")),with alias of 'a.age' as 'newAge' and 'a.tname' as 'newName', by map
    // this is projected by MapProjector
    #[test]
    fn simple_project_tag_map_mapping_test() {
        let project_opr_pb = pb::Project {
            mappings: vec![pb::project::ExprAlias {
                expr: Some(to_expr_map_pb(vec![
                    ("newName".to_string(), (Some(TAG_A.into()), Some("name".into()))),
                    ("newAge".to_string(), (Some(TAG_A.into()), Some("age".into()))),
                ])),
                alias: None,
            }],
            is_append: false,
        };
        let mut result = project_test(init_source_with_tag(), project_opr_pb);

        let mut object_result = vec![];
        while let Some(Ok(res)) = result.next() {
            let collection = res
                .get(None)
                .unwrap()
                .as_any_ref()
                .downcast_ref::<CollectionEntry>()
                .unwrap();
            let mut result = vec![];
            for entry in collection.inner.iter() {
                let pair_entry = entry
                    .as_any_ref()
                    .downcast_ref::<PairEntry>()
                    .unwrap();
                let key = pair_entry
                    .get_left()
                    .as_any_ref()
                    .downcast_ref::<Object>()
                    .unwrap();
                let value = pair_entry
                    .get_right()
                    .as_any_ref()
                    .downcast_ref::<Object>()
                    .unwrap();
                result.push((key.clone(), value.clone()));
            }
            object_result.push(result);
        }
        let expected_result = vec![
            vec![(object!("newName"), object!("marko")), (object!("newAge"), object!(29))],
            vec![(object!("newName"), object!("vadas")), (object!("newAge"), object!(27))],
        ];
        assert_eq!(object_result, expected_result);
    }

    // g.V().as("a").select("a").by(valueMap("age", "name")), with expr_opr as a nested VariableKeyValues
    // this is projected by MapProjector (nested)
    #[test]
    fn project_nested_map_test() {
        // project a.{name, age}
        let variable_key_value = common_pb::VariableKeyValues {
            key_vals: vec![
                common_pb::VariableKeyValue {
                    key: Some(common_pb::Value::from("name".to_string())),
                    value: Some(common_pb::variable_key_value::Value::Val(to_var_pb(
                        None,
                        Some("name".into()),
                    ))),
                },
                common_pb::VariableKeyValue {
                    key: Some(common_pb::Value::from("age".to_string())),
                    value: Some(common_pb::variable_key_value::Value::Val(to_var_pb(
                        None,
                        Some("age".into()),
                    ))),
                },
            ],
        };
        let nested_variable_key_vals = common_pb::VariableKeyValues {
            key_vals: vec![common_pb::VariableKeyValue {
                key: Some(common_pb::Value::from("a".to_string())),
                value: Some(common_pb::variable_key_value::Value::Nested(variable_key_value)),
            }],
        };
        let project_opr_pb = pb::Project {
            mappings: vec![pb::project::ExprAlias {
                expr: Some(common_pb::Expression {
                    operators: vec![common_pb::ExprOpr {
                        node_type: None,
                        item: Some(common_pb::expr_opr::Item::Map(nested_variable_key_vals)),
                    }],
                }),
                alias: None,
            }],
            is_append: false,
        };
        let mut result = project_test(init_source_with_tag(), project_opr_pb);

        let mut object_result = vec![];
        while let Some(Ok(res)) = result.next() {
            let collection = res
                .get(None)
                .unwrap()
                .as_any_ref()
                .downcast_ref::<CollectionEntry>()
                .unwrap();
            let mut result = vec![];
            for entry in collection.inner.iter() {
                let pair_entry = entry
                    .as_any_ref()
                    .downcast_ref::<PairEntry>()
                    .unwrap();
                let key = pair_entry
                    .get_left()
                    .as_any_ref()
                    .downcast_ref::<Object>()
                    .unwrap();
                let value = pair_entry
                    .get_right()
                    .as_any_ref()
                    .downcast_ref::<CollectionEntry>()
                    .unwrap();
                let mut result_value = vec![];
                for entry in value.inner.iter() {
                    let inner_pair = entry
                        .as_any_ref()
                        .downcast_ref::<PairEntry>()
                        .unwrap();
                    let inner_key = inner_pair
                        .get_left()
                        .as_any_ref()
                        .downcast_ref::<Object>()
                        .unwrap();
                    let inner_val = inner_pair
                        .get_right()
                        .as_any_ref()
                        .downcast_ref::<Object>()
                        .unwrap();
                    result_value.push((inner_key.clone(), inner_val.clone()));
                }
                result.push((key.clone(), result_value.clone()));
            }
            object_result.push(result);
        }
        let expected_result = vec![
            vec![(object!("a"), vec![(object!("name"), object!("marko")), (object!("age"), object!(29))])],
            vec![(object!("a"), vec![(object!("name"), object!("vadas")), (object!("age"), object!(27))])],
        ];
        assert_eq!(object_result, expected_result);
    }

    #[test]
    fn project_multi_mapping_tags() {
        let project_opr_pb = pb::Project {
            mappings: vec![
                pb::project::ExprAlias {
                    expr: Some(to_expr_var_pb(Some(TAG_A.into()), None)),
                    alias: Some(TAG_C.into()),
                },
                pb::project::ExprAlias {
                    expr: Some(to_expr_var_pb(Some(TAG_B.into()), None)),
                    alias: Some(TAG_D.into()),
                },
            ],
            is_append: true,
        };
        let mut result = project_test(init_source_with_multi_tags(), project_opr_pb);
        let mut results = vec![];
        while let Some(Ok(res)) = result.next() {
            let v1 = res
                .get(Some(TAG_C))
                .unwrap()
                .as_vertex()
                .unwrap();
            let v2 = res
                .get(Some(TAG_D))
                .unwrap()
                .as_vertex()
                .unwrap();
            results.push(v1.id());
            results.push(v2.id());
        }
        let expected_results = vec![1, 2];
        assert_eq!(results, expected_results);
    }

    // g.V().select('a')
    #[test]
    fn project_none_exist_tag_test() {
        let project_opr_pb = pb::Project {
            mappings: vec![pb::project::ExprAlias {
                expr: Some(to_expr_var_pb(Some(TAG_A.into()), None)),
                alias: None,
            }],
            is_append: true,
        };
        let mut result = project_test(init_source(), project_opr_pb);
        let mut result_cnt = 0;
        while let Some(Ok(_res)) = result.next() {
            result_cnt += 1;
        }
        assert_eq!(result_cnt, 0);
    }

    // g.V().as('a').select('a').by('test')
    #[test]
    fn project_none_exist_prop_test() {
        let project_opr_pb = pb::Project {
            mappings: vec![pb::project::ExprAlias {
                expr: Some(to_expr_var_pb(Some(TAG_A.into()), Some("test".into()))),
                alias: None,
            }],
            is_append: true,
        };
        let mut result = project_test(init_source_with_tag(), project_opr_pb);
        let mut result_cnt = 0;
        while let Some(Ok(_res)) = result.next() {
            result_cnt += 1;
        }
        assert_eq!(result_cnt, 0);
    }

    // g.V().select("a","b")
    #[test]
    fn project_multi_none_exist_tag_test() {
        let project_opr_pb = pb::Project {
            mappings: vec![
                pb::project::ExprAlias {
                    expr: Some(to_expr_var_pb(Some(TAG_A.into()), None)),
                    alias: Some(TAG_C.into()),
                },
                pb::project::ExprAlias {
                    expr: Some(to_expr_var_pb(Some(TAG_B.into()), None)),
                    alias: Some(TAG_D.into()),
                },
            ],
            is_append: false,
        };
        let mut result = project_test(init_source(), project_opr_pb);
        let mut result_cnt = 0;
        while let Some(Ok(res)) = result.next() {
            result_cnt += 1;
            // head should be None
            assert_eq!(res.get(None), None);
            let c_val = res
                .get(Some(TAG_C))
                .unwrap()
                .as_object()
                .unwrap();
            let d_val = res
                .get(Some(TAG_D))
                .unwrap()
                .as_object()
                .unwrap();
            match (c_val, d_val) {
                (Object::None, Object::None) => {
                    assert!(true)
                }
                _ => {
                    assert!(false)
                }
            }
        }
        assert_eq!(result_cnt, 2);
    }

    // g.V().as("a").select("a").by(valueMap("test1", "test2"))
    #[test]
    fn project_multi_none_exist_props_test() {
        let project_opr_pb = pb::Project {
            mappings: vec![
                pb::project::ExprAlias {
                    expr: Some(to_expr_var_pb(Some(TAG_A.into()), Some("test1".into()))),
                    alias: Some(TAG_C.into()),
                },
                pb::project::ExprAlias {
                    expr: Some(to_expr_var_pb(Some(TAG_A.into()), Some("test2".into()))),
                    alias: Some(TAG_D.into()),
                },
            ],
            is_append: false,
        };
        let mut result = project_test(init_source_with_tag(), project_opr_pb);
        let mut result_cnt = 0;
        while let Some(Ok(res)) = result.next() {
            result_cnt += 1;
            // head should be None
            assert_eq!(res.get(None), None);
            let c_val = res
                .get(Some(TAG_C))
                .unwrap()
                .as_object()
                .unwrap();
            let d_val = res
                .get(Some(TAG_D))
                .unwrap()
                .as_object()
                .unwrap();
            match (c_val, d_val) {
                (Object::None, Object::None) => {
                    assert!(true)
                }
                _ => {
                    assert!(false)
                }
            }
        }
        assert_eq!(result_cnt, 2);
    }

    // g.V().values('id').as('a').select('a').by('id'), throw an unexpected data type error
    #[test]
    fn project_error_test() {
        let common_obj1 = object!(1);
        let common_obj2 = object!(2);
        let r1 = Record::new(common_obj1, Some(TAG_A.into()));
        let r2 = Record::new(common_obj2, Some(TAG_A.into()));
        let source = vec![r1, r2];
        let project_opr_pb = pb::Project {
            mappings: vec![pb::project::ExprAlias {
                expr: Some(to_expr_var_pb(Some(TAG_A.into()), Some("id".into()))),
                alias: None,
            }],
            is_append: true,
        };
        let mut result = project_test(source, project_opr_pb);
        if let Some(Err(_res)) = result.next() {
            assert!(true)
        }
    }

    #[test]
    fn project_extract_from_date_test() {
        // 2010-01-02
        let date_obj = Object::DateFormat(dyn_type::DateTimeFormats::from_date32(20100102).unwrap());
        // 12:34:56.100
        let time_obj = Object::DateFormat(dyn_type::DateTimeFormats::from_time32(123456100).unwrap());
        // 2020-10-10 10:10:10
        let datetime_obj =
            Object::DateFormat(dyn_type::DateTimeFormats::from_timestamp_millis(1602324610100).unwrap());
        let mut r1 = Record::new(date_obj.clone(), Some(TAG_A.into()));
        r1.append(time_obj, Some(TAG_B.into()));
        r1.append(datetime_obj, Some(TAG_C.into()));

        let extract_date_year_opr = common_pb::ExprOpr {
            node_type: None,
            item: Some(common_pb::expr_opr::Item::Extract(common_pb::Extract {
                interval: common_pb::extract::Interval::Year as i32,
            })),
        };

        let extract_time_hour_opr = common_pb::ExprOpr {
            node_type: None,
            item: Some(common_pb::expr_opr::Item::Extract(common_pb::Extract {
                interval: common_pb::extract::Interval::Hour as i32,
            })),
        };

        let extract_datetime_month_opr = common_pb::ExprOpr {
            node_type: None,
            item: Some(common_pb::expr_opr::Item::Extract(common_pb::Extract {
                interval: common_pb::extract::Interval::Month as i32,
            })),
        };

        let extract_datetime_minute_opr = common_pb::ExprOpr {
            node_type: None,
            item: Some(common_pb::expr_opr::Item::Extract(common_pb::Extract {
                interval: common_pb::extract::Interval::Minute as i32,
            })),
        };

        let tag_a_opr = common_pb::ExprOpr {
            node_type: None,
            item: Some(common_pb::expr_opr::Item::Var(to_var_pb(Some(TAG_A.into()), None))),
        };
        let tag_b_opr = common_pb::ExprOpr {
            node_type: None,
            item: Some(common_pb::expr_opr::Item::Var(to_var_pb(Some(TAG_B.into()), None))),
        };
        let tag_c_opr = common_pb::ExprOpr {
            node_type: None,
            item: Some(common_pb::expr_opr::Item::Var(to_var_pb(Some(TAG_C.into()), None))),
        };

        let expr1 = common_pb::Expression { operators: vec![extract_date_year_opr, tag_a_opr] };

        let expr2 = common_pb::Expression { operators: vec![extract_time_hour_opr, tag_b_opr] };

        let expr3 =
            common_pb::Expression { operators: vec![extract_datetime_month_opr, tag_c_opr.clone()] };

        let expr4 =
            common_pb::Expression { operators: vec![extract_datetime_minute_opr, tag_c_opr.clone()] };

        let source = vec![r1];
        // To project: year of 2010-01-02, hour of 12:34:56.100, month of 2020-10-10 10:10:10, and minute of 2020-10-10 10:10:10
        let project_opr_pb = pb::Project {
            mappings: vec![
                pb::project::ExprAlias { expr: Some(expr1), alias: Some(TAG_D.into()) },
                pb::project::ExprAlias { expr: Some(expr2), alias: Some(TAG_E.into()) },
                pb::project::ExprAlias { expr: Some(expr3), alias: Some(TAG_F.into()) },
                pb::project::ExprAlias { expr: Some(expr4), alias: Some(TAG_G.into()) },
            ],
            is_append: true,
        };

        let mut result = project_test(source, project_opr_pb);
        let expected_results = vec![object!(2010), object!(12), object!(10), object!(10)];
        let mut results = vec![];
        while let Some(Ok(res)) = result.next() {
            let year = res
                .get(Some(TAG_D))
                .unwrap()
                .as_object()
                .unwrap();
            let hour = res
                .get(Some(TAG_E))
                .unwrap()
                .as_object()
                .unwrap();
            let month = res
                .get(Some(TAG_F))
                .unwrap()
                .as_object()
                .unwrap();
            let minute = res
                .get(Some(TAG_G))
                .unwrap()
                .as_object()
                .unwrap();
            results.push(year.clone());
            results.push(hour.clone());
            results.push(month.clone());
            results.push(minute.clone());
        }
        assert_eq!(results, expected_results);
    }

    fn build_path(vids: Vec<i64>) -> GraphPath {
        let details = DynDetails::default();
        let mut path = GraphPath::new(
            Vertex::new(vids[0], None, details.clone()),
            pb::path_expand::PathOpt::Arbitrary,
            pb::path_expand::ResultOpt::AllV,
        )
        .unwrap();
        for i in 1..vids.len() {
            path.append(Vertex::new(vids[i], None, details.clone()));
        }
        path
    }

    fn build_simple_path(vids: Vec<i64>) -> GraphPath {
        let details = DynDetails::default();
        let mut path = GraphPath::new(
            Vertex::new(vids[0], None, details.clone()),
            pb::path_expand::PathOpt::Simple,
            pb::path_expand::ResultOpt::AllV,
        )
        .unwrap();
        for i in 1..vids.len() {
            path.append(Vertex::new(vids[i], None, details.clone()));
        }
        path
    }

    fn build_project_path_concat(
        left_endpoint: common_pb::path_concat::Endpoint, right_endpoint: common_pb::path_concat::Endpoint,
    ) -> pb::Project {
        let path_concat = common_pb::PathConcat {
            left: Some(common_pb::path_concat::ConcatPathInfo {
                path_tag: Some(to_var_pb(Some(TAG_A.into()), None)),
                endpoint: left_endpoint as i32,
            }),
            right: Some(common_pb::path_concat::ConcatPathInfo {
                path_tag: Some(to_var_pb(Some(TAG_B.into()), None)),
                endpoint: right_endpoint as i32,
            }),
        };
        pb::Project {
            mappings: vec![pb::project::ExprAlias {
                expr: Some(common_pb::Expression {
                    operators: vec![common_pb::ExprOpr {
                        item: Some(common_pb::expr_opr::Item::PathConcat(path_concat)),
                        node_type: None,
                    }],
                }),
                alias: Some(TAG_C.into()),
            }],
            is_append: false,
        }
    }

    fn project_concat_allv_path_test(
        left_path: GraphPath, right_path: GraphPath, project_opr_pb: pb::Project, concat_path: GraphPath,
    ) {
        let mut r1 = Record::new(left_path, Some(TAG_A.into()));
        r1.append(right_path, Some(TAG_B.into()));
        let mut result = project_test(vec![r1], project_opr_pb);
        let mut results = vec![];
        while let Some(Ok(res)) = result.next() {
            let path = res
                .get(Some(TAG_C))
                .unwrap()
                .as_any_ref()
                .downcast_ref::<GraphPath>()
                .unwrap();
            results.push(path.clone());
        }
        assert_eq!(results, vec![concat_path]);
    }

    #[test]
    fn project_concat_allv_path_test_01() {
        // sub_path1: [1,2]
        let sub_path1 = build_path(vec![1, 2]);
        // sub_path2: [3,2]
        let sub_path2 = build_path(vec![3, 2]);
        // concat project
        let project_opr_pb = build_project_path_concat(
            common_pb::path_concat::Endpoint::End,
            common_pb::path_concat::Endpoint::End,
        );
        // concat path: [1,2,3]
        let concat_path = build_path(vec![1, 2, 3]);
        project_concat_allv_path_test(sub_path1, sub_path2, project_opr_pb, concat_path);
    }

    #[test]
    fn project_concat_allv_path_test_02() {
        // sub_path1: [1,2]
        let sub_path1 = build_path(vec![1, 2]);
        // sub_path2: [2,3]
        let sub_path2 = build_path(vec![2, 3]);
        // concat project
        let project_opr_pb = build_project_path_concat(
            common_pb::path_concat::Endpoint::End,
            common_pb::path_concat::Endpoint::Start,
        );
        // concat path: [1,2,3]
        let concat_path = build_path(vec![1, 2, 3]);
        project_concat_allv_path_test(sub_path1, sub_path2, project_opr_pb, concat_path);
    }

    #[test]
    fn project_concat_allv_path_test_03() {
        // sub_path1: [2,1]
        let sub_path1 = build_path(vec![2, 1]);
        // sub_path2: [3,2]
        let sub_path2 = build_path(vec![3, 2]);
        // concat project
        let project_opr_pb = build_project_path_concat(
            common_pb::path_concat::Endpoint::Start,
            common_pb::path_concat::Endpoint::End,
        );
        // concat path: [1,2,3]
        let concat_path = build_path(vec![1, 2, 3]);
        project_concat_allv_path_test(sub_path1, sub_path2, project_opr_pb, concat_path);
    }

    #[test]
    fn project_concat_allv_path_test_04() {
        // sub_path1: [2,1]
        let sub_path1 = build_path(vec![2, 1]);
        // sub_path2: [2,3]
        let sub_path2 = build_path(vec![2, 3]);
        // concat project
        let project_opr_pb = build_project_path_concat(
            common_pb::path_concat::Endpoint::Start,
            common_pb::path_concat::Endpoint::Start,
        );
        // concat path: [1,2,3]
        let concat_path = build_path(vec![1, 2, 3]);
        project_concat_allv_path_test(sub_path1, sub_path2, project_opr_pb, concat_path);
    }

    #[test]
    fn project_concat_allve_path_test() {
        let details = DynDetails::default();
        // sub_path1: [1 -> 2]
        let mut sub_path1 = GraphPath::new(
            Vertex::new(1, None, details.clone()),
            pb::path_expand::PathOpt::Arbitrary,
            pb::path_expand::ResultOpt::AllVE,
        )
        .unwrap();
        sub_path1.append(Edge::new(12, None, 1, 2, details.clone()));
        sub_path1.append(Vertex::new(2, None, details.clone()));
        // sub_path2: [3 <- 2]
        let mut sub_path2 = GraphPath::new(
            Vertex::new(3, None, details.clone()),
            pb::path_expand::PathOpt::Arbitrary,
            pb::path_expand::ResultOpt::AllVE,
        )
        .unwrap();
        sub_path2.append(Edge::new(23, None, 2, 3, details.clone()));
        sub_path2.append(Vertex::new(2, None, details.clone()));
        // concat path: [1 -> 2 <- 3]
        let mut concat_path = GraphPath::new(
            Vertex::new(1, None, details.clone()),
            pb::path_expand::PathOpt::Arbitrary,
            pb::path_expand::ResultOpt::AllVE,
        )
        .unwrap();
        concat_path.append(Edge::new(12, None, 1, 2, details.clone()));
        concat_path.append(Vertex::new(2, None, details.clone()));
        concat_path.append(Edge::new(23, None, 2, 3, details.clone()));
        concat_path.append(Vertex::new(3, None, details.clone()));

        let mut r1 = Record::new(sub_path1, Some(TAG_A.into()));
        r1.append(sub_path2, Some(TAG_B.into()));

        let source = vec![r1];
        let project_opr_pb = build_project_path_concat(
            common_pb::path_concat::Endpoint::End,
            common_pb::path_concat::Endpoint::End,
        );
        let mut result = project_test(source, project_opr_pb);
        let mut results = vec![];
        while let Some(Ok(res)) = result.next() {
            let path = res
                .get(Some(TAG_C))
                .unwrap()
                .as_any_ref()
                .downcast_ref::<GraphPath>()
                .unwrap();
            results.push(path.clone());
        }
        assert_eq!(results, vec![concat_path]);
    }

    #[test]
    fn project_concat_simple_path_test_01() {
        // sub_path1: [1,2]
        let sub_path1 = build_simple_path(vec![1, 2]);
        // sub_path2: [3,2]
        let sub_path2 = build_simple_path(vec![3, 2]);
        // concat project
        let project_opr_pb = build_project_path_concat(
            common_pb::path_concat::Endpoint::End,
            common_pb::path_concat::Endpoint::End,
        );
        // concat path: [1,2,3]
        let concat_path = build_path(vec![1, 2, 3]);
        project_concat_allv_path_test(sub_path1, sub_path2, project_opr_pb, concat_path);
    }

    #[test]
    fn project_concat_simple_path_test_02() {
        // sub_path1: [1,4,2]
        let sub_path1 = build_simple_path(vec![1, 4, 2]);
        // sub_path2: [3,4,2]
        let sub_path2 = build_simple_path(vec![3, 4, 2]);
        // concat project
        let project_opr_pb = build_project_path_concat(
            common_pb::path_concat::Endpoint::End,
            common_pb::path_concat::Endpoint::End,
        );
        // concat path: None
        let mut r1 = Record::new(sub_path1, Some(TAG_A.into()));
        r1.append(sub_path2, Some(TAG_B.into()));
        let mut result = project_test(vec![r1], project_opr_pb);
        assert!(result.next().is_none());
    }

    // a fail test case
    #[test]
    fn project_concat_allv_path_error_test() {
        // sub_path1: [1,2]
        let sub_path1 = build_path(vec![1, 2]);
        // sub_path2: [2,3]
        let sub_path2 = build_path(vec![2, 3]);
        // concat project, if concat sub_path1.start and sub_path2.start, it will fail
        let project_opr_pb = build_project_path_concat(
            common_pb::path_concat::Endpoint::Start,
            common_pb::path_concat::Endpoint::Start,
        );

        let mut r1 = Record::new(sub_path1, Some(TAG_A.into()));
        r1.append(sub_path2, Some(TAG_B.into()));
        let mut result = project_test(vec![r1], project_opr_pb);
        if let Some(res) = result.next() {
            assert!(res.is_err());
        }
    }

    fn init_path_record() -> Record {
        let vertex1 = init_vertex1();
        let vertex2 = init_vertex2();
        let mut path =
            GraphPath::new(vertex1, pb::path_expand::PathOpt::Arbitrary, pb::path_expand::ResultOpt::AllV)
                .unwrap();
        path.append(vertex2);
        Record::new(path, None)
    }

    fn to_path_func_pb(
        tag: Option<NameOrId>, path_key: common_pb::path_function::PathKey,
        opt: common_pb::path_function::FuncOpt,
    ) -> common_pb::PathFunction {
        common_pb::PathFunction {
            tag: tag.map(|t| t.into()),
            opt: opt as i32,
            node_type: None,
            path_key: Some(path_key),
        }
    }

    fn to_expr_path_func_pb(
        tag: Option<NameOrId>, path_key: common_pb::path_function::PathKey,
        opt: common_pb::path_function::FuncOpt,
    ) -> common_pb::Expression {
        common_pb::Expression {
            operators: vec![common_pb::ExprOpr {
                node_type: None,
                item: Some(common_pb::expr_opr::Item::PathFunc(to_path_func_pb(tag, path_key, opt))),
            }],
        }
    }

    // g.V().out(2..3).values('name')
    #[test]
    fn project_path_prop_test() {
        let path_key = common_pb::path_function::PathKey::Property(to_prop_pb("name".into()));
        let project_opr_pb = pb::Project {
            mappings: vec![pb::project::ExprAlias {
                expr: Some(to_expr_path_func_pb(None, path_key, common_pb::path_function::FuncOpt::Vertex)),
                alias: Some(TAG_A.into()),
            }],
            is_append: false,
        };
        let mut result = project_test(vec![init_path_record()], project_opr_pb);

        let mut object_result = Object::None;
        if let Some(Ok(res)) = result.next() {
            let a_entry = res.get(Some(TAG_A));
            object_result = a_entry.unwrap().as_object().unwrap().clone();
        }
        assert!(result.next().is_none());
        let expected_result = Object::Vector(vec![object!("marko"), object!("vadas")]);
        assert_eq!(object_result, expected_result);
    }

    // g.V().out(2..3).values('name','age')
    #[test]
    fn project_path_prop_vars_test() {
        let path_key = common_pb::path_function::PathKey::Vars(common_pb::path_function::PathElementKeys {
            keys: vec![to_prop_pb("name".into()), to_prop_pb("age".into())],
        });
        let project_opr_pb = pb::Project {
            mappings: vec![pb::project::ExprAlias {
                expr: Some(to_expr_path_func_pb(None, path_key, common_pb::path_function::FuncOpt::Vertex)),
                alias: Some(TAG_A.into()),
            }],
            is_append: false,
        };
        let mut result = project_test(vec![init_path_record()], project_opr_pb);
        let mut object_result = Object::None;
        if let Some(Ok(res)) = result.next() {
            let a_entry = res.get(Some(TAG_A));
            object_result = a_entry.unwrap().as_object().unwrap().clone();
        }
        assert!(result.next().is_none());
        let expected_result = Object::Vector(vec![
            Object::Vector(vec![object!("marko"), object!(29)]),
            Object::Vector(vec![object!("vadas"), object!(27)]),
        ]);
        assert_eq!(object_result, expected_result);
    }

    // g.V().out(2..3).valueMap('name','age')
    #[test]
    fn project_path_prop_map_test() {
        let path_key =
            common_pb::path_function::PathKey::Map(common_pb::path_function::PathElementKeyValues {
                key_vals: vec![
                    common_pb::path_function::path_element_key_values::PathElementKeyValue {
                        key: Some("name".to_string().into()),
                        val: Some(to_prop_pb("name".into())),
                    },
                    common_pb::path_function::path_element_key_values::PathElementKeyValue {
                        key: Some("age".to_string().into()),
                        val: Some(to_prop_pb("age".into())),
                    },
                ],
            });
        let project_opr_pb = pb::Project {
            mappings: vec![pb::project::ExprAlias {
                expr: Some(to_expr_path_func_pb(None, path_key, common_pb::path_function::FuncOpt::Vertex)),
                alias: Some(TAG_A.into()),
            }],
            is_append: false,
        };
        let mut result = project_test(vec![init_path_record()], project_opr_pb);

        let mut object_result = Object::None;
        if let Some(Ok(res)) = result.next() {
            let a_entry = res.get(Some(TAG_A));
            object_result = a_entry.unwrap().as_object().unwrap().clone();
        }
        assert!(result.next().is_none());
        let expected_result = Object::Vector(vec![
            Object::KV(
                vec![(object!("name"), object!("marko")), (object!("age"), object!(29))]
                    .into_iter()
                    .collect(),
            ),
            Object::KV(
                vec![(object!("name"), object!("vadas")), (object!("age"), object!(27))]
                    .into_iter()
                    .collect(),
            ),
        ]);
        assert_eq!(object_result, expected_result);
    }

    // g.V().out(2..3).elementMap('name')
    #[test]
    fn project_path_elem_prop_map_test() {
        let path_key =
            common_pb::path_function::PathKey::Map(common_pb::path_function::PathElementKeyValues {
                key_vals: vec![
                    common_pb::path_function::path_element_key_values::PathElementKeyValue {
                        key: Some("~id".to_string().into()),
                        val: Some(common_pb::Property {
                            item: Some(common_pb::property::Item::Id(common_pb::IdKey {})),
                        }),
                    },
                    common_pb::path_function::path_element_key_values::PathElementKeyValue {
                        key: Some("~label".to_string().into()),
                        val: Some(common_pb::Property {
                            item: Some(common_pb::property::Item::Label(common_pb::LabelKey {})),
                        }),
                    },
                    common_pb::path_function::path_element_key_values::PathElementKeyValue {
                        key: Some("name".to_string().into()),
                        val: Some(to_prop_pb("name".into())),
                    },
                ],
            });
        let project_opr_pb = pb::Project {
            mappings: vec![pb::project::ExprAlias {
                expr: Some(to_expr_path_func_pb(None, path_key, common_pb::path_function::FuncOpt::Vertex)),
                alias: Some(TAG_A.into()),
            }],
            is_append: false,
        };
        let mut result = project_test(vec![init_path_record()], project_opr_pb);

        let mut object_result = Object::None;
        if let Some(Ok(res)) = result.next() {
            let a_entry = res.get(Some(TAG_A));
            object_result = a_entry.unwrap().as_object().unwrap().clone();
        }
        assert!(result.next().is_none());
        let expected_result = Object::Vector(vec![
            Object::KV(
                vec![
                    (object!("~id"), object!(1)),
                    (object!("~label"), object!(PERSON_LABEL)),
                    (object!("name"), object!("marko")),
                ]
                .into_iter()
                .collect(),
            ),
            Object::KV(
                vec![
                    (object!("~id"), object!(2)),
                    (object!("~label"), object!(PERSON_LABEL)),
                    (object!("name"), object!("vadas")),
                ]
                .into_iter()
                .collect(),
            ),
        ]);
        assert_eq!(object_result, expected_result);
    }
}
