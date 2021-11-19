use ::rand::prelude::*;
use crate::db::api::*;
use super::types;
use super::data;
use super::util::*;
use self::benchmark::*;
use crate::db::api::multi_version_graph::MultiVersionGraph;

pub fn bench_insert_vertex<G: MultiVersionGraph>(graph: G) {
    let benchmark = InsertVertexBenchmark::new(graph);
    benchmark.execute();
}

pub fn bench_insert_edge<G: MultiVersionGraph>(graph: G) {
    let benchmark = InsertEdgeBenchmark::new(graph);
    benchmark.execute();
}

mod benchmark {
    use super::*;
    use std::collections::HashMap;

    pub struct InsertVertexBenchmark<G: MultiVersionGraph> {
        graph: G,
    }

    impl<G: MultiVersionGraph> InsertVertexBenchmark<G> {
        pub fn new(graph: G) -> Self {
            InsertVertexBenchmark {
                graph,
            }
        }

        pub fn execute(&self) {
            self.one_string_property_bench();
            self.one_numeric_property_bench();
            self.multi_numeric_properties_bench();
            self.multi_string_properties_bench();
        }

        fn one_string_property_bench(&self) {
            println!("=== benchmark of one string property ===");
            println!("string length\trps");
            let type_def = types::create_one_property_type_def(ValueType::String);
            let mut schema_version = 1;
            // length from 128B to 4KB
            for value_len in 7..=12 {
                let label = value_len as LabelId;
                self.graph.create_vertex_type(1, schema_version, label, &type_def, schema_version).unwrap();
                schema_version += 1;
                let len = 1<<value_len;
                let properties = data::gen_one_string_properties(&type_def, len);

                let test_count = 100000;
                let rps = self.bench(label, &properties, test_count);
                println!("{:8}\t{:.2}", len, rps);
            }
            println!("========================================");
        }

        fn one_numeric_property_bench(&self) {
            println!("=== benchmark of one numeric property ===");
            println!("type\trps");
            let mut label = 100;
            let mut schema_version = 1;
            for r#type in vec![ValueType::Int, ValueType::Long, ValueType::Float, ValueType::Double] {
                label += 1;
                let type_def = types::create_one_property_type_def(r#type);
                self.graph.create_vertex_type(1, schema_version, label, &type_def, schema_version).unwrap();
                schema_version += 1;
                let properties = data::gen_properties(&type_def);

                let test_count = 100000;
                let rps = self.bench(label, &properties, test_count);
                println!("{:?}\t{:.2}", r#type, rps);
            }
            println!("========================================");
        }

        fn multi_numeric_properties_bench(&self) {
            println!("=== benchmark of multi numeric properties ===");
            println!("properties count\trps");
            let mut label = 200;
            let mut schema_version = 1;
            for count in vec![5, 10, 20, 50, 100, 200] {
                label += 1;
                let type_def = types::multi_numeric_properties_type_def(count);
                self.graph.create_vertex_type(1, schema_version, label, &type_def, schema_version).unwrap();
                schema_version += 1;
                let properties = data::gen_properties(&type_def);

                let test_count = 100000;
                let rps = self.bench(label, &properties, test_count);
                println!("{:8}\t{:.2}", count, rps);
            }
            println!("========================================");
        }

        fn multi_string_properties_bench(&self) {
            println!("=== benchmark of multi string properties ===");
            println!("properties count\trps");
            let mut label = 300;
            let mut schema_version = 1;
            for count in vec![5, 10, 20, 50, 100, 200] {
                label += 1;
                let type_def = types::multi_string_properties_type_def(count);
                self.graph.create_vertex_type(1, schema_version, label, &type_def, schema_version).unwrap();
                schema_version += 1;
                let properties = data::gen_properties(&type_def);

                let test_count = 100000;
                let rps = self.bench(label, &properties, test_count);
                println!("{:8}\t{:.2}", count, rps);
            }
            println!("========================================");
        }

        fn bench(&self, label: LabelId, properties: &HashMap<PropertyId, Value>, test_count: usize) -> f64 {
            // warmup
            for _ in 0..test_count {
                let id = random();
                self.graph.insert_overwrite_vertex(1, id, label, properties).unwrap();
            }

            let timer = Timer::new();
            for _ in 0..test_count {
                let id = random();
                self.graph.insert_overwrite_vertex(1, id, label, properties).unwrap();
            }
            let cost = timer.elapsed_secs();
            test_count as f64 / cost
        }
    }

    pub struct InsertEdgeBenchmark<G: MultiVersionGraph> {
        graph: G,
    }

    impl<G: MultiVersionGraph> InsertEdgeBenchmark<G> {
        pub fn new(graph: G) -> Self {
            InsertEdgeBenchmark {
                graph,
            }
        }

        pub fn execute(&self) {
            self.one_string_property_bench();
            self.one_numeric_property_bench();
            self.multi_numeric_properties_bench();
            self.multi_string_properties_bench();
        }

        fn one_string_property_bench(&self) {
            println!("=== benchmark of one string property ===");
            println!("string length\trps");
            let type_def = types::create_one_property_type_def(ValueType::String);
            // length from 128B to 4KB
            let mut schema_version = 1;
            for value_len in 7..=12 {
                let label = value_len as LabelId;
                let edge_type = EdgeKind::new(label, label+1, label+2);
                self.graph.create_edge_type(1, schema_version, label, &type_def).unwrap();
                schema_version += 1;
                self.graph.add_edge_kind(1, schema_version, &edge_type, schema_version).unwrap();
                schema_version += 1;

                let len = 1<<value_len;
                let properties = data::gen_one_string_properties(&type_def, len);

                let test_count = 100000;
                let rps = self.bench(&edge_type, &properties, test_count);
                println!("{:8}\t{:.2}", len, rps);
            }
            println!("========================================");
        }

        fn one_numeric_property_bench(&self) {
            println!("=== benchmark of one numeric property ===");
            println!("type\trps");
            let mut label = 100;
            let mut schema_version = 1;
            for r#type in vec![ValueType::Int, ValueType::Long, ValueType::Float, ValueType::Double] {
                label += 1;
                let type_def = types::create_one_property_type_def(r#type);
                let edge_type = EdgeKind::new(label, label+1, label+2);
                self.graph.create_edge_type(1, schema_version, label, &type_def).unwrap();
                schema_version += 1;
                self.graph.add_edge_kind(1, schema_version, &edge_type, schema_version).unwrap();
                schema_version += 1;
                let properties = data::gen_properties(&type_def);

                let test_count = 100000;
                let rps = self.bench(&edge_type, &properties, test_count);
                println!("{:?}\t{:.2}", r#type, rps);
            }
            println!("========================================");
        }

        fn multi_numeric_properties_bench(&self) {
            println!("=== benchmark of multi numeric properties ===");
            println!("properties count\trps");
            let mut label = 200;
            let mut schema_version = 1;
            for count in vec![5, 10, 20, 50, 100, 200] {
                label += 1;
                let type_def = types::multi_numeric_properties_type_def(count);
                let edge_type = EdgeKind::new(label, label+1, label+2);
                self.graph.create_edge_type(1, schema_version, label, &type_def).unwrap();
                schema_version += 1;
                self.graph.add_edge_kind(1, schema_version, &edge_type, schema_version).unwrap();
                schema_version += 1;
                let properties = data::gen_properties(&type_def);

                let test_count = 100000;
                let rps = self.bench(&edge_type, &properties, test_count);
                println!("{:8}\t{:.2}", count, rps);
            }
            println!("========================================");
        }

        fn multi_string_properties_bench(&self) {
            println!("=== benchmark of multi string properties ===");
            println!("properties count\trps");
            let mut label = 300;
            let mut schema_version = 1;
            for count in vec![5, 10, 20, 50, 100, 200] {
                label += 1;
                let type_def = types::multi_string_properties_type_def(count);
                let edge_type = EdgeKind::new(label, label+1, label+2);
                self.graph.create_edge_type(1,  schema_version, label, &type_def).unwrap();
                schema_version += 1;
                self.graph.add_edge_kind(1, schema_version,  &edge_type, schema_version).unwrap();
                schema_version += 1;
                let properties = data::gen_properties(&type_def);

                let test_count = 100000;
                let rps = self.bench(&edge_type, &properties, test_count);
                println!("{:8}\t{:.2}", count, rps);
            }
            println!("========================================");
        }

        fn bench(&self, edge_type: &EdgeKind, properties: &HashMap<PropertyId, Value>, test_count: usize) -> f64 {
            // warmup
            self.insert(edge_type, properties, test_count);

            let timer = Timer::new();
            self.insert(edge_type, properties, test_count);
            let cost = timer.elapsed_secs();
            test_count as f64 / cost
        }

        fn insert(&self, edge_type: &EdgeKind, properties: &HashMap<PropertyId, Value>, mut test_count: usize) {
            let mut inner_id = 0;
            while test_count > 0 {
                let src_id = random();
                let mut count = random::<i64>() % 10 + 1;
                if count > test_count as i64 {
                    count = test_count as i64;
                }
                for dst_id in src_id+1..=src_id+count {
                    inner_id += 1;
                    let id = EdgeId::new(src_id, dst_id, inner_id);
                    self.graph.insert_overwrite_edge(1, id, edge_type, true, properties).unwrap();
                }
                test_count -= count as usize;
            }
        }
    }
}

