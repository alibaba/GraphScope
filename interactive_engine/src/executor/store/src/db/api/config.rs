#![allow(dead_code)]
use std::collections::HashMap;

#[derive(Debug, Clone, Default)]
pub struct GraphConfig {
    storage_engine: String,
    storage_options: HashMap<String, String>,
}

impl GraphConfig {
    pub fn get_storage_engine(&self) -> &str {
        self.storage_engine.as_str()
    }

    pub fn get_storage_options(&self) -> &HashMap<String, String> {
        &self.storage_options
    }

    pub fn get_storage_option(&self, k: &str) -> Option<&String> {
        self.storage_options.get(k)
    }
}

pub struct GraphConfigBuilder {
    config: GraphConfig,
}

impl GraphConfigBuilder {
    pub fn new() -> Self {
        GraphConfigBuilder {
            config: Default::default(),
        }
    }

    pub fn set_storage_engine(&mut self, engine: &str) -> &mut Self {
        self.config.storage_engine = engine.to_owned();
        self
    }

    pub fn add_storage_option(&mut self, key: &str, val: &str) -> &mut Self {
        self.config.storage_options.insert(key.to_owned(), val.to_owned());
        self
    }

    pub fn set_storage_options(&mut self, options: HashMap<String, String>) -> &mut Self {
        self.config.storage_options = options;
        self
    }

    pub fn build(self) -> GraphConfig {
        self.config
    }
}
