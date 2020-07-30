//
//! Copyright 2020 Alibaba Group Holding Limited.
//! 
//! Licensed under the Apache License, Version 2.0 (the "License");
//! you may not use this file except in compliance with the License.
//! You may obtain a copy of the License at
//! 
//!     http://www.apache.org/licenses/LICENSE-2.0
//! 
//! Unless required by applicable law or agreed to in writing, software
//! distributed under the License is distributed on an "AS IS" BASIS,
//! WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//! See the License for the specific language governing permissions and
//! limitations under the License.

use std::env;
use super::fs;
use log4rs;
use log4rs::file::Deserializers;
use log4rs::file::Deserialize;
use log4rs::append::Append;
use log4rs::append::rolling_file::*;
use std::error::Error;
use log4rs::encode::EncoderConfig;
use serde_value::Value;
use serde;
use regex::Regex;
use std::collections::{HashMap, BTreeMap};
use regex::Captures;
use std::path::PathBuf;


/// Init log4rs configuration from exe_dir/../conf/log4rs.yml or ./log4rs.yml.
/// Call it once on process startup.
#[allow(dead_code)]
pub fn init_log4rs() {
    let exe = env::current_exe().unwrap();
    let conf_dir = exe.parent().unwrap().parent().unwrap().join("conf");
    let mut log4rs_config = conf_dir.clone();
    log4rs_config.push("log4rs.yml");

    let deser = get_deserializer();
    if fs::exists(log4rs_config.as_path()) {
        log4rs::init_file(log4rs_config.as_path(), deser)
            .expect("init log4rs from ./conf/log4rs.yml failed");
    } else {
        if let Ok(p) = env::var("CONF_DIR") {
            let mut log4rs_config = PathBuf::from(p);
            log4rs_config.push("log4rs.yml");
            log4rs::init_file(log4rs_config.as_path(), deser)
                .expect("init log4rs from ./conf/log4rs.yml failed");
        } else {
            log4rs::init_file("log4rs.yml", deser)
                .expect("init log4rs from ./log4rs.yml failed");
        }
    }
}


pub(crate) fn get_deserializer() -> Deserializers {
    let mut deser = Deserializers::new();
    deser.insert("env_rolling_file", EnvRollingFileAppenderDeserializer);
    deser
}

/// Configuration for the env rolling file appender.
/// Copied from log4rs's `RollingFileAppenderConfig` for visibility reasons.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct EnvRollingFileAppenderConfig {
    path: String,
    append: Option<bool>,
    encoder: Option<EncoderConfig>,
    policy: Policy,
}

struct Policy {
    kind: String,
    pub config: Value,
}

impl<'de> serde::Deserialize<'de> for Policy {
    fn deserialize<D>(d: D) -> Result<Policy, D::Error>
        where
            D: serde::Deserializer<'de>,
    {
        let mut map = BTreeMap::<Value, Value>::deserialize(d)?;

        let kind = match map.remove(&Value::String("kind".to_owned())) {
            Some(kind) => kind.deserialize_into().map_err(|e| e.to_error())?,
            None => "compound".to_owned(),
        };
        let mut config = Value::Map(map);
        let env_vars: HashMap<String, String> = ::std::env::vars().collect();
        substitute_value(&mut config, &env_vars);
        Ok(Policy {
            kind,
            config,
        })
    }
}

struct EnvRollingFileAppenderDeserializer;

impl Deserialize for EnvRollingFileAppenderDeserializer {
    type Trait = dyn Append;

    type Config = EnvRollingFileAppenderConfig;

    fn deserialize(&self,
                   config: EnvRollingFileAppenderConfig,
                   deserializers: &Deserializers)
                   -> Result<Box<dyn Append>, Box<dyn Error + Sync + Send>> {
        let env_vars: HashMap<String, String> = ::std::env::vars().collect();

        let mut builder = RollingFileAppender::builder();
        if let Some(append) = config.append {
            builder = builder.append(append);
        }
        if let Some(mut encoder) = config.encoder {
            substitute_value(&mut encoder.config, &env_vars);
            let encoder = deserializers.deserialize(&encoder.kind, encoder.config)?;
            builder = builder.encoder(encoder);
        }
        let policy = deserializers.deserialize(&config.policy.kind, config.policy.config)?;
        let path = substitute_string(config.path.as_str(), &env_vars);
        let appender = builder.build(path, policy)?;
        Ok(Box::new(appender))
    }
}


/// Replace a string containing property placeholder with given key-value dictionary.
/// Placeholder follows syntax of log4j, which can be:
///   - `${env:ENV_NAME}` will be replaced with value of the `ENV_NAME` key in dictionary.
///   - `${env:ENV_NAME:-default_value}` will be replaced with value of the `ENV_NAME` key in dictionary
/// if present, otherwise `default_value` will be used.
fn substitute_string(input: &str, replacement: &HashMap<String, String>) -> String {
    let re = Regex::new(r"\$\{env:(?P<env>[^=:]+)(:-(?P<default>[^}]+))?}").unwrap();
    let res = re.replace_all(input, |caps: &Captures| {
        let env = caps.name("env").map_or("", |m| m.as_str());
        let default_value = caps.name("default").map_or("", |m| m.as_str());
        match replacement.get(env) {
            Some(env_value) => format!("{}", env_value),
            None => default_value.to_owned()
        }
    });
    (*res).to_owned()
}

/// Substitute strings in a `Value` with variables recursively.
fn substitute_value(value: &mut Value, replacement: &HashMap<String, String>) {
    match value {
        Value::String(ref mut v) => *v = substitute_string(v, &replacement),
        Value::Option(ref mut v) => if v.is_some() { substitute_value(v.as_mut().unwrap(), &replacement); }
        Value::Newtype(v) => substitute_value(v.as_mut(), &replacement),
        Value::Seq(value_vec) => value_vec.iter_mut().for_each(|v| substitute_value(v, &replacement)),
        Value::Map(value_map) => value_map.iter_mut().for_each(|(_k, v)| substitute_value(v, &replacement)),
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use log4rs::file::RawConfig;

    #[test]
    fn test_regex() {
        let mut replacement = HashMap::new();
        replacement.insert("LOG_PATH".to_owned(), "/tmp/log".to_owned());
        replacement.insert("WORKER_ID".to_owned(), "876".to_owned());
        let result = substitute_string("log_dir: ${env:LOG_PATH:-/home/admin/log}/${env:WORKER_ID:--1}/my.log", &replacement);
        assert_eq!(result, "log_dir: /tmp/log/876/my.log");

        let mut replacement = HashMap::new();
        replacement.insert("WORKER_ID".to_owned(), "876".to_owned());
        let result = substitute_string("log_dir: ${env:LOG_PATH:-/home/admin/log}/${env:WORKER_ID:--1}/my.log", &replacement);
        assert_eq!(result, "log_dir: /home/admin/log/876/my.log");

        let mut replacement = HashMap::new();
        replacement.insert("LOG_PATH".to_owned(), "/tmp/log".to_owned());
        let result = substitute_string("log_dir: ${env:LOG_PATH:-/home/admin/log}/${env:WORKER_ID:--1}/my.log", &replacement);
        assert_eq!(result, "log_dir: /tmp/log/-1/my.log");
    }

    #[test]
    fn test_substitute_value() {
        let mut replacement = HashMap::new();
        replacement.insert("LOG_PATH".to_owned(), "/tmp/log".to_owned());

        let mut value = Value::String("log_dir: ${env:LOG_PATH}/${env:WORKER_ID:--1}/my.log".to_owned());
        substitute_value(&mut value, &replacement);
        assert_eq!(value, Value::String("log_dir: /tmp/log/-1/my.log".to_owned()));

        let mut value = Value::Option(None);
        substitute_value(&mut value, &replacement);
        assert_eq!(value, Value::Option(None));

        let mut value = Value::Option(Some(Box::new(Value::String("log_dir: ${env:LOG_PATH}/${env:WORKER_ID:--1}/my.log".to_owned()))));
        substitute_value(&mut value, &replacement);
        assert_eq!(value,
                   Value::Option(Some(Box::new(Value::String("log_dir: /tmp/log/-1/my.log".to_owned())))));

        let mut value = Value::Newtype(Box::new(Value::String("log_dir: ${env:LOG_PATH}/${env:WORKER_ID:--1}/my.log".to_owned())));
        substitute_value(&mut value, &replacement);
        assert_eq!(value,
                   Value::Newtype(Box::new(Value::String("log_dir: /tmp/log/-1/my.log".to_owned()))));

        let mut value = Value::Seq(vec![
            Value::String("log_dir: ${env:LOG_PATH}/${env:WORKER_ID:--1}/my.log".to_owned()),
            Value::String("log_dir: ${env:LOG_PATH}/${env:WORKER_ID:--1}/your.log".to_owned())
        ]);
        substitute_value(&mut value, &replacement);
        assert_eq!(value, Value::Seq(vec![
            Value::String("log_dir: /tmp/log/-1/my.log".to_owned()),
            Value::String("log_dir: /tmp/log/-1/your.log".to_owned())
        ]));


        let mut value = Value::Map({
            let mut map = BTreeMap::new();
            map.insert(Value::U8(1), Value::String("log_dir: ${env:LOG_PATH}/${env:WORKER_ID:--1}/my.log".to_owned()));
            map.insert(Value::U8(2), Value::String("log_dir: ${env:LOG_PATH}/${env:WORKER_ID:--1}/your.log".to_owned()));
            map
        });

        substitute_value(&mut value, &replacement);
        assert_eq!(value, Value::Map({
            let mut map = BTreeMap::new();
            map.insert(Value::U8(1), Value::String("log_dir: /tmp/log/-1/my.log".to_owned()));
            map.insert(Value::U8(2), Value::String("log_dir: /tmp/log/-1/your.log".to_owned()));
            map
        }));
    }


    #[test]
    fn test_deser() {
        let config_str = r#"
appenders:
  file:
    kind: env_rolling_file
    append: true
    path: "${env:TEST_LOG_DIR:-/Users/xiafei/tmp}/xxx.log"
    encoder:
      pattern: "{d(%Y-%m-%d %H:%M:%S)} {h({l:<5})} [{f}:{L}] [TEST_LOG_TAG] {m}{n}"
    policy:
      trigger:
        kind: size
        limit: 4mb
      roller:
        kind: fixed_window
        pattern: "${env:TEST_LOG_DIR:-/Users/xiafei/tmp}/xxx.log.archive.{}"
        count: 3


root:
  level: debug
  appenders:
    - file
         "#;
        ::std::env::set_var("TEST_LOG_DIR", "/tmp");
        ::std::env::set_var("TEST_LOG_TAG", "UT");
        let deser = get_deserializer();
        let config = ::serde_yaml::from_str::<RawConfig>(config_str).unwrap();
        let (appenders, errors) = config.appenders_lossy(&deser);
        assert!(errors.is_empty());
        assert_eq!(appenders.len(), 1);
        let appender = &appenders[0];
        assert_eq!(appender.name(), "file");

        // no valid accessors, I have to check displayed string.
        let appender_str = format!("{:?}", appender);
        assert!(appender_str.contains("\"/tmp/xxx.log\""));
        assert!(appender_str.contains("\"/tmp/xxx.log.archive.{}\""));
        assert!(appender_str.contains("\"{d(%Y-%m-%d %H:%M:%S)} {h({l:<5})} [{f}:{L}] [TEST_LOG_TAG] {m}{n}\""));
    }
}
