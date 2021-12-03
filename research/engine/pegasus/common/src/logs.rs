//
//! Copyright 2020 Alibaba Group Holding Limited.
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

#[cfg(feature = "log4rs")]
pub fn init_log() {
    log_4_rs::init_log();
}

#[cfg(all(not(feature = "log4rs"), feature = "env_logger"))]
pub fn init_log() {
    log_env::init_log();
}

#[cfg(all(not(feature = "log4rs"), not(feature = "env_logger")))]
pub fn init_log() {
    panic!("can't init logging;");
}

#[cfg(features = "log4rs")]
mod log_4_rs {
    use log::LevelFilter;
    use log4rs::append::console::{ConsoleAppender, Target};
    use log4rs::append::file::FileAppender;
    use log4rs::config::{Appender, Config, Logger, Root};
    use log4rs::encode::pattern::PatternEncoder;
    use log4rs::filter::threshold::ThresholdFilter;

    pub fn init_log() {
        if let Err(err) = log4rs::init_file("log4rs.yml", Default::default()) {
            eprintln!("init logger failure : {:?}", err);
            let stderr = ConsoleAppender::builder()
                .target(Target::Stderr)
                .build();
            let exe_name = std::env::current_exe()
                .expect("can't get exec path")
                .file_name()
                .expect("can't get exec name")
                .to_string_lossy()
                .into_owned();
            let log_file = format!("/tmp/{}.log", exe_name);
            let appender = FileAppender::builder()
                .encoder(Box::new(PatternEncoder::new(
                    "{d(%Y-%m-%d %H:%M:%S.%f)} {h({l:<5})} (({f}:{L})) [{T}] {m}{n}",
                )))
                .append(false)
                .build(log_file)
                .expect("init file appender failure");

            let config = Config::builder()
                .appender(Appender::builder().build("logfile", Box::new(appender)))
                .appender(
                    Appender::builder()
                        .filter(Box::new(ThresholdFilter::new(LevelFilter::Error)))
                        .build("stderr", Box::new(stderr)),
                )
                //.logger(Logger::builder().appender("logfile").additive(false).build("app::requests", LevelFilter::Debug))
                .build(
                    Root::builder()
                        .appender("logfile")
                        .appender("stderr")
                        .build(LevelFilter::Debug),
                )
                .expect("init default logger failure");

            log4rs::init_config(config).expect("init default logger failure");
        }
    }
}

#[cfg(feature = "env_logger")]
mod log_env {
    use std::io::Write;

    use env_logger::fmt::Color;
    use log::Level;

    pub fn init_log() {
        env_logger::Builder::from_default_env()
            .format(|buf, record| {
                let t = time::now();
                let mut level_style = buf.style();
                match record.level() {
                    Level::Error => {
                        level_style.set_color(Color::Red).set_bold(true);
                    }
                    Level::Warn => {
                        level_style
                            .set_color(Color::Yellow)
                            .set_bold(true);
                    }
                    Level::Info => {
                        level_style
                            .set_color(Color::Green)
                            .set_bold(false);
                    }
                    Level::Debug => {
                        level_style.set_color(Color::White);
                    }
                    Level::Trace => {
                        level_style.set_color(Color::Blue);
                    }
                };

                writeln!(
                    buf,
                    "{},{:03} {}\t[{}] [{}:{}] {}",
                    time::strftime("%Y-%m-%d %H:%M:%S", &t).unwrap(),
                    t.tm_nsec / 1000_000,
                    level_style.value(record.level()),
                    ::std::thread::current()
                        .name()
                        .unwrap_or("unknown"),
                    record.file().unwrap_or(""),
                    record.line().unwrap_or(0),
                    record.args()
                )
            })
            .try_init()
            .ok();
    }
}
