use std::collections::HashMap;
use std::fmt::Display;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::str::FromStr;
use std::sync::{Arc, MutexGuard};
use std::{io::BufWriter, sync::Mutex};

use itertools::Itertools;
use log::debug;
use metrics::{Key, Recorder};

#[derive(Debug)]
pub struct MetricErr(String, String);

pub enum Action {
    Flush,
    Write,
}

impl FromStr for Action {
    type Err = MetricErr;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "flush" => Ok(Action::Flush),
            "write" => Ok(Action::Write),
            _ => Err(MetricErr("Action".into(), s.into())),
        }
    }
}

impl Display for Action {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Action::Flush => f.write_fmt(format_args!("{}", "flush")),
            Action::Write => f.write_fmt(format_args!("{}", "write")),
        }
    }
}

pub enum Label {
    Action,
    Duration,
}

impl FromStr for Label {
    type Err = MetricErr;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "action" => Ok(Label::Action),
            "duration" => Ok(Label::Duration),
            _ => Err(MetricErr("Label".into(), s.into())),
        }
    }
}

impl Display for Label {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Label::Action => f.write_fmt(format_args!("{}", "action")),
            Label::Duration => f.write_fmt(format_args!("{}", "duration")),
        }
    }
}

type KeyOutputMap = HashMap<String, BufWriter<File>>;

pub struct FileRecorder {
    key_output_map: Arc<Mutex<KeyOutputMap>>,
}

impl Drop for FileRecorder {
    fn drop(&mut self) {
        debug!("FileRecorder dropped!");
        let mut map = self.key_output_map.lock().unwrap();
        for writer in map.values_mut() {
            writer.flush().unwrap();
        }
    }
}

impl FileRecorder {
    pub fn new() -> Self {
        std::fs::create_dir("log/").ok();
        let key_output_map = Arc::new(Mutex::new(HashMap::new()));
        Self { key_output_map }
    }
}

impl Default for FileRecorder {
    fn default() -> Self {
        Self::new()
    }
}

impl Recorder for FileRecorder {
    fn register_counter(
        &self,
        _key: &Key,
        _unit: Option<metrics::Unit>,
        _description: Option<&'static str>,
    ) {
        todo!()
    }

    fn register_gauge(
        &self,
        key: &Key,
        _unit: Option<metrics::Unit>,
        _description: Option<&'static str>,
    ) {
        debug!("{}", key.name());
        let writer = init_file(key);
        let mut map = self.key_output_map.lock().unwrap();
        map.insert(key.name().to_string(), writer);
    }

    fn register_histogram(
        &self,
        _key: &Key,
        _unit: Option<metrics::Unit>,
        _description: Option<&'static str>,
    ) {
        todo!()
    }

    fn increment_counter(&self, _key: &Key, _value: u64) {
        todo!()
    }

    fn update_gauge(&self, key: &Key, value: metrics::GaugeValue) {
        if let metrics::GaugeValue::Absolute(val) = value {
            let labels_map = key.labels().map(|l| (l.key(), l.value())).into_group_map();

            for (label, action_vec) in labels_map {
                let mut map = self.key_output_map.lock().unwrap();
                match label {
                    "action" => {
                        action_vec.iter().for_each(|action| {
                            handle_action(
                                &mut map,
                                key,
                                Action::from_str(*action).unwrap(),
                                Some(val),
                            )
                            .unwrap();
                        });
                    }

                    _ => {
                        continue;
                    }
                }
            }
        }
    }

    fn record_histogram(&self, _key: &Key, _value: f64) {
        todo!()
    }
}

fn handle_action<T: ToString>(
    map: &mut MutexGuard<KeyOutputMap>,
    key: &Key,
    action: Action,
    opt_value: Option<T>,
) -> Result<(), std::io::Error> {
    debug!("{:?}", map.keys().collect_vec());
    debug!("{}", key.name());

    let writer = match map.get_mut(&key.name().to_string()) {
        Some(writer) => writer,
        None => {
            init_file(key);
            map.get_mut(&key.name().to_string()).unwrap()
        }
    };

    match action {
        Action::Flush => {
            writer.flush()?;
            Ok(())
        }
        Action::Write => {
            if let Some(val) = opt_value {
                let buf = format!("{}\n", val.to_string());
                writer.write(buf.as_bytes()).map(|_| ())
            } else {
                Ok(())
            }
        }
    }
}
fn init_file(key: &Key) -> BufWriter<File> {
    debug!("Init-file called!");
    let file = OpenOptions::new()
        .write(true)
        .append(true)
        .create(true)
        .open(key.name())
        .unwrap();
    let mut writer = BufWriter::new(file);
    let header = "throughput (msg/s)\n".as_bytes();
    writer.write_all(header).unwrap();
    writer
}
