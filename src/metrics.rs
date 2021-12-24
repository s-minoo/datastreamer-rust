use std::collections::HashMap;
use std::fs::File;
use std::io::Write;
use std::sync::Arc;
use std::{io::BufWriter, sync::Mutex};

use metrics::{Key, Recorder};

type KeyOutputMap = Mutex<HashMap<Key, BufWriter<File>>>;

pub struct FileRecorder {
    key_output_map: Arc<KeyOutputMap>,
}

impl FileRecorder {
    pub fn new() -> Self {
        std::fs::create_dir("log/").unwrap();
        let key_output_map = Arc::new(Mutex::new(HashMap::new()));
        Self { key_output_map }
    }
}

impl Drop for FileRecorder {
    fn drop(&mut self) {
        let mut map = self.key_output_map.lock().unwrap();
        for writer in map.values_mut() {
            writer.flush().unwrap();
        }
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
        let writer = init_file(key);
        let mut map = self.key_output_map.lock().unwrap();
        map.insert(key.clone(), writer);
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
        let buf = format!("{:?}", value);

        let mut map = self.key_output_map.lock().unwrap();
        match map.get_mut(key) {
            Some(writer) => {
                writer.write_all(buf.as_bytes()).unwrap();
            }
            None => {
                let mut writer = init_file(key); 
                writer.write_all(buf.as_bytes()).unwrap();
                map.insert(key.clone(), writer); 
            }
        }
    }

    fn record_histogram(&self, _key: &Key, _value: f64) {
        todo!()
    }
}

fn init_file(key: &Key) -> BufWriter<File> {
    let file = File::create(key.name()).unwrap();
    let mut writer = BufWriter::new(file);
    let header = "throughput (msg/s)".as_bytes();
    writer.write_all(header).unwrap();
    writer
}
