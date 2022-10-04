use futures_util::future::join_all;
use serde::Deserialize;
use tokio::fs::File;
use tokio::io::BufReader;
use walkdir::WalkDir;

#[derive(Debug, Deserialize, Copy, Clone)]
pub enum Mode {
    Constant,
    Periodic,
}

#[derive(Debug, Deserialize, Copy, Clone)]
pub enum DataFmt {
    JSON,
    XML,
    CSV,
}

#[derive(Debug, Deserialize)]
pub struct Config {
    pub streamconfigs: Vec<StreamConfig>,
    pub log_level: Option<String>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct StreamConfig {
    pub ip: String,
    #[serde(default = "default_port")]
    pub port: u16,
    pub mode: Mode,
    #[serde(default = "default_volume")]
    pub volume: u32,
    #[serde(default = "default_interval_ms")]
    pub interval_ms: u32,
    #[serde(default = "default_period_ms")]
    pub calm_period_ms: u32,
    pub output_format: DataFmt,
    #[serde(default = "default_input_fmt")]
    pub input_format: DataFmt,
    pub data_folder: Option<String>,
}

fn default_input_fmt() -> DataFmt {
    DataFmt::JSON
}
fn default_volume() -> u32 {
    1
}
fn default_interval_ms() -> u32 {
    400
}
fn default_period_ms() -> u32 {
    400
}
fn default_port() -> u16 {
    9000
}

pub fn get_output(config: &StreamConfig) -> String {
    let id = get_id(config);
    format!("log/{}.data.log.csv", id)
}
pub fn get_id(config: &StreamConfig) -> String {
    format!("{}_{}_{:?}", config.ip, config.port, config.mode)
}

pub async fn create_file_buffers(data_root: &str) -> Vec<BufReader<File>> {
    let future_buffers: Vec<_> = join_all(recurs_get_files(data_root))
        .await
        .into_iter()
        .map(|f| f.unwrap())
        .map(BufReader::new)
        .collect();
    future_buffers
}

fn recurs_get_files(
    data_root_dir: &str,
) -> Vec<impl futures_util::Future<Output = std::result::Result<File, std::io::Error>>> {
    WalkDir::new(data_root_dir)
        .sort_by_file_name()
        .into_iter()
        .filter_map(|f| f.ok())
        .filter(|f| f.file_type().is_file())
        .map(|f| f.into_path())
        .map(File::open)
        .collect()
}
