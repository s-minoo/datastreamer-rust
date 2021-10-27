use futures_util::future::join_all;
use serde::Deserialize;
use tokio::fs::File;
use tokio::io::{AsyncBufReadExt, BufReader};
use walkdir::WalkDir;

#[derive(Debug, Deserialize)]
pub enum Mode {
    Constant,
    Periodic,
}

#[derive(Debug, Deserialize)]
pub struct Config {
    pub streamconfigs: Vec<StreamConfig>,
    pub log_level: Option<&'static str>,
}

#[derive(Deserialize, Debug)]
pub struct StreamConfig {
    pub ip: String,
    pub port: Option<u16>,
    pub mode: Mode,
    pub calm_period_ms: Option<u32>,
    pub output_format: Option<&'static str>,
    pub data_folder: Option<&'static str>,
}

pub async fn create_file_buffers(
    config_struct: &StreamConfig,
) -> Vec<File> {
    let future_buffers: Vec<_> = join_all(recurs_get_files(config_struct.data_folder.unwrap()))
        .await
        .into_iter()
        .map(|f| f.unwrap())
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
