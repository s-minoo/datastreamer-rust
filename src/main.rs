pub mod processor;
pub mod publisher;
pub mod util;

use crate::{processor::ndwprocessor::NDWProcessor, publisher::{constant::{ConstantPublisher, PeriodicPublisher}, start_stream}, util::Config};
use env_logger::Env;
use futures_util::future::{Either, join_all};
use log::{debug, info};
use tokio_tungstenite::tungstenite::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let config = include_str!("./resources/config.toml");
    let config_struct: Config = toml::from_str(config).unwrap();

    let env = Env::default()
        .filter_or("MY_LOG_LEVEL", config_struct.log_level.unwrap_or("debug"))
        .write_style_or("MY_LOG_STYLE", "auto");

    env_logger::init_from_env(env);

    info!("starting up");
    debug!("{:?}", config_struct);

    let configs = config_struct.streamconfigs;

    let stream_futures: Vec<_> = configs
        .into_iter()
        .map(move |config| {
            match config.mode {
                util::Mode::Constant => Either::Left(start_stream(config, &ConstantPublisher{processor:NDWProcessor})),
                util::Mode::Periodic => Either::Right(start_stream(config, &PeriodicPublisher{processor:NDWProcessor})),
            }
        })
        .collect();


    join_all(stream_futures).await;

    Ok(())

    //start_stream(config_struct).await;
}
