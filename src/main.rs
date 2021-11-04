pub mod processor;
pub mod publisher;
pub mod util;

use std::pin::Pin;

use crate::{
    processor::ndwprocessor::NDWProcessor,
    publisher::{
        default::{ConstantPublisher, PeriodicPublisher},
        start_stream,
    },
    util::Config,
};
use env_logger::Env;
use futures_util::{future::join_all, Future};
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
        .map(move |config| -> Pin<Box<dyn Future<Output = Result<()>>>> {
            match config.mode {
                util::Mode::Constant => {
                    Box::pin(start_stream::<NDWProcessor, _>(config, &ConstantPublisher))
                }
                util::Mode::Periodic => {
                    Box::pin(start_stream::<NDWProcessor, _>(config, &PeriodicPublisher))
                }
            }
        })
        .collect();

    join_all(stream_futures).await;

    Ok(())

    //start_stream(config_struct).await;
}
