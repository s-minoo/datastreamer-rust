pub mod processor;
pub mod publisher;
pub mod util;

use crate::{
    processor::ndwprocessor::{NDWFlowModel, NDWProcessor, NDWSpeedModel},
    publisher::{
        default::{ConstantPublisher, Metrics, PeriodicPublisher},
        start_stream,
    },
    util::Config,
};
use clap::{App, Arg};
use env_logger::Env;
use futures_util::{future::join_all, Future};
use log::{debug, info};
use std::{marker::PhantomData, pin::Pin};
use tokio_tungstenite::tungstenite::{self, Result};

#[tokio::main]
async fn main() -> Result<()> {
    let mut args = Vec::new();
    args.append(&mut vec![
        Arg::with_name("config")
            .value_name("CONFIG")
            .required(false)
            .index(1)
            .help("The config file in which the streams are configured. Check examples/config.toml as an example"),
    ]);

    let cli_args = App::new("Image semantics extraction")
        .args(&args)
        .get_matches();

    let config_path = cli_args
        .value_of("config")
        .unwrap_or("examples/config.toml");
    let config_content = std::fs::read_to_string(config_path)?.into_boxed_str();
    let config_content: &'static str = Box::leak(config_content);
    let config_struct: Config = toml::from_str(config_content)
        .map_err(|_| tungstenite::Error::Io(std::io::ErrorKind::InvalidData.into()))?;

    let env = Env::default()
        .filter_or("MY_LOG_LEVEL", config_struct.log_level.unwrap_or("debug"))
        .write_style_or("MY_LOG_STYLE", "auto");

    env_logger::init_from_env(env);

    info!("starting up");
    debug!("{:?}", config_struct);

    let configs = config_struct.streamconfigs;

    let mut stream_futures = Vec::new();
    for config in configs {
        let metrics = Metrics {
            ..Default::default()
        };
        // this is so ugly this needs refactoring in a rust idiomatic way
        let future: Pin<Box<dyn Future<Output = Result<()>>>> = match config.mode {
            util::Mode::Constant => {
                let publi = Box::new(ConstantPublisher {
                    config: config.clone(),
                    metrics,
                });
                let publi: &'static ConstantPublisher = Box::leak(publi);
                let proc: &'static NDWProcessor<NDWSpeedModel> = Box::leak(Box::new(
                    NDWProcessor::new(util::DataFmt::JSON, util::DataFmt::JSON),
                ));
                Box::pin(start_stream(config.clone(), publi, proc))
            }
            util::Mode::Periodic => {
                let publi = Box::new(PeriodicPublisher {
                    config: config.clone(),
                    metrics,
                });
                let publi: &'static PeriodicPublisher = Box::leak(publi);
                let proc: &'static NDWProcessor<NDWSpeedModel> = Box::leak(Box::new(
                    NDWProcessor::new(util::DataFmt::JSON, util::DataFmt::JSON),
                ));
                Box::pin(start_stream(config.clone(), publi, proc))
            }
        };
        stream_futures.push(future);
    }

    join_all(stream_futures).await;

    Ok(())

    //start_stream(config_struct).await;
}
