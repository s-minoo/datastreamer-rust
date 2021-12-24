pub mod processor;
pub mod publisher;
pub mod util;

use crate::{
    processor::ndwprocessor::{NDWFlowModel, NDWProcessor, NDWSpeedModel},
    publisher::{
        default::{ConstantPublisher, PeriodicPublisher},
        start_stream, Publisher,
    },
    util::Config,
};
use clap::{App, Arg};
use datastreamer::metrics::FileRecorder;
use env_logger::Env;
use futures_util::{future::join_all, Future};
use log::{debug, info};
use std::pin::Pin;
use tokio_tungstenite::tungstenite::{self, Result};

#[tokio::main]
async fn main() -> Result<()> {
    let  recorder= Box::new(FileRecorder::new());
    metrics::set_boxed_recorder(recorder).unwrap();
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
        let output_format = config.output_format;
        let input_format = config.input_format;
        // this is so ugly this needs refactoring in a rust idiomatic way
        // god help me refactor this monstrosity

        let folder_name = config.data_folder.unwrap();
        let output_log_file = util::get_output(&config);

        let future: Pin<Box<dyn Future<Output = Result<()>>>> = match config.mode {
            util::Mode::Constant => {
                // Refactor this monstrosity please
                let publi = Box::new(ConstantPublisher::new(
                    config.clone(),
                    output_log_file,
                    ));
                let publi: &'static ConstantPublisher = Box::leak(publi);

                create_pinned_future(folder_name, input_format, output_format, publi)
            }
            util::Mode::Periodic => {
                // Refactor this monstrosity please
                let publi = Box::new(PeriodicPublisher::new(
                    config.clone(),
                    output_log_file,
                    ));
                let publi: &'static PeriodicPublisher = Box::leak(publi);
                create_pinned_future(folder_name, input_format, output_format, publi)
            }
        };
        stream_futures.push(future);
    }

    join_all(stream_futures).await;

    Ok(())
    //start_stream(config_struct).await;
}

fn create_pinned_future<Pub>(
    folder_name: &str,
    input_format: util::DataFmt,
    output_format: util::DataFmt,
    publi: &'static Pub,
) -> Pin<Box<dyn Future<Output = std::result::Result<(), tungstenite::Error>>>>
where
    Pub: Publisher + Send + Sync,
{
    if folder_name.contains("flow") {
        let processor = NDWProcessor::<NDWFlowModel>::new(input_format, output_format);
        let proc: &'static NDWProcessor<NDWFlowModel> = Box::leak(Box::new(processor));
        Box::pin(start_stream(publi, proc))
    } else {
        let proc: &'static NDWProcessor<NDWSpeedModel> =
            Box::leak(Box::new(NDWProcessor::new(input_format, output_format)));
        Box::pin(start_stream(publi, proc))
    }
}
