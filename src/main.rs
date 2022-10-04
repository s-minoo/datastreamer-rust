use clap::{App, Arg};
use datastreamer::metrics::FileRecorder;
use datastreamer::processor::ndwprocessor::{NDWFlowModel, NDWProcessor, NDWSpeedModel};
use datastreamer::processor::Processor;
use datastreamer::publisher::{
    default::{ConstantPublisher, PeriodicPublisher},
    start_stream, Publisher,
};
use datastreamer::util::Config;
use datastreamer::util::{self, StreamConfig};
use env_logger::Env;
use futures_util::{future::join_all, Future};
use log::{debug, info};
use std::pin::Pin;
use tokio_tungstenite::tungstenite::{self, Result};

#[tokio::main]
async fn main() -> Result<()> {
    let recorder = Box::new(FileRecorder::new());
    metrics::set_boxed_recorder(recorder).unwrap();
    let mut args = Vec::new();
    args.append(&mut vec![
        Arg::with_name("config")
            .value_name("CONFIG")
            .required(false)
            .index(1)
            .help("The config file in which the streams are configured. Check examples/config.toml (default usage) as an example"),
    ]);

    let cli_args = App::new("Image semantics extraction")
        .args(&args)
        .get_matches();

    let config_path = cli_args
        .value_of("config")
        .unwrap_or("examples/config.toml");
    let config_content = std::fs::read_to_string(config_path)?;
    let config_struct: Config = toml::from_str(&config_content)
        .map_err(|_| tungstenite::Error::Io(std::io::ErrorKind::InvalidData.into()))?;

    let env = Env::default()
        .filter_or(
            "MY_LOG_LEVEL",
            config_struct
                .log_level
                .as_deref()
                .unwrap_or("debug"),
        )
        .write_style_or("MY_LOG_STYLE", "auto");

    env_logger::init_from_env(env);

    info!("starting up");
    debug!("{:?}", config_struct);

    let configs = config_struct.streamconfigs;

    let mut stream_futures = Vec::new();
    for config in configs {
        let folder_name = config.data_folder.as_ref().unwrap();

        let future: Pin<Box<dyn Future<Output = Result<()>>>> =
            match (config.mode, folder_name.contains("flow")) {
                (util::Mode::Constant, true) => {
                    create_pinned_future::<ConstantPublisher, NDWProcessor<NDWFlowModel>>(config)
                }
                (util::Mode::Constant, false) => {
                    create_pinned_future::<ConstantPublisher, NDWProcessor<NDWSpeedModel>>(config)
                }
                (util::Mode::Periodic, true) => {
                    create_pinned_future::<PeriodicPublisher, NDWProcessor<NDWFlowModel>>(config)
                }
                (util::Mode::Periodic, false) => {
                    create_pinned_future::<PeriodicPublisher, NDWProcessor<NDWSpeedModel>>(config)
                }
            };
        stream_futures.push(future);
    }

    join_all(stream_futures).await;

    Ok(())
}

fn create_pinned_future<Pub, Proc>(
    config: StreamConfig,
) -> Pin<Box<dyn Future<Output = std::result::Result<(), tungstenite::Error>>>>
where
    Pub: Publisher + Send + Sync + 'static,
    Proc: Processor + 'static,
{
    let output = util::get_output(&config);
    let proc = Proc::new(config.input_format, config.output_format);
    let publi = Pub::new(config, output);
    Box::pin(start_stream(publi, proc))
}
