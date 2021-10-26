pub mod publisher;

use env_logger::Env;
use futures_util::future::join_all;
use futures_util::{future, SinkExt, StreamExt};
use log::{debug, error, info};
use serde::Deserialize;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, BufReader};
use std::net::SocketAddr;
use tokio::fs::File;
use tokio::net::{TcpListener, TcpStream};
use tokio::time::Duration;
use tokio_tungstenite::tungstenite::{Message, Result};
use tokio_tungstenite::{accept_async, tungstenite::Error};
use walkdir::WalkDir;

#[derive(Debug, Deserialize)]
enum Mode {
    Constant,
    Periodic,
}

#[derive(Deserialize, Debug)]
struct Config {
    ip: String,
    port: Option<u16>,
    mode: Mode,
    log_level: Option<&'static str>,
    data_folder: Option<&'static str>,
}

pub fn get_data_files(data_root_dir: &str) -> Vec<impl futures_util::Future<Output= std::result::Result<File,std::io::Error>>>{
    WalkDir::new(data_root_dir)
        .sort_by_file_name()
        .into_iter()
        .filter_map(|f| f.ok())
        .filter(|f| f.file_type().is_file())
        .map(|f| f.into_path())
        .map(File::open)
        .collect()
}

#[tokio::main]
async fn main() -> Result<()>{
    let config = include_str!("./resources/config.toml");
    let config_struct: Config = toml::from_str(config).unwrap();

    let env = Env::default()
        .filter_or("MY_LOG_LEVEL", config_struct.log_level.unwrap_or("debug"))
        .write_style_or("MY_LOG_STYLE", "auto");

    env_logger::init_from_env(env);

    info!("starting up");

    let future_buffers =  join_all(get_data_files(config_struct.data_folder.unwrap())).await
        .into_iter()
        .map(|f| f.unwrap())
        .map(|f| BufReader::new(f).lines());



    for mut buffer in future_buffers{
        while let Some(line) = buffer.next_line().await? {
            println!("{}",line);
        }
    }

    debug!("{:?}", config_struct);

    Ok(())
    
    //start_stream(config_struct).await;
}

async fn start_stream(config_struct: Config) {
    let addr = format!(
        "{}:{}",
        config_struct.ip,
        config_struct.port.unwrap_or(9000)
    );
    let listener = TcpListener::bind(&addr).await.expect("Can't listen");
    info!("Listening on: {}", addr);
    while let Ok((stream, _)) = listener.accept().await {
        let peer = stream
            .peer_addr()
            .expect("connected streams should have a peer address");

        info!("Peer address: {}", peer);

        tokio::spawn(accept_connection(peer, stream));
    }
}

async fn accept_connection(peer: std::net::SocketAddr, stream: tokio::net::TcpStream) {
    if let Err(e) = handle_connection(peer, stream).await {
        match e {
            Error::ConnectionClosed | Error::Protocol(_) | Error::Utf8 => (),
            err => error!("Error processing connection: {}", err),
        }
    }
}
async fn handle_connection(peer: SocketAddr, stream: TcpStream) -> Result<()> {
    let ws_stream = accept_async(stream).await.expect("Failed to accept");
    info!("New WebSocket connection: {}", peer);
    let (mut ws_sender, mut ws_receiver) = ws_stream.split();
    let mut interval = tokio::time::interval(Duration::from_millis(1000));

    // Echo incoming WebSocket messages and send a message periodically every second.
    //
    //

    loop {
        tokio::select! {
            msg = ws_receiver.next() => {
                match msg {
                    Some(msg) => {
                        let msg = msg?;
                        if msg.is_text() ||msg.is_binary() {
                            ws_sender.send(msg).await?;
                        } else if msg.is_close() {
                            break;
                        }
                    }
                    None => break,
                }
            }
            _ = interval.tick() => {
                ws_sender.send(Message::Text("tick".to_owned())).await?;
            }
        }
    }

    Ok(())
}
