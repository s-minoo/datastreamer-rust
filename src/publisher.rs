use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;

use crate::parser::Processor;
use crate::util::{self, StreamConfig};
use futures_util::{SinkExt, StreamExt};
use log::{debug, error, info};

use std::io::Error as StdError;
use std::io::ErrorKind as StdErrorKind;
use std::net::SocketAddr;
use tokio::io::AsyncBufReadExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::RwLock;
use tokio_tungstenite::tungstenite::{Message, Result};
use tokio_tungstenite::{accept_async, tungstenite::Error};
pub mod constant;

type SharedData = Arc<RwLock<Vec<String>>>;

trait Publisher {
    fn publish(_config_struct: StreamConfig) {}
}
///
/// Starts a TCP stream for the given stream configuration.
/// New web socekts will be spawned in tokio async threads for each new connection.
pub async fn start_stream<T: 'static, Proc: 'static>(
    config_struct: StreamConfig,
    parser_f: Proc,
) -> Result<()>
where
    T: std::marker::Send + Debug,
    Proc: Fn(&str) -> T + std::marker::Send + Copy,
{
    let addr = format!("{}:{}", config_struct.ip, config_struct.port);
    let data_root = config_struct.data_folder.unwrap();
    info!(
        "For connection: {}:{}\nReading files from the given data root directory: {}",
        config_struct.ip, config_struct.port, data_root
    );

    let data = store_file_mem(data_root, parser_f)
        .await?
        .into_iter()
        .map(|f| format!("{:?}", f) )
        .collect();
    info!("Finished reading");

    let shared_data: SharedData = Arc::new(RwLock::new(data));

    let listener = TcpListener::bind(&addr).await.expect("Can't listen");
    info!("Listening on: {}", addr);

    while let Ok((stream, _)) = listener.accept().await {
        let peer = stream
            .peer_addr()
            .expect("connected streams should have a peer address");

        let shared_data = shared_data.clone();
        info!("Peer address: {}", peer);

        tokio::spawn(accept_connection(peer, stream, shared_data));
    }
    Ok(())
}

async fn test_store_file_mem< F: 'static>(data_root: &str, parser_f: F) -> Result<HashMap<<F as Processor>::Key, Vec<<F as Processor>::Model >>>
where
    F: Processor + std::marker::Send + std::marker::Sync
{
    let mut files = util::create_file_buffers(data_root).await;
    debug!("{:?}", data_root);
    debug!("{:?}", files);
    let mut data = Vec::new();

    for bf in &mut files {
        let mut lines = bf.lines();
        let mut file_data = Vec::new();
        while let Some(line) = lines.next_line().await? {
            let line = match tokio::task::spawn_blocking(move || F::parse(line.as_str())).await {
                Ok(it) => it,
                Err(err) => return Err(Error::Io(StdError::new(StdErrorKind::Other, err))),
            };
            file_data.push(line);
        }

        //Blocking thread sort 
        let mut file_data = match tokio::task::spawn_blocking(move || {
            file_data.sort_by(|el, el2| el.0.cmp(&el2.0));
            file_data
        })
        .await
        {
            Ok(data) => data,
            Err(err) => return Err(Error::Io(StdError::new(StdErrorKind::Other, err))),
        };

        data.append(&mut file_data);
    }

    let data = match tokio::task::spawn_blocking(move || {
        F::group_output(data)
    }).await
    {
            Ok(res) => res,
            Err(err) => return Err(Error::Io(StdError::new(StdErrorKind::Other, err))),
    };

    Ok(data)
}
///
/// Recursively find the data files in the given root data directory
/// and creates async BufReaders which are used to read the
async fn store_file_mem<T: 'static, F: 'static>(data_root: &str, parser_f: F) -> Result<Vec<T>>
where
    F: Fn(&str) -> T + std::marker::Send + Copy,
    T: std::marker::Send,
{
    let mut files = util::create_file_buffers(data_root).await;
    debug!("{:?}", data_root);
    debug!("{:?}", files);
    let mut data = Vec::new();

    for bf in &mut files {
        let mut lines = bf.lines();
        while let Some(line) = lines.next_line().await? {
            let line = match tokio::task::spawn_blocking(move || parser_f(line.as_str())).await {
                Ok(it) => it,
                Err(err) => return Err(Error::Io(StdError::new(StdErrorKind::Other, err))),
            };

            data.push(line);
        }
    }

    Ok(data)
}

async fn accept_connection(
    peer: std::net::SocketAddr,
    stream: tokio::net::TcpStream,
    data_lock: SharedData,
) {
    if let Err(e) = handle_connection(peer, stream, data_lock).await {
        match e {
            Error::ConnectionClosed | Error::Protocol(_) | Error::Utf8 => (),
            err => error!("Error processing connection: {}", err),
        }
    }
}

async fn handle_connection(
    peer: SocketAddr,
    stream: TcpStream,
    data_lock: SharedData,
) -> Result<()> {
    let ws_stream = accept_async(stream).await.expect("Failed to accept");
    info!("New WebSocket connection: {}", peer);
    let (mut ws_sender, _) = ws_stream.split();
    let read = data_lock.read().await;
    for line in read.iter() {
        ws_sender.send(Message::Text(line.to_string())).await?;
    }
    ws_sender.send(Message::Close(None)).await?;
    Ok(())
}
