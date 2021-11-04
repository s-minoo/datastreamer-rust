use crate::processor::{Processor, Record};

use crate::util::{self, StreamConfig};
use async_trait::async_trait;
use futures_util::stream::SplitSink;
use futures_util::StreamExt;
use log::{debug, error, info};
use std::collections::HashMap;

use std::sync::Arc;

use std::io::Error as StdError;
use std::io::ErrorKind as StdErrorKind;
use std::net::SocketAddr;

use tokio::io::AsyncBufReadExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::RwLock;
use tokio_tungstenite::tungstenite::{Message, Result};
use tokio_tungstenite::{accept_async, tungstenite::Error};
pub mod constant;

///shorthand type associations
type SharedData<F> = Arc<RwLock<GroupedData<F>>>;
type DataKey<P> = <<P as Processor>::Model as Record>::Key;
type Data<P> = <P as Processor>::Model;
type GroupedData<F> = HashMap<DataKey<F>, Vec<Data<F>>>;
type Proc<Pub> = <Pub as Publisher>::Processor;

///
/// Starts a TCP stream for the given stream configuration.
/// New web socekts will be spawned in tokio async threads for each new connection.
pub async fn start_stream<Pub: 'static>(
    config_struct: StreamConfig,
    publisher: &'static Pub,
) -> Result<()>
where
    Pub: Publisher + Send + Sync,
{
    let addr = format!("{}:{}", config_struct.ip, config_struct.port);
    let data_root = config_struct.data_folder.unwrap();
    info!(
        "For connection: {}:{}\nReading files from the given data root directory: {}",
        config_struct.ip, config_struct.port, data_root
    );

    let data = read_file_in_mem::<Proc<Pub>>(data_root).await?;

    info!("Finished reading and grouping");

    let shared_data = Arc::new(RwLock::new(data));

    let listener = TcpListener::bind(&addr).await.expect("Can't listen");
    info!("Listening on: {}", addr);

    while let Ok((stream, _)) = listener.accept().await {
        let peer = stream
            .peer_addr()
            .expect("connected streams should have a peer address");

        let shared_data = shared_data.clone();
        info!("Peer address: {}", peer);

        tokio::spawn(accept_connection(peer, stream, shared_data, publisher));
    }
    Ok(())
}

async fn read_file_in_mem<F: 'static>(data_root: &str) -> Result<HashMap<DataKey<F>, Vec<Data<F>>>>
where
    F: Processor,
{
    let mut files = util::create_file_buffers(data_root).await;
    debug!("{:?}", data_root);
    debug!("{:?}", files);
    let mut data = Vec::new();

    //Start parsing data in files to custom data models
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
        data.append(&mut file_data);
    }

    //Group the data models by their key
    let data = match tokio::task::spawn_blocking(move || F::group_output(data)).await {
        Ok(res) => res,
        Err(err) => return Err(Error::Io(StdError::new(StdErrorKind::Other, err))),
    };
    Ok(data)
}

async fn accept_connection<Pub: 'static>(
    peer: std::net::SocketAddr,
    stream: tokio::net::TcpStream,
    data_lock: SharedData<Proc<Pub>>,
    publisher: &Pub,
) where
    Pub: Publisher,
{
    if let Err(e) = handle_connection(peer, stream, data_lock, publisher).await {
        match e {
            Error::ConnectionClosed | Error::Protocol(_) | Error::Utf8 => (),
            err => error!("Error processing connection: {}", err),
        }
    }
}

async fn handle_connection<Pub>(
    peer: SocketAddr,
    stream: TcpStream,
    data_lock: SharedData<Proc<Pub>>,
    _publisher: &Pub,
) -> Result<()>
where
    Pub: Publisher,
{
    let ws_stream = accept_async(stream).await.expect("Failed to accept");
    info!("New WebSocket connection: {}", peer);
    let (ws_sender, _) = ws_stream.split();
    <Pub as Publisher>::publish_data::<Proc<Pub>>(data_lock, ws_sender).await?;
    Ok(())
}

#[async_trait]
pub trait Publisher {
    type Processor: Processor;
    async fn publish_data<F>(
        data_lock: SharedData<F>,
        mut ws_sender: SplitSink<tokio_tungstenite::WebSocketStream<TcpStream>, Message>,
    ) -> Result<()>
    where
        F: Processor;
}
