//!
//! This module is responsible for handling the incoming connections and
//! stream data through the websockets.
//!
//! The sub-modules containing the concrete implementation of the
//! [`Publisher`] trait will be used to publish the data with varying data stream characteristics.
//!
//!
//! [`Publisher`]: Publisher
use crate::processor::{Processor, Record};
use crate::util::{self, StreamConfig};
use async_trait::async_trait;
use futures_util::stream::SplitSink;
use futures_util::stream::SplitStream;
use futures_util::StreamExt;
use log::{debug, error, info};
use std::collections::HashMap;
use std::io::Error as StdError;
use std::io::ErrorKind as StdErrorKind;
use std::net::SocketAddr;
use std::sync::Arc;

use tokio::io::AsyncBufReadExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::RwLock;
use tokio_tungstenite::tungstenite::{Message, Result};
use tokio_tungstenite::{accept_async, tungstenite::Error};

pub mod default;

type SharedData<F> = Arc<RwLock<GroupedData<F>>>;
type DataKey<P> = <<P as Processor>::Model as Record>::Key;
type Data<P> = <P as Processor>::Model;
type GroupedData<F> = HashMap<DataKey<F>, Vec<Data<F>>>;

/// Starts a TCP stream for the given stream configuration.
///
/// Each new web sockets will be handled in their own tokio async threads.
pub async fn start_stream<Proc: 'static, Pub: 'static>(
    publisher: &'static Pub,
    proc: &'static Proc,
) -> Result<()>
where
    Pub: Publisher + Send + Sync,
    Proc: Processor + Send + Sync,
{
    let config_struct = publisher.get_config();
    let addr = format!("{}:{}", config_struct.ip, config_struct.port);
    let data_root = config_struct.data_folder.unwrap();
    info!(
        "For connection: {}:{}, reading files from the given data root directory: {}",
        config_struct.ip, config_struct.port, data_root
    );

    let data = read_file_in_mem(data_root, proc).await?;
    info!("Finished reading and grouping data: {}", data_root);

    let shared_data = Arc::new(RwLock::new(data));
    let listener = TcpListener::bind(&addr).await.expect("Can't listen");
    info!(
        "Listening on: {}, with publishing mode: {:?}",
        addr, config_struct.mode
    );

    while let Ok((stream, _)) = listener.accept().await {
        let peer = stream
            .peer_addr()
            .expect("connected streams should have a peer address");

        let shared_data = shared_data.clone();
        info!("Peer address: {}", peer);
        tokio::spawn(accept_connection::<Proc, _>(
            peer,
            stream,
            shared_data,
            publisher,
        ));
    }

    Ok(())
}

/// Returns the data [records] in a [`HashMap`] grouped by a [key].
///
/// Reads all the files located under the given data root folder and process
/// them with a data [`Processor`]. The processor will group the [records] by
/// their corresponding [key].
///
/// [records]: crate::processor::Record
/// [key]: crate::processor::Record::Key
/// [`Processor`]: crate::processor::Processor
async fn read_file_in_mem<F: 'static>(
    data_root: &str,
    proc: &'static F,
) -> Result<HashMap<DataKey<F>, Vec<Data<F>>>>
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
            let line = match tokio::task::spawn_blocking(move || proc.parse(line.as_str())).await {
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

/// Accepts the incoming client connection and handle it for
/// data streaming.
async fn accept_connection<Proc, Pub: 'static>(
    peer: std::net::SocketAddr,
    stream: tokio::net::TcpStream,
    data_lock: SharedData<Proc>,
    publisher: &Pub,
) where
    Proc: Processor,
    Pub: Publisher,
{
    if let Err(e) = handle_connection::<Proc, _>(peer, stream, data_lock, publisher).await {
        match e {
            Error::ConnectionClosed | Error::Protocol(_) | Error::Utf8 => (),
            err => error!("Error processing connection: {}", err),
        }
    }
}

/// Handle the incoming connection by opening a websocket.
///
/// The peer will start receiving streaming data with the streaming behaviour
/// as defined by the [`Publisher`]'s implementations.
///
/// [`Publisher`]: crate::publisher::Publisher
async fn handle_connection<Proc, Pub>(
    peer: SocketAddr,
    stream: TcpStream,
    data_lock: SharedData<Proc>,
    publisher: &Pub,
) -> Result<()>
where
    Proc: Processor,
    Pub: Publisher,
{
    let ws_stream = accept_async(stream).await.expect("Failed to accept");
    info!("New WebSocket connection: {}", peer);
    let (ws_sender, ws_receiver) = ws_stream.split();
    publisher.publish_data::<Proc>(data_lock, ws_sender, ws_receiver).await?;
    Ok(())
}

/// Publishes data through a websocket connection.
///
/// The behaviour of the data stream (i.e. stream-rate, delays) will be defined
/// by the underlying implementations of this trait.
///
#[async_trait]
pub trait Publisher: Sized {
    fn new(config: StreamConfig, output: String) -> Self;

    fn new_leaked(config: StreamConfig, output: String) -> &'static Self {
        Box::leak(Box::new(Self::new(config, output)))
    }

    /// Publishes the given data through a websocket.
    async fn publish_data<F>(
        &self,
        data_lock: SharedData<F>,
        mut ws_sender: SplitSink<tokio_tungstenite::WebSocketStream<TcpStream>, Message>,
        mut ws_receiver: SplitStream<tokio_tungstenite::WebSocketStream<TcpStream>>
    ) -> Result<()>
    where
        F: Processor;

    fn get_config(&self) -> &StreamConfig;
}
