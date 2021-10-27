use crate::publisher::SharedData;
use crate::util::{self, StreamConfig};
use futures_util::{SinkExt, StreamExt};
use log::{debug, error, info, warn};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::AsyncBufReadExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::RwLock;

use tokio_tungstenite::tungstenite::{Message, Result};
use tokio_tungstenite::{accept_async, tungstenite::Error};

pub async fn start_stream(config_struct: StreamConfig) -> Result<()> {
    let addr = format!("{}:{}", config_struct.ip, config_struct.port);

    let data = store_file_mem(&config_struct).await?;

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

async fn store_file_mem(config_struct: &StreamConfig) -> Result<Vec<String>> {
    let mut files = util::create_file_buffers(config_struct).await;
    debug!("{:?}", config_struct);
    debug!("{:?}", files);
    let mut data = Vec::new();
    for bf in &mut files {
        let mut lines = bf.lines();
        warn!("{:?}", lines);
        while let Some(line) = lines.next_line().await? {
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

    // Echo incoming WebSocket messages and send a message periodically every second.
    //
    //
    let read = data_lock.read().await;

    for line in read.iter() {
        ws_sender.send(Message::Text(line.clone())).await?;
    }

    ws_sender.send(Message::Close(None)).await?;

    Ok(())
}
