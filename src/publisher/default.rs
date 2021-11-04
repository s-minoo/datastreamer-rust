//!
//! This module contains default implementations of the 
//! [`Publisher`] trait.
//!
//! Currently, there are two types of publishers: [Constant] and [Periodic].
//! They define the stream rate at which the data is
//! published through the websocket. 
//!
//!
//! [`Publisher`]: super::Publisher 
//! [Constant]: ConstantPublisher
//! [Periodic]: PeriodicPublisher
use super::Publisher;

use crate::processor::Processor;
use async_trait::async_trait;
use futures_util::stream::SplitSink;
use futures_util::SinkExt;
use lazy_static::lazy_static;
use log::debug;
use metered::{measure, HitCount, Throughput};

use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::Result;

use super::SharedData;

/// Publishes data at a periodic rate. 
///
/// For example, the data will be published at a 
/// high rate of 14000 messages/s every 10 seconds.  
/// A low constant rate of 400 messages/s are published during the 
/// 10 seconds downtime. 
///
pub struct PeriodicPublisher;
#[async_trait]
impl Publisher for PeriodicPublisher {

    async fn publish_data<F>(
        data_lock: SharedData<F>,
        mut ws_sender: SplitSink<tokio_tungstenite::WebSocketStream<TcpStream>, Message>,
    ) -> Result<()>
    where
        F: Processor,
    {
        let data = data_lock.read().await;

        for line in data.iter() {
            measure!(&METRICS.throughput, {
                ws_sender.send(Message::Text(line.0.to_string())).await?;
            });
        }
        debug!("Finished sending data");
        measure!(&METRICS.throughput, {
            ws_sender.send(Message::Close(None)).await?;
        });

        debug!(
            "The throughput for this socket is: {:?}",
            METRICS.throughput
        );
        debug!("{}", serde_yaml::to_string(&METRICS.throughput).unwrap());
        Ok(())
    }
}


/// Publishes the data at a constant rate (e.g. 400 messages/sec)
///
pub struct ConstantPublisher;


#[async_trait]
impl Publisher for ConstantPublisher {

    async fn publish_data<F>(
        data_lock: SharedData<F>,
        mut ws_sender: SplitSink<tokio_tungstenite::WebSocketStream<TcpStream>, Message>,
    ) -> Result<()>
    where
        F: Processor,
    {
        let data = data_lock.read().await;

        for line in data.iter() {
            measure!(&METRICS.throughput, {
                ws_sender.send(Message::Text(line.0.to_string())).await?;
            });
        }
        debug!("Finished sending data");
        measure!(&METRICS.throughput, {
            ws_sender.send(Message::Close(None)).await?;
        });

        debug!(
            "The throughput for this socket is: {:?}",
            METRICS.throughput
        );
        debug!("{}", serde_yaml::to_string(&METRICS.throughput).unwrap());
        Ok(())
    }
}

#[derive(Debug, Default)]
pub struct Metrics {
    pub throughput: Throughput,
    pub hitcount: HitCount,
}

lazy_static! {
    static ref METRICS: Metrics = Metrics {
        ..Default::default()
    };
}
