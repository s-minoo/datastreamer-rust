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
pub struct PeriodicPublisher<T> {
    processor: T,
}
#[async_trait]
impl<T: Processor> Publisher for PeriodicPublisher<T> {
    type Processor = T;

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

pub struct ConstantPublisher<P: Processor> {
    pub processor: P,
}

#[async_trait]
impl<T: Processor> Publisher for ConstantPublisher<T> {
    type Processor = T;

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
