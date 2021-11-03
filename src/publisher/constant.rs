use crate::processor::Processor;
use crate::publisher::Publisher;
use async_trait::async_trait;
use futures_util::stream::SplitSink;
use futures_util::SinkExt;
use lazy_static::lazy_static;
use log::debug;
use metered::{measure, HitCount, Throughput};
use std::sync::Arc;

use tokio::{net::TcpStream, sync::RwLock};
use tokio_tungstenite::tungstenite::Result;
use tokio_tungstenite::{tungstenite::Message, WebSocketStream};

use super::SharedData;

pub struct ConstantPublisher;

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
impl ConstantPublisher{
    async fn publish_data<Proc>(
        data_lock: Arc<RwLock<Vec<String>>>,
        mut ws_sender: SplitSink<WebSocketStream<TcpStream>, Message>,
    ) -> Result<()> {
        let data = data_lock.read().await;

        for line in data.iter() {
            measure!(&METRICS.throughput, {
                ws_sender.send(Message::Text(line.to_string())).await?;
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

#[async_trait]
impl Publisher for ConstantPublisher {
    async fn publish_data<Proc>(
        data_lock: SharedData<Proc>,
        mut ws_sender: SplitSink<tokio_tungstenite::WebSocketStream<TcpStream>, Message>,
    ) -> Result<()> 
        where 
            Proc: Processor{
                Ok(())
            }
}
