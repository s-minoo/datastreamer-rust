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
use crate::processor::Record;
use async_trait::async_trait;
use futures_util::stream::SplitSink;
use futures_util::stream;
use futures_util::{SinkExt, StreamExt};
use itertools::Itertools;
use lazy_static::lazy_static;
use log::debug;
use metered::{measure, HitCount, Throughput};

use tokio::net::TcpStream;
use tokio::time::Duration;
use tokio::time;
use tokio::time::MissedTickBehavior;
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
        {
            // u
            // Acquire read lock on data_lock
            let r_lock = data_lock.read().await;
            let data = &*r_lock;


            // Set up interval ticks 
            let tick_second = &mut (time::interval(Duration::from_secs(1)));
            let tick_100ms = &mut (time::interval(Duration::from_millis(100))); 
            let tick_5ms = &mut(time::interval(Duration::from_millis(5)));

            tick_second.set_missed_tick_behavior(MissedTickBehavior::Delay);
            tick_100ms.set_missed_tick_behavior(MissedTickBehavior::Delay);
            tick_5ms.set_missed_tick_behavior(MissedTickBehavior::Delay);

            for key in data.keys().sorted(){
                for batch in 0..9 { // Lasts 1 seconds
                    for record in data.get(key).unwrap(){
                        ws_sender.send(Message::Text(format!("{}",record.get_data()))).await?;
                        tick_5ms.tick().await;
                    }
                    tick_100ms.tick().await;
                }
                tick_second.tick().await;
                debug!("One second elapsed!");
            }



            for line in data.iter() {
                measure!(&METRICS.throughput, {
                    ws_sender.send(Message::Text(line.0.to_string())).await?;
                });
            }

            // Drop read lock on data_lock
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
