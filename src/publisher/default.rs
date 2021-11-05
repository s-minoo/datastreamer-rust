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
use chrono::Timelike;
use futures_util::stream;
use futures_util::stream::SplitSink;
use futures_util::{SinkExt, StreamExt};
use itertools::Itertools;
use lazy_static::lazy_static;
use log::debug;
use metered::{measure, HitCount, Throughput};

use tokio::net::TcpStream;
use tokio::select;
use tokio::time;
use tokio::time::Duration;
use tokio::time::Interval;
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
pub struct PeriodicPublisher {
    pub data_bloat: u32,
}

#[async_trait]
impl Publisher for PeriodicPublisher {
    async fn publish_data<F>(
        data_lock: SharedData<F>,
        mut ws_sender: SplitSink<tokio_tungstenite::WebSocketStream<TcpStream>, Message>,
    ) -> Result<()>
    where
        F: Processor,
    {
        {
            // Acquire read lock on data;
            let r_lock = data_lock.read().await;
            let data = &*r_lock;
            let (mut tick_second, mut tick_100ms, mut tick_5ms) = create_tickers();

            let mut minute_interval = time::interval(Duration::from_secs(60));
            minute_interval.set_missed_tick_behavior(MissedTickBehavior::Delay);

            for key in data.keys().sorted() {
                // Last 1 second

                let bucket_data = data.get(key).unwrap();
                let time_stamp = bucket_data[0].get_timestamp();

                if time_stamp != None {
                    let minute_time = time_stamp.unwrap().minute();
                    //Every 10 minute of data, publish a burst of data
                    if minute_time % 10 == 0 {
                        debug!("Starting burst publishing for timestamp: {:?}", time_stamp);
                        Self::publish_burst(bucket_data, &mut ws_sender, None).await?;
                        debug!("Finished burst publishing for timestamp: {:?}", time_stamp);
                    } else {
                        publish_constant(
                            bucket_data,
                            &mut ws_sender,
                            &mut tick_100ms,
                            &mut tick_5ms,
                            None,
                        )
                        .await?;
                    }
                } else {
                    select! {
                        biased;
                        _ = minute_interval.tick() => {
                            Self::publish_burst(bucket_data, &mut ws_sender,None).await?;
                        }
                        _ = async {} =>{
                            publish_constant(bucket_data, &mut ws_sender, &mut tick_100ms, &mut tick_5ms, None).await?;
                        }
                    }
                }
                tick_second.tick().await;
                debug!("One second elapsed!");
            }
            // Drop read lock on data;
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

impl PeriodicPublisher {
    async fn publish_burst<T: Record>(
        data: &Vec<T>,
        ws_sender: &mut SplitSink<tokio_tungstenite::WebSocketStream<TcpStream>, Message>,
        volume: Option<u32>,
    ) -> Result<()> {
        let volume = match volume {
            Some(val) => val,
            None => 1,
        };

        // no delays since its a burst publish
        for _batch in 0..9 {
            for record in data {
                for _iteration in 0..volume {
                    let current_rec = record.insert_current_time();
                    ws_sender
                        .send(Message::Text(format!("{}", current_rec.get_data())))
                        .await?;
                }
            }
        }

        
        Ok(())
    }
}

async fn publish_constant<T: Record>(
    data: &Vec<T>,
    ws_sender: &mut SplitSink<tokio_tungstenite::WebSocketStream<TcpStream>, Message>,
    tick_100ms: &mut Interval,
    tick_5ms: &mut Interval,
    volume: Option<u32>,
) -> Result<()> {
    let volume = match volume {
        Some(val) => val,
        None => 1,
    };

    for _batch in 0..9 {
        // Lasts 100ms
        for record in data {
            //Last 5ms
            for _iteration in 0..volume {
                let current_rec = record.insert_current_time();
                ws_sender
                    .send(Message::Text(format!("{}", current_rec.get_data())))
                    .await?;
            }
            tick_5ms.tick().await;
        }
        tick_100ms.tick().await;
    }

    Ok(())
}
///
/// Publishes the data at a constant rate (e.g. 400 messages/sec)
///
pub struct ConstantPublisher {
    pub data_bloat: u32,
}

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
            // Acquire read lock on data_lock
            let r_lock = data_lock.read().await;
            let data = &*r_lock;
            let (mut tick_second, mut tick_100ms, mut tick_5ms) = create_tickers();

            for key in data.keys().sorted() {
                // Last 1 second
                for _batch in 0..9 {
                    // Lasts 100ms
                    for record in data.get(key).unwrap() {
                        // Last 5ms
                        let current_rec = record.insert_current_time();
                        ws_sender
                            .send(Message::Text(format!("{}", current_rec.get_data())))
                            .await?;
                        tick_5ms.tick().await;
                    }
                    tick_100ms.tick().await;
                }
                tick_second.tick().await;
                debug!("One second elapsed!");
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

fn create_tickers() -> (Interval, Interval, Interval) {
    // Set up interval ticks
    let mut tick_second = time::interval(Duration::from_secs(1));
    let mut tick_100ms = time::interval(Duration::from_millis(100));
    let mut tick_5ms = time::interval(Duration::from_millis(5));

    tick_second.set_missed_tick_behavior(MissedTickBehavior::Delay);
    tick_100ms.set_missed_tick_behavior(MissedTickBehavior::Delay);
    tick_5ms.set_missed_tick_behavior(MissedTickBehavior::Delay);

    (tick_second, tick_100ms, tick_5ms)
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