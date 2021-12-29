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
use std::time::{Instant};

use super::Publisher;


use crate::processor::Processor;
use crate::processor::Record;
use crate::util::StreamConfig;
use async_trait::async_trait;
use chrono::Timelike;
use futures_util::stream::SplitSink;
use futures_util::SinkExt;
use itertools::Itertools;
use log::debug;
use log::info;
use metrics::gauge;
use metrics::register_gauge;
use metrics::Unit;
use tokio::net::TcpStream;
use tokio::select;
use tokio::time;
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
    pub config: StreamConfig,
    output: String,
}

#[async_trait]
impl Publisher for PeriodicPublisher {
    async fn publish_data<F>(
        &self,
        data_lock: SharedData<F>,
        mut ws_sender: SplitSink<tokio_tungstenite::WebSocketStream<TcpStream>, Message>,
    ) -> Result<()>
    where
        F: Processor,
    {
        register_gauge!(format!("{}", self.output), Unit::CountPerSecond);
        {
            // Acquire read lock on data;
            let r_lock = data_lock.read().await;
            let data = &*r_lock;
            let (mut tick_5second, mut tick_second, mut tick_100ms, mut tick_5ms) = create_tickers();

            let mut minute_interval = time::interval(time::Duration::from_secs(60));
            minute_interval.set_missed_tick_behavior(MissedTickBehavior::Delay);

            for key in data.keys().sorted() {
                // Last 1 second

                let bucket_data = data.get(key).unwrap();
                let time_stamp = bucket_data[0].get_timestamp();
                let now = Instant::now();
                let throughput;

                if time_stamp != None {
                    let minute_time = time_stamp.unwrap().minute();
                    //Every 10 minute of data, publish a burst of data
                    if minute_time % 10 == 0 {
                        debug!("Starting burst publishing for timestamp: {:?}", time_stamp);
                        throughput = self.publish_burst(bucket_data, &mut ws_sender).await?;
                        debug!("Finished burst publishing for timestamp: {:?}", time_stamp);
                    } else {
                        throughput = self.publish_constant(
                            bucket_data,
                            &mut ws_sender,
                            &mut tick_100ms,
                            &mut tick_5ms,
                        )
                       .await?;
                    }
                } else {
                    // If the records themselves are not chronologically ordered,
                    // we will use machine time for data burst instead
                    throughput = select! {
                        biased;
                        _ = minute_interval.tick() => {
                            self.publish_burst(bucket_data, &mut ws_sender).await?
                        }
                        _ = async {} =>{
                            self.publish_constant(bucket_data, &mut ws_sender, &mut tick_100ms, &mut tick_5ms).await?
                        }
                    };

                
                }
                tick_second.tick().await;
                debug!("One second elapsed!");

                let elapsed = now.elapsed().as_secs_f64(); 
                debug!("Elapsed time: {}, throughput: {}", elapsed, throughput);
                gauge!(format!("{}", self.output), throughput/elapsed, "action" => "write");

                select! {
                    _ = tick_5second.tick() =>{
                        gauge!(format!("{}",self.output), 0.0, "action" => "flush");
                        info!("5 seconds elapsed, flusing metrics logs!");
                    }
                    _ = async {} => {
                        continue;
                    }
                };
            }
            // Drop read lock on data;
        }
        debug!("Finished sending data");
        Ok(())
    }

    fn get_config(&self) -> &StreamConfig {
        &self.config
    }
}

impl PeriodicPublisher {
    pub fn new(config: StreamConfig, output: String) -> Self {
        Self { config, output }
    }

    async fn publish_burst<T: Record>(
        &self,
        data: &Vec<T>,
        ws_sender: &mut SplitSink<tokio_tungstenite::WebSocketStream<TcpStream>, Message>,
    ) -> Result<f64> {
        let volume = self.config.volume;
        // no delays since its a burst publish
        //
        let mut throughput = 0.0;
        for _batch in 0..9 {
            for record in data {
                for _iteration in 0..volume {
                    let current_rec = record.insert_current_time();
                    throughput += 1.0;
                    ws_sender
                        .send(Message::Text(format!("{}", current_rec.get_data())))
                        .await?;
                }
            }
        }

        Ok(throughput)
    }
    async fn publish_constant<T: Record>(
        &self,
        data: &Vec<T>,
        ws_sender: &mut SplitSink<tokio_tungstenite::WebSocketStream<TcpStream>, Message>,
        tick_100ms: &mut Interval,
        tick_5ms: &mut Interval,
    ) -> Result<f64> {
        let mut throughput = 0.0;
        for _batch in 0..9 {
            // Lasts 100ms
            for record in data {
                //Last 5ms
                let current_rec = record.insert_current_time();
                throughput += 1.0;
                ws_sender
                    .send(Message::Text(format!("{}", current_rec.get_data())))
                    .await?;
                tick_5ms.tick().await;
            }
            tick_100ms.tick().await;
        }

        Ok(throughput)
    }
}

///
/// Publishes the data at a constant rate (e.g. 400 messages/sec)
///
pub struct ConstantPublisher {
    pub config: StreamConfig,
    output: String,
}

impl ConstantPublisher {
    pub fn new(config: StreamConfig, output: String) -> Self {
        Self { config, output }
    }
}

#[async_trait]
impl Publisher for ConstantPublisher {
    async fn publish_data<F>(
        &self,
        data_lock: SharedData<F>,
        mut ws_sender: SplitSink<tokio_tungstenite::WebSocketStream<TcpStream>, Message>,
    ) -> Result<()>
    where
        F: Processor,
    {
        register_gauge!(format!("{}", self.output), Unit::CountPerSecond);
        {
            // Acquire read lock on data_lock
            let r_lock = data_lock.read().await;
            let data = &*r_lock;
            let (mut tick_5second, mut tick_second, mut tick_100ms, mut tick_5ms) =
                create_tickers();
            let volume = self.config.volume;

            for key in data.keys().sorted() {
                // Last 1 second

                let now = Instant::now();

                let mut throughput = 0.0;
                for _batch in 0..9 {
                    // Lasts 100ms
                    for record in data.get(key).unwrap() {
                        // Last 5ms
                        for _iter in 0..volume {
                            let current_rec = record.insert_current_time();
                            throughput += 1.0;
                            ws_sender
                                .send(Message::Text(format!("{}", current_rec.get_data())))
                                .await?;
                        }
                        tick_5ms.tick().await;
                    }
                    tick_100ms.tick().await;
                }


                tick_second.tick().await;
                debug!("One second elapsed!");

                let elapsed = now.elapsed().as_secs_f64();
                gauge!(format!("{}", self.output), throughput/elapsed, "action" => "write");

                select! {
                    _ = tick_5second.tick() =>{
                        gauge!(format!("{}",self.output), 0.0, "action" => "flush");
                        info!("5 seconds elapsed, flusing metrics logs!");
                    }
                    _ = async {} => {
                        continue;
                    }
                };
            }

            // Drop read lock on data_lock
        }

        debug!("Finished sending data");
        ws_sender.send(Message::Close(None)).await?;

        Ok(())
    }

    fn get_config(&self) -> &StreamConfig {
        &self.config
    }
}

fn create_tickers() -> (Interval, Interval, Interval, Interval) {
    // Set up interval ticks
    let mut tick_5second = time::interval(time::Duration::from_secs(5));
    let mut tick_second = time::interval(time::Duration::from_secs(1));
    let mut tick_100ms = time::interval(time::Duration::from_millis(100));
    let mut tick_5ms = time::interval(time::Duration::from_millis(5));

    tick_5second.set_missed_tick_behavior(MissedTickBehavior::Delay);
    tick_second.set_missed_tick_behavior(MissedTickBehavior::Delay);
    tick_100ms.set_missed_tick_behavior(MissedTickBehavior::Delay);
    tick_5ms.set_missed_tick_behavior(MissedTickBehavior::Delay);
    (tick_5second, tick_second, tick_100ms, tick_5ms)
}
