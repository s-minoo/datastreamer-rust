use chrono::NaiveDateTime;
use itertools::Itertools;
use lazy_static::lazy_static;
use regex::Regex;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::{
    collections::HashMap,
    fmt::Display,
    marker::PhantomData,
    time::{SystemTime, UNIX_EPOCH},
};

use crate::util::DataFmt;

use super::{Processor, Record};
lazy_static! {
    static ref TIMESTAMP_REGEX: Regex =
        Regex::new(r#"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d*"#).unwrap();
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct NDWCommon {
    timestamp: NaiveDateTime,
    #[serde(skip_deserializing)]
    current_timestamp: u128, 
    accuracy: u8,
    lat: f64,
    long: f64,
    num_lanes: u8,
    #[serde(rename = "internalId" )]
    internal_id: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct NDWSpeedModel {
    speed: u16,
    #[serde(flatten)]
    common: NDWCommon,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct NDWFlowModel {
    period: u8,
    flow: u16,
    #[serde(flatten)]
    common: NDWCommon,

}

impl Model for NDWFlowModel{
    fn update_timestamp(&mut self, timestamp_ms:u128 ) {
        self.common.current_timestamp  = timestamp_ms; 
    }

    fn get_timestamp(&self)->NaiveDateTime {
        self.common.timestamp
    }
}
impl Model for NDWSpeedModel{
    fn update_timestamp(&mut self, timestamp_ms:u128 ) {
        self.common.current_timestamp  = timestamp_ms; 
    }

    fn get_timestamp(&self)->NaiveDateTime {
        self.common.timestamp
    }
}
pub trait Model:Clone{
    fn update_timestamp(&mut self, timestamp_ms:u128 );
    fn get_timestamp(&self)->NaiveDateTime; 
}

#[derive(Debug)]
pub struct NDWModel<A: Model> {
    pub model: A,
    output_fmt: DataFmt,
}

impl<A: Model + Serialize + DeserializeOwned> Record for NDWModel<A> {
    type Key = NaiveDateTime;
    type Data = String;
    fn deserialize(input: String, output_fmt: DataFmt) -> Self {
        let processed_input = NDWProcessor::<A>::preprocess_input(&input);
        let model = serde_json::from_str::<A>(&processed_input).unwrap();

        NDWModel{
            output_fmt,
            model
        }
    }

    fn serialize(&self, fmt: &DataFmt) -> String {
        let val = &self.model;
        let result = match fmt {
            DataFmt::JSON => serde_json::to_string(val).unwrap(),
            DataFmt::XML => serde_xml_rs::to_string(val).unwrap(),
            DataFmt::CSV => {
                let mut writer = csv::Writer::from_writer(vec![]);
                writer.serialize(val).unwrap();
                String::from_utf8(writer.into_inner().unwrap()).unwrap()
            }
        };
        return result;
    }

    fn get_timestamp(&self) -> Option<NaiveDateTime> {
        Some(self.model.get_timestamp())
    }

    fn insert_current_time(&self) -> Self {
        let current_time_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis();

        let mut model = self.model.clone();
        model.update_timestamp(current_time_ms);
        NDWModel{
            output_fmt: self.output_fmt,
            model,
        }
    }

    fn get_key(&self) -> Self::Key {
        self.model.get_timestamp()
    }

    fn get_data(&self) -> Self::Data {
        self.serialize(&self.output_fmt)
    }
}


pub struct NDWProcessor<A> {
    pub phantom: PhantomData<A>,
    input_fmt: DataFmt,
    output_fmt: DataFmt,
}

impl<A: Model> NDWProcessor<A> {
    pub fn new(input_fmt: DataFmt, output_fmt: DataFmt) -> Self {
        NDWProcessor::<A> {
            input_fmt,
            output_fmt,
            phantom: PhantomData,
        }
    }

    fn preprocess_input(input: &str) -> String{
        //Same as the DataUtils method in data-stream-generator module of the
        //open stream processing benchmark (OSP benchmark)
        let splitted_line: Vec<_> = input.split("=").collect();
        let (key, body) = (
            splitted_line[0].trim().to_owned(),
            splitted_line[1].trim().to_owned(),
        );
        let lane = Self::extract_lane(&key);
        let message = format!("{{\"internalId\": \"{}\", {}", lane, &body[1..].trim());
        message
    }

    fn extract_lane(key: &String) -> String {
        key.match_indices("/lane")
            .map(|(idx, _)| key[idx..].to_string())
            .collect::<Vec<String>>()
            .pop()
            .unwrap_or(String::from("UNKNOWN"))
    }
}

impl<A: Model + DeserializeOwned  + Serialize + Sync + Send + std::fmt::Debug> Processor for NDWProcessor<A> {
    type Model = NDWModel<A>;

    fn group_output(
        input_data: Vec<(<Self::Model as Record>::Key, Self::Model)>,
    ) -> HashMap<<Self::Model as Record>::Key, Vec<Self::Model>> {
        input_data.into_iter().into_group_map()
    }

    fn get_inputfmt(&self) -> &DataFmt {
        &self.output_fmt
    }

    fn get_outputfmt(&self) -> &DataFmt {
        &self.input_fmt
    }
}
