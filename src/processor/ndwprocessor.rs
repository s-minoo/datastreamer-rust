use chrono::NaiveDateTime;
use itertools::Itertools;

use log::debug;

use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::{
    collections::HashMap,
    marker::PhantomData,
    time::{SystemTime, UNIX_EPOCH},
};

use serde::de;
use std::fmt;
use crate::util::DataFmt;

use super::{Processor, Record};

struct NaiveDateTimeVisitor;

impl<'de> de::Visitor<'de> for NaiveDateTimeVisitor {
    type Value = NaiveDateTime;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        write!(formatter, "a string represents chrono::NaiveDateTime")
    }

    fn visit_str<E>(self, s: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        match NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S.%f") {
            Ok(t) => Ok(t),
            Err(_) => Err(de::Error::invalid_value(de::Unexpected::Str(s), &self)),
        }
    }
}

fn from_timestamp<'de, D>(d: D) -> Result<NaiveDateTime, D::Error>
where
    D: de::Deserializer<'de>,
{
    d.deserialize_str(NaiveDateTimeVisitor)
}

//Would love to use another struct containing common fields 
//but will result in erros with the serialization crates 
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct NDWSpeedModel {
    speed: f32,
    #[serde(deserialize_with="from_timestamp")]
    timestamp: NaiveDateTime,
    #[serde(skip_deserializing)]
    current_timestamp: u64,
    accuracy: u8,
    lat: f64,
    long: f64,
    num_lanes: u8,
    #[serde(rename = "internalId")]
    internal_id: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct NDWFlowModel {
    period: u8,
    flow: u16,
    #[serde(deserialize_with="from_timestamp")]
    timestamp: NaiveDateTime,
    #[serde(skip_deserializing)]
    current_timestamp: u64,
    accuracy: u8,
    lat: f64,
    long: f64,
    num_lanes: u8,
    #[serde(rename = "internalId")]
    internal_id: String,
}

impl Model for NDWFlowModel {
    fn update_timestamp(&mut self, timestamp_ms: u128) {
        self.current_timestamp = timestamp_ms as u64;
    }

    fn get_timestamp(&self) -> NaiveDateTime {
        self.timestamp
    }
}
impl Model for NDWSpeedModel {
    fn update_timestamp(&mut self, timestamp_ms: u128) {
        self.current_timestamp = timestamp_ms as u64;
    }

    fn get_timestamp(&self) -> NaiveDateTime {
        self.timestamp
    }
}
pub trait Model: Clone {
    fn update_timestamp(&mut self, timestamp_ms: u128);
    fn get_timestamp(&self) -> NaiveDateTime;
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

        NDWModel { output_fmt, model }
    }

    fn serialize(&self, fmt: &DataFmt) -> String {
        let val = &self.model;
        let result = match fmt {
            DataFmt::JSON => serde_json::to_string(val).unwrap(),
            DataFmt::XML => serde_xml_rs::to_string(val).unwrap(),
            DataFmt::CSV => {
                let mut writer = csv::Writer::from_writer(vec![]);
                writer.serialize(val).unwrap();
                String::from_utf8(writer.into_inner().unwrap()).unwrap().replace(" ", "\n")
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
        NDWModel {
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
        debug!("{:?}", output_fmt);
        NDWProcessor::<A> {
            input_fmt,
            output_fmt,
            phantom: PhantomData,
        }
    }

    fn preprocess_input(input: &str) -> String {
        //Same as the DataUtils method in data-stream-generator module of the
        //open stream processing benchmark (OSP benchmark)
        let splitted_line: Vec<_> = input.split("=").collect();
        let (key, body) = (
            splitted_line[0].trim().to_owned(),
            splitted_line[1].trim().to_owned(),
        );
        let lane = Self::extract_lane(&key);
        let message = format!("{{\"internalId\": \"{}\", {}", &lane[1..], &body[1..].trim());
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

impl<A: Model + DeserializeOwned + Serialize + Sync + Send + std::fmt::Debug> Processor
    for NDWProcessor<A>
{
    type Model = NDWModel<A>;

    fn group_output(
        input_data: Vec<(<Self::Model as Record>::Key, Self::Model)>,
    ) -> HashMap<<Self::Model as Record>::Key, Vec<Self::Model>> {
        input_data.into_iter().into_group_map()
    }

    fn get_inputfmt(&self) -> &DataFmt {
        &self.input_fmt
    }

    fn get_outputfmt(&self) -> &DataFmt {
        &self.output_fmt
    }
}
