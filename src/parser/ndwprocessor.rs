use chrono::NaiveDateTime;
use itertools::Itertools;
use lazy_static::lazy_static;
use regex::Regex;
use std::{
    collections::HashMap,
    fmt::Display,
    time::{SystemTime, UNIX_EPOCH},
};

use super::Processor;
lazy_static! {
    static ref TIMESTAMP_REGEX: Regex =
        Regex::new(r#"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d*"#).unwrap();
}

#[derive(Debug)]
pub struct NDWModel {
    pub timestamp: NaiveDateTime,
    pub message: String,
}

impl Display for NDWModel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(
            f,
            "NDWModel(timestamp: {:?}, message:{:?})",
            &self.timestamp, &self.message
        )
    }
}

pub struct NDWProcessor;

impl NDWProcessor {
    fn extract_timestamp(text: &String) -> &str {
        let matches = TIMESTAMP_REGEX.find(text).unwrap();
        &text[matches.start()..matches.end()]
    }

    fn assemble_model(key: &String, body: &String) -> NDWModel {
        let lane = NDWProcessor::extract_lane(key);
        let message = format!("{{\"internalId\": \"{}\", {}", lane, &body[1..].trim());

        let timestamp_str = NDWProcessor::extract_timestamp(body);
        let timestamp = NaiveDateTime::parse_from_str(timestamp_str, "%Y-%m-%d %T%.f").expect(
            format!(
                "Something went wrong while parsing timestamp: {:?}",
                timestamp_str
            )
            .as_str(),
        );

        NDWModel { timestamp, message }
    }

    fn extract_lane(key: &String) -> String {
        key.match_indices("/lane")
            .map(|(idx, _)| key[idx..].to_string())
            .collect::<Vec<String>>()
            .pop()
            .unwrap_or(String::from("UNKNOWN"))
    }
}

impl Processor for NDWProcessor {
    type Model = NDWModel;
    type Output = NDWModel;
    type Key = NaiveDateTime;
    fn parse(input_line: &str) -> (Self::Key, Self::Model) {
        //Same as the DataUtils method in data-stream-generator module of the
        //open stream processing benchmark (OSP benchmark)
        let splitted_line: Vec<_> = input_line.split("=").collect();
        let (key, body) = (
            splitted_line[0].trim().to_owned(),
            splitted_line[1].trim().to_owned(),
        );

        let model = NDWProcessor::assemble_model(&key, &body);

        (model.timestamp, model)
    }

    fn group_output(
        input_data: Vec<(Self::Key, Self::Model)>,
    ) -> HashMap<Self::Key, Vec<Self::Output>> {
        input_data.into_iter().into_group_map()
    }

    fn insert_current_time(model: Self::Model) -> Self::Model {
        let current_time_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis();
        let body = format!(
            r#"{}, "timestamp": {} }} "#,
            model.message[..model.message.len() - 1].to_string(),
            current_time_ms.to_string()
        );
        NDWModel {
            message: body,
            ..model
        }
    }
}
