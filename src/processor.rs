//! This module contains processors for parsing the
//! string input into a custom data model.
//!
//! **TODO**: Generalize the input to accept any type instead of
//! just `&str`
pub mod ndwprocessor;
use std::fmt::Debug;
use std::hash::Hash;
use std::{collections::HashMap, fmt::Display};

use chrono::NaiveDateTime;

type ProcKey<T> = <T as Record>::Key;

/// Processes the string input and deserializes them
/// into internal data representation
///
/// **TODO**: Might want to use [`serde`] crate for this
///
/// [`serde`]: serde
pub trait Processor {
    type Model: Record + Send + Sync + Display + Debug;

    /// Returns a tuple containing the key and the data.
    ///
    /// Parses the given input string to the custom data model.
    fn parse(input_line: &str) -> (ProcKey<Self::Model>, Self::Model) {
        let model = Self::Model::parse(input_line);
        (model.get_key(), model)
    }

    /// Groups the given `(key, data)` array by their key.
    fn group_output(
        input_data: Vec<(ProcKey<Self::Model>, Self::Model)>,
    ) -> HashMap<ProcKey<Self::Model>, Vec<Self::Model>>;
}

pub trait Record {
    type Key: Ord + Send + Sync + Hash + Display + Debug;
    type Data: Send + Sync + Display + Debug + Clone;

    fn parse(input: &str) -> Self;
    fn get_timestamp(&self) -> Option<NaiveDateTime>;
    fn insert_current_time(&self) -> Self;
    fn get_key(&self) -> Self::Key;
    fn get_data(&self) -> &Self::Data;
}
