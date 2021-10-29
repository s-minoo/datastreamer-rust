pub mod ndwprocessor;
use std::collections::HashMap;

pub trait Processor {
    type Model: Send;
    type Output;
    type Key: Ord + Send;
    fn parse(input_line: &str) -> (Self::Key, Self::Model);
    fn group_output(
        input_data: Vec<(Self::Key, Self::Model)>,
    ) -> HashMap<Self::Key, Vec<Self::Model>>;
}

