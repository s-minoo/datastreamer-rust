pub mod ndwprocessor;
use std::collections::HashMap;

type ProcKey<T> = <T as Record>::Key;

pub trait Processor {
    type Model: Record + Send + Sync;
    fn parse(input_line: &str) -> (ProcKey<Self::Model>, Self::Model) {
        let model = Self::Model::parse(input_line);
        (model.get_key(), model)
    }
    fn group_output(
        input_data: Vec<(ProcKey<Self::Model>, Self::Model)>,
    ) -> HashMap<ProcKey<Self::Model>, Vec<Self::Model>>;
}

pub trait Record {
    type Key: Ord + Send + Sync;
    type Data: Send + Sync;

    fn parse(input: &str) -> Self;
    fn insert_current_time(self) -> Self;
    fn get_key(&self) -> Self::Key;
    fn get_data(&self) -> &Self::Data;
}
