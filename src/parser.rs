pub struct NDWProcessor {}


impl NDWProcessor {
    fn put_lane_number_in_body(key: &String, body: &String) -> String {
        let lane = key
            .match_indices("/lane")
            .map(|(idx, _)| key[idx..].to_string())
            .collect::<Vec<String>>()
            .pop()
            .unwrap_or(String::from("UNKNOWN"));

        format!("{{\"internalId\": \"{}\" {}", lane, &body[1..])
    }
}
pub trait Processor<T> {
    fn parse(input_line: &str) -> T;
}

impl Processor<String> for NDWProcessor {
    fn parse(input_line: &str) -> String {
        //Same as the DataUtils method in data-stream-generator module of the
        //open stream processing benchmark (OSP benchmark)
        let splitted_line: Vec<_> = input_line.split("=").collect();
        let (key, body) = (
            splitted_line[0].trim().to_owned(),
            splitted_line[1].trim().to_owned(),
        );
        NDWProcessor::put_lane_number_in_body(&key, &body)
    }
}
