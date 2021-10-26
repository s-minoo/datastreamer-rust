use std::fs;
use std::fs::File;
use std::io::{prelude::*, BufReader, BufWriter, Write};
use std::net::{Shutdown, TcpListener, TcpStream};
use std::str::FromStr;
use std::thread::sleep;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use clap::{App, Arg};
use json::JsonValue;

fn main() {
    let matches = App::new("StreamFromFile")
    .args(&[
    Arg::from_usage("-b --bind-address=[IP:PORT] 'Sets the bind address of the TCP socket.'").default_value("0.0.0.0:9999"),
    Arg::from_usage("-d --delay=[nanoseconds] 'Sets the delay *between* sending two messages, in nanoseconds.'").default_value("0"),
    Arg::from_usage("-f --input-file=[PATH TO INPUT FILE] 'The path to the ndjson file of which each line will be sent as a record to the RDF conversion tool.'").required(true),
    Arg::from_usage("-i --id-field=[ID FIELD] 'The field of the json that acts as the ID'").required(true),
    Arg::from_usage("-r --duration=[seconds] 'The duration of the run, in seconds'").default_value("60"),
    Arg::from_usage("-o --output-file=[PATH TO OUTPUT FILE] 'The path to the file where timings will be written.'").required(true),
  ]).get_matches();

    let bind_address = matches.value_of("bind-address").unwrap();
    println!("bind-address: {}", bind_address);
    let delay_arg = matches.value_of("delay").unwrap();
    println!("delay:        {} nanoseconds", delay_arg);
    let delay_value = u32::from_str(delay_arg).expect("'delay' should be a number (u32)");
    let delay = Duration::new(0, delay_value);
    let duration_arg = matches.value_of("duration").unwrap();
    println!("duration:     {} seconds", duration_arg);
    let duration = u64::from_str(duration_arg).expect("'duration' should be a number (u64)");
    let in_file = matches
        .value_of("input-file")
        .expect("No input file given.");
    println!("input-file:   {}", in_file);
    let id_field = matches.value_of("id-field").expect("No id field given.");
    println!("id-field:     {}", id_field);
    let time_file = matches
        .value_of("output-file")
        .expect("No output file given.");

    let listener = TcpListener::bind(bind_address).expect("Could not start TCP listener");

    let data = "Some data!";
    fs::write("./i-am-alive", data).expect("Unable to write i-am-alive file.");
    for stream in listener.incoming() {
        match stream {
            Ok(tcp_stream) => {
                // do something with the TcpStream
                println!("Got stream.");
                stream_file(&tcp_stream, in_file, time_file, delay, id_field, duration);
                println!("Shutting down stream.");
                tcp_stream
                    .shutdown(Shutdown::Both)
                    .expect("shutdown call failed");
                break; // exit the program
            }
            Err(e) => panic!("encountered IO error: {}", e),
        }
    }
}

fn stream_file(
    mut stream: &TcpStream,
    input_file: &str,
    time_file: &str,
    delay: Duration,
    id_field: &str,
    duration: u64,
) {
    // open input file
    let input_lines = load_file(input_file);

    // open time records file
    let t_file = File::create(time_file).expect("Could not create time records file");
    let mut time_buffer = BufWriter::new(t_file);

    let mut record_counter: u64 = 0;

    // start duration "timer"
    let start_time = SystemTime::now();

    let mut timeout = false;

    let mut total_bytes_sent: usize = 0;

    while !timeout {
        for line in &input_lines {
            let id = record_counter.to_string();
            record_counter += 1;

            // replace the value at "id_str" with number of the record.
            let mut json = json::parse(&line).expect("Could not parse JSON input");
            json[id_field] = JsonValue::from(id.clone());
            let mut record_to_stream = json.dump();
            record_to_stream.push('\n');

            let bytes_to_send = record_to_stream.as_bytes();
            total_bytes_sent += bytes_to_send.len();

            // calculate current timestamp
            let current_time = SystemTime::now();
            let since_the_epoch = current_time
                .duration_since(UNIX_EPOCH)
                .expect("ERROR: Time went backwards");
            let timestamp = since_the_epoch.as_millis(); // currently the receiver supports millisec.

            // write to socket
            stream
                .write_all(bytes_to_send)
                .expect("ERROR: Something went wrong writing to client"); // Rust Strings are UTF-8 encoded.

            // log time stamp and bytes written.
            let mut timelog_str = String::new();
            timelog_str.push_str(&id);
            timelog_str.push(',');
            timelog_str.push_str(&timestamp.to_string());
            timelog_str.push(',');
            timelog_str.push_str(&total_bytes_sent.to_string());
            timelog_str.push('\n');
            time_buffer
                .write_all(timelog_str.as_bytes())
                .expect("ERROR: Writing id to file went wrong");

            sleep(delay);

            // check if `duration` has not passed yet.
            let elapsed_time = start_time
                .elapsed()
                .expect("Something went wrong reading system time");
            if elapsed_time.as_secs() > duration {
                timeout = true;
                println!("Time-out. Stopping.");
                break;
            }
        }
    }
}

fn load_file(input_file: &str) -> Vec<String> {
    let file = File::open(input_file).expect("no such file");
    let buf = BufReader::new(file);
    buf.lines()
        .map(|l| l.expect("Could not parse line"))
        .collect()
}
