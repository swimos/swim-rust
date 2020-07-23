use crate::model::parser::parse_document;
use std::fs;

#[test]
fn from_string() {
    let config = parse_document(
        "@config {
    @client {
        buffer_size: 2
        router: @params {
            retry_strategy: @exponential(max_interval: 16, max_backoff: 300, retry_no: 0),
            idle_timeout: 60
            conn_reaper_frequency: 60
            buffer_size: 100
        }
    }
    @downlinks {
        back_pressure: \"propagate\"
        queue_size: 5
        idle_timeout: 60000
        buffer_size: 5
        on_invalid: \"terminate\"
        yield_after: 256
    }
}",
    );

    println!("{:?}", config)
}

#[test]
fn from_file() {
    let contents = fs::read_to_string("common/src/model/parser/tests/configuration/client_config.recon")
        .expect("Something went wrong reading the file");

    let config = parse_document(&contents);

    println!("{:?}", config)
}

// use std::env;
// let path = env::current_dir().unwrap();
// println!("The current directory is {}", path.display());
