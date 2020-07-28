use crate::configuration::downlink::ConfigHierarchy;
use common::model::parser::parse_single;
use std::fs;
use std::io::Read;

#[test]
fn from_string() {
    let config = parse_single(
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
    let mut file = fs::File::open("client/src/configuration/tests/test_config.recon").unwrap();

    let mut contents = String::new();

    file.read_to_string(&mut contents).unwrap();

    let config = parse_single(&contents).unwrap();

    let config = ConfigHierarchy::try_from_value(config, true).unwrap();

    let default_config = ConfigHierarchy::default();

    println!("{:?}", config);
    println!("{:?}", default_config);
    assert_eq!(config, default_config)
}

// fn parse_config()

// use std::env;
// let path = env::current_dir().unwrap();
// println!("The current directory is {}", path.display());
