use crate::configuration::downlink::ConfigHierarchy;
use crate::connections::factory::tungstenite::TungsteniteWsFactory;
use crate::interface::SwimClient;
use common::model::parser::parse_single;
use std::fs;
use std::fs::File;
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
    let mut file =
        fs::File::open("client/src/configuration/tests/resources/test_config.recon").unwrap();

    let mut contents = String::new();

    file.read_to_string(&mut contents).unwrap();

    let config = parse_single(&contents).unwrap();

    let config = ConfigHierarchy::try_from_value(config, true).unwrap();

    let default_config = ConfigHierarchy::default();

    println!("{:?}", config);
    println!("{:?}", default_config);
    assert_eq!(config, default_config)
}

#[test]
fn test_from_file_invalid_value() {
    let mut file =
        fs::File::open("client/src/configuration/tests/resources/invalid-value.recon").unwrap();
    let mut contents = String::new();
    file.read_to_string(&mut contents).unwrap();
    let config = parse_single(&contents).unwrap();

    let result = ConfigHierarchy::try_from_value(config, false);

    if let Err(err) = result {
        assert_eq!(
            err.to_string(),
            "Invalid value \"test\" for \"buffer_size\"."
        )
    } else {
        panic!("Expected configuration parser error!")
    }
}

#[test]
fn test_from_file_unexpected_slot() {
    let mut file =
        fs::File::open("client/src/configuration/tests/resources/unexpected-slot.recon").unwrap();
    let mut contents = String::new();
    file.read_to_string(&mut contents).unwrap();
    let config = parse_single(&contents).unwrap();

    let result = ConfigHierarchy::try_from_value(config, false);

    if let Err(err) = result {
        assert_eq!(err.to_string(), "Unexpected slot \"\"@client\":{\"32\"}\".")
    } else {
        panic!("Expected configuration parser error!")
    }
}

#[test]
fn test_from_file_unexpected_key() {
    let mut file =
        fs::File::open("client/src/configuration/tests/resources/unexpected-key.recon").unwrap();
    let mut contents = String::new();
    file.read_to_string(&mut contents).unwrap();
    let config = parse_single(&contents).unwrap();

    let result = ConfigHierarchy::try_from_value(config, false);

    if let Err(err) = result {
        assert_eq!(err.to_string(), "Unexpected attribute \"@hello\".")
    } else {
        panic!("Expected configuration parser error!")
    }
}

#[test]
fn test_from_file_unexpected_value() {
    let mut file =
        fs::File::open("client/src/configuration/tests/resources/unexpected-value.recon").unwrap();
    let mut contents = String::new();
    file.read_to_string(&mut contents).unwrap();
    let config = parse_single(&contents).unwrap();

    let result = ConfigHierarchy::try_from_value(config, false);

    if let Err(err) = result {
        assert_eq!(err.to_string(), "Unexpected value \"2\".")
    } else {
        panic!("Expected configuration parser error!")
    }
}

#[test]
fn test_from_file_unnamed_record() {
    let mut file =
        fs::File::open("client/src/configuration/tests/resources/unnamed-attr.recon").unwrap();
    let mut contents = String::new();
    file.read_to_string(&mut contents).unwrap();
    let config = parse_single(&contents).unwrap();

    let result = ConfigHierarchy::try_from_value(config, false);

    if let Err(err) = result {
        assert_eq!(
            err.to_string(),
            "Unnamed record \"{@client{buffer_size:2}}\"."
        )
    } else {
        panic!("Expected configuration parser error!")
    }
}

#[test]
fn test_from_file_unexpected_attr() {
    let mut file =
        fs::File::open("client/src/configuration/tests/resources/unexpected-attr.recon").unwrap();
    let mut contents = String::new();
    file.read_to_string(&mut contents).unwrap();
    let config = parse_single(&contents).unwrap();

    let result = ConfigHierarchy::try_from_value(config, false);

    if let Err(err) = result {
        assert_eq!(err.to_string(), "Unexpected attribute \"@configuration\".")
    } else {
        panic!("Expected configuration parser error!")
    }
}

#[tokio::test]
async fn test_client_file_conf_non_utf8_error() {
    let file =
        File::open("client/src/configuration/tests/resources/non-utf-8-config.recon").unwrap();
    let result = SwimClient::new_with_file(file, false, TungsteniteWsFactory::new(5).await).await;

    if let Err(err) = result {
        assert_eq!(
            err.to_string(),
            "Client error. Caused by: File error: stream did not contain valid UTF-8"
        )
    } else {
        panic!("Expected file error!")
    }
}

#[tokio::test]
async fn test_client_file_conf_recon_error() {
    let file =
        File::open("client/src/configuration/tests/resources/parse-err-config.recon").unwrap();
    let result = SwimClient::new_with_file(file, false, TungsteniteWsFactory::new(5).await).await;

    if let Err(err) = result {
        assert_eq!(
            err.to_string(),
            "Client error. Caused by: Recon error: Bad token at offset: 63"
        )
    } else {
        panic!("Expected file error!")
    }
}

// fn parse_config()

// use std::env;
// let path = env::current_dir().unwrap();
// println!("The current directory is {}", path.display());
