// Copyright 2015-2021 SWIM.AI inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::configuration::downlink::{
    BackpressureMode, ClientParams, ConfigHierarchy, DownlinkParams, OnInvalidMessage,
};
use crate::configuration::router::RouterParams;
use crate::interface::SwimClientBuilder;
use std::fs;
use std::fs::File;
use std::io::Read;
use std::num::NonZeroUsize;
use swim_model::path::AbsolutePath;
use swim_recon::parser::parse_value as parse_single;
use swim_utilities::future::retryable::{Quantity, RetryStrategy};
use tokio::time::Duration;
use url::Url;

#[test]
fn test_conf_from_file_default_manual() {
    let mut file =
        fs::File::open("src/configuration/tests/resources/valid/default-config-manual.recon")
            .unwrap();

    let mut contents = String::new();
    file.read_to_string(&mut contents).unwrap();
    let config = parse_single(&contents).unwrap();
    let config = ConfigHierarchy::try_from_value(config, false).unwrap();

    let expected = ConfigHierarchy::default();

    assert_eq!(config, expected)
}

#[test]
fn test_conf_from_file_default_automatic() {
    let mut file =
        fs::File::open("src/configuration/tests/resources/valid/default-config-manual.recon")
            .unwrap();

    let mut contents = String::new();
    file.read_to_string(&mut contents).unwrap();
    let config = parse_single(&contents).unwrap();
    let config = ConfigHierarchy::try_from_value(config, true).unwrap();

    let expected = ConfigHierarchy::default();

    assert_eq!(config, expected)
}

#[test]
fn test_conf_from_file_default_mixed() {
    let mut file =
        fs::File::open("src/configuration/tests/resources/valid/default-config-manual.recon")
            .unwrap();

    let mut contents = String::new();
    file.read_to_string(&mut contents).unwrap();
    let config = parse_single(&contents).unwrap();
    let config = ConfigHierarchy::try_from_value(config, true).unwrap();

    let expected = ConfigHierarchy::default();

    assert_eq!(config, expected)
}

#[test]
fn test_conf_from_file_retry_exponential() {
    let mut file = fs::File::open(
        "src/configuration/tests/resources/valid/client-config-retry-exponential.recon",
    )
    .unwrap();

    let mut contents = String::new();
    file.read_to_string(&mut contents).unwrap();
    let config = parse_single(&contents).unwrap();
    let config = ConfigHierarchy::try_from_value(config, true).unwrap();

    let expected = ConfigHierarchy::new(
        ClientParams::new(
            NonZeroUsize::new(2).unwrap(),
            RouterParams::new(
                RetryStrategy::exponential(Duration::from_secs(30), Quantity::Infinite),
                Duration::from_secs(100),
                Duration::from_secs(6000),
                NonZeroUsize::new(15).unwrap(),
            ),
        ),
        Default::default(),
    );

    assert_eq!(config, expected)
}

#[test]
fn test_conf_from_file_retry_immediate() {
    let mut file = fs::File::open(
        "src/configuration/tests/resources/valid/client-config-retry-immediate.recon",
    )
    .unwrap();

    let mut contents = String::new();
    file.read_to_string(&mut contents).unwrap();
    let config = parse_single(&contents).unwrap();
    let config = ConfigHierarchy::try_from_value(config, true).unwrap();

    let expected = ConfigHierarchy::new(
        ClientParams::new(
            NonZeroUsize::new(2).unwrap(),
            RouterParams::new(
                RetryStrategy::immediate(NonZeroUsize::new(10).unwrap()),
                Duration::from_secs(100),
                Duration::from_secs(6000),
                NonZeroUsize::new(15).unwrap(),
            ),
        ),
        Default::default(),
    );

    assert_eq!(config, expected)
}

#[test]
fn test_conf_from_file_retry_interval() {
    let mut file = fs::File::open(
        "src/configuration/tests/resources/valid/client-config-retry-interval.recon",
    )
    .unwrap();

    let mut contents = String::new();
    file.read_to_string(&mut contents).unwrap();
    let config = parse_single(&contents).unwrap();
    let config = ConfigHierarchy::try_from_value(config, true).unwrap();

    let expected = ConfigHierarchy::new(
        ClientParams::new(
            NonZeroUsize::new(2).unwrap(),
            RouterParams::new(
                RetryStrategy::interval(Duration::from_secs(5), Quantity::Infinite),
                Duration::from_secs(100),
                Duration::from_secs(6000),
                NonZeroUsize::new(15).unwrap(),
            ),
        ),
        Default::default(),
    );

    assert_eq!(config, expected)
}

#[test]
fn test_conf_from_file_retry_none() {
    let mut file =
        fs::File::open("src/configuration/tests/resources/valid/client-config-retry-none.recon")
            .unwrap();

    let mut contents = String::new();
    file.read_to_string(&mut contents).unwrap();
    let config = parse_single(&contents).unwrap();
    let config = ConfigHierarchy::try_from_value(config, true).unwrap();

    let expected = ConfigHierarchy::new(
        ClientParams::new(
            NonZeroUsize::new(2).unwrap(),
            RouterParams::new(
                RetryStrategy::none(),
                Duration::from_secs(100),
                Duration::from_secs(6000),
                NonZeroUsize::new(15).unwrap(),
            ),
        ),
        Default::default(),
    );

    assert_eq!(config, expected)
}

fn create_full_config() -> ConfigHierarchy {
    let mut config = ConfigHierarchy::new(
        ClientParams::new(
            NonZeroUsize::new(5).unwrap(),
            RouterParams::new(
                RetryStrategy::immediate(NonZeroUsize::new(10).unwrap()),
                Duration::from_secs(15),
                Duration::from_secs(22),
                NonZeroUsize::new(50).unwrap(),
            ),
        ),
        DownlinkParams::new(
            BackpressureMode::Release {
                input_buffer_size: NonZeroUsize::new(15).unwrap(),
                bridge_buffer_size: NonZeroUsize::new(15).unwrap(),
                max_active_keys: NonZeroUsize::new(15).unwrap(),
                yield_after: NonZeroUsize::new(15).unwrap(),
            },
            Duration::from_secs(30000),
            10,
            OnInvalidMessage::Ignore,
            512,
        )
        .unwrap(),
    );

    config.for_host(
        Url::parse("ws://127.0.0.1").unwrap(),
        DownlinkParams::new(
            BackpressureMode::Propagate,
            Duration::from_secs(40000),
            15,
            OnInvalidMessage::Ignore,
            200,
        )
        .unwrap(),
    );

    config.for_host(
        Url::parse("ws://127.0.0.2").unwrap(),
        DownlinkParams::new(
            BackpressureMode::Propagate,
            Duration::from_secs(50000),
            25,
            OnInvalidMessage::Terminate,
            300,
        )
        .unwrap(),
    );

    config.for_lane(
        &AbsolutePath::new(Url::parse("ws://192.168.0.1").unwrap(), "bar", "baz"),
        DownlinkParams::new(
            BackpressureMode::Propagate,
            Duration::from_secs(90000),
            40,
            OnInvalidMessage::Ignore,
            100,
        )
        .unwrap(),
    );

    config.for_lane(
        &AbsolutePath::new(Url::parse("ws://192.168.0.2").unwrap(), "qux", "quz"),
        DownlinkParams::new(
            BackpressureMode::Release {
                input_buffer_size: NonZeroUsize::new(20).unwrap(),
                bridge_buffer_size: NonZeroUsize::new(20).unwrap(),
                max_active_keys: NonZeroUsize::new(20).unwrap(),
                yield_after: NonZeroUsize::new(20).unwrap(),
            },
            Duration::from_secs(100000),
            50,
            OnInvalidMessage::Terminate,
            600,
        )
        .unwrap(),
    );

    config
}

#[test]
fn test_conf_from_file_full_ordered() {
    let mut file =
        fs::File::open("src/configuration/tests/resources/valid/client-config-full-ordered.recon")
            .unwrap();

    let mut contents = String::new();
    file.read_to_string(&mut contents).unwrap();
    let config = parse_single(&contents).unwrap();
    let config = ConfigHierarchy::try_from_value(config, false).unwrap();

    let expected = create_full_config();

    assert_eq!(config, expected)
}

#[test]
fn test_conf_from_file_full_unordered() {
    let mut file = fs::File::open(
        "src/configuration/tests/resources/valid/client-config-full-unordered.recon",
    )
    .unwrap();

    let mut contents = String::new();
    file.read_to_string(&mut contents).unwrap();
    let config = parse_single(&contents).unwrap();
    let config = ConfigHierarchy::try_from_value(config, false).unwrap();

    let expected = create_full_config();

    assert_eq!(config, expected)
}

#[test]
fn test_client_file_conf_non_utf8_error() {
    let file =
        File::open("src/configuration/tests/resources/invalid/non-utf-8-config.recon").unwrap();
    let result = SwimClientBuilder::new_from_file(file, false);

    if let Err(err) = result {
        assert_eq!(
            err.to_string(),
            "File error: stream did not contain valid UTF-8"
        )
    } else {
        panic!("Expected file error!")
    }
}

#[test]
fn test_client_file_conf_recon_error() {
    let file =
        File::open("src/configuration/tests/resources/invalid/parse-err-config.recon").unwrap();
    let result = SwimClientBuilder::new_from_file(file, false);

    if let Err(err) = result {
        assert_eq!(err.to_string(), "Recon error: Failed to parse the input. rule = 'Char' at (4:17).")
    } else {
        panic!("Expected file error!")
    }
}

#[test]
fn test_conf_from_file_downlink_error() {
    let mut file =
        fs::File::open("src/configuration/tests/resources/invalid/downlink-error.recon").unwrap();
    let mut contents = String::new();
    file.read_to_string(&mut contents).unwrap();
    let config = parse_single(&contents).unwrap();

    let result = ConfigHierarchy::try_from_value(config, false);

    if let Err(err) = result {
        assert_eq!(err.to_string(), "Downlink error: Timeout must be positive.")
    } else {
        panic!("Expected configuration parsing error!")
    }
}

#[test]
fn test_conf_from_file_missing_attribute() {
    let mut file =
        fs::File::open("src/configuration/tests/resources/invalid/missing-attr.recon").unwrap();
    let mut contents = String::new();
    file.read_to_string(&mut contents).unwrap();
    let config = parse_single(&contents).unwrap();

    let result = ConfigHierarchy::try_from_value(config, false);

    if let Err(err) = result {
        assert_eq!(
            err.to_string(),
            "Missing \"@downlinks\" attribute in \"@config\"."
        )
    } else {
        panic!("Expected configuration parsing error!")
    }
}

#[test]
fn test_conf_from_file_missing_key() {
    let mut file =
        fs::File::open("src/configuration/tests/resources/invalid/missing-key.recon").unwrap();
    let mut contents = String::new();
    file.read_to_string(&mut contents).unwrap();
    let config = parse_single(&contents).unwrap();

    let result = ConfigHierarchy::try_from_value(config, false);

    if let Err(err) = result {
        assert_eq!(err.to_string(), "Missing \"router\" key in \"@client\".")
    } else {
        panic!("Expected configuration parsing error!")
    }
}

#[test]
fn test_conf_from_file_invalid_key() {
    let mut file =
        fs::File::open("src/configuration/tests/resources/invalid/invalid-key.recon").unwrap();
    let mut contents = String::new();
    file.read_to_string(&mut contents).unwrap();
    let config = parse_single(&contents).unwrap();

    let result = ConfigHierarchy::try_from_value(config, false);

    if let Err(err) = result {
        assert_eq!(err.to_string(), "Invalid key \"foo-url\" in \"host\".")
    } else {
        panic!("Expected configuration parsing error!")
    }
}

#[test]
fn test_conf_from_file_invalid_value() {
    let mut file =
        fs::File::open("src/configuration/tests/resources/invalid/invalid-value.recon").unwrap();
    let mut contents = String::new();
    file.read_to_string(&mut contents).unwrap();
    let config = parse_single(&contents).unwrap();

    let result = ConfigHierarchy::try_from_value(config, false);

    if let Err(err) = result {
        assert_eq!(
            err.to_string(),
            "Invalid value \"test\" in \"buffer_size\"."
        )
    } else {
        panic!("Expected configuration parsing error!")
    }
}

#[test]
fn test_conf_from_file_unexpected_attr_top() {
    let mut file =
        fs::File::open("src/configuration/tests/resources/invalid/unexpected-attr-top.recon")
            .unwrap();
    let mut contents = String::new();
    file.read_to_string(&mut contents).unwrap();
    let config = parse_single(&contents).unwrap();

    let result = ConfigHierarchy::try_from_value(config, false);

    if let Err(err) = result {
        assert_eq!(err.to_string(), "Unexpected attribute \"@configuration\".")
    } else {
        panic!("Expected configuration parsing error!")
    }
}

#[test]
fn test_conf_from_file_unexpected_attr_nested() {
    let mut file =
        fs::File::open("src/configuration/tests/resources/invalid/unexpected-attr-nested.recon")
            .unwrap();
    let mut contents = String::new();
    file.read_to_string(&mut contents).unwrap();
    let config = parse_single(&contents).unwrap();

    let result = ConfigHierarchy::try_from_value(config, false);

    if let Err(err) = result {
        assert_eq!(
            err.to_string(),
            "Unexpected attribute \"@hello\" in \"config\"."
        )
    } else {
        panic!("Expected configuration parsing error!")
    }
}

#[test]
fn test_conf_from_file_unexpected_slot() {
    let mut file =
        fs::File::open("src/configuration/tests/resources/invalid/unexpected-slot.recon").unwrap();
    let mut contents = String::new();
    file.read_to_string(&mut contents).unwrap();
    let config = parse_single(&contents).unwrap();

    let result = ConfigHierarchy::try_from_value(config, false);

    if let Err(err) = result {
        assert_eq!(
            err.to_string(),
            "Unexpected slot \"\"@client\":{\"32\"}\" in \"config\"."
        )
    } else {
        panic!("Expected configuration parsing error!")
    }
}

#[test]
fn test_conf_from_file_unexpected_key() {
    let mut file =
        fs::File::open("src/configuration/tests/resources/invalid/unexpected-key.recon").unwrap();
    let mut contents = String::new();
    file.read_to_string(&mut contents).unwrap();
    let config = parse_single(&contents).unwrap();

    let result = ConfigHierarchy::try_from_value(config, false);

    if let Err(err) = result {
        assert_eq!(err.to_string(), "Unexpected key \"hello\" in \"client\".")
    } else {
        panic!("Expected configuration parsing error!")
    }
}

#[test]
fn test_conf_from_file_unexpected_value_top() {
    let mut file =
        fs::File::open("src/configuration/tests/resources/invalid/unexpected-value-top.recon")
            .unwrap();
    let mut contents = String::new();
    file.read_to_string(&mut contents).unwrap();
    let config = parse_single(&contents).unwrap();

    let result = ConfigHierarchy::try_from_value(config, false);

    if let Err(err) = result {
        assert_eq!(err.to_string(), "Unexpected value \"2\".")
    } else {
        panic!("Expected configuration parsing error!")
    }
}

#[test]
fn test_conf_from_file_unexpected_value_nested() {
    let mut file =
        fs::File::open("src/configuration/tests/resources/invalid/unexpected-value-nested.recon")
            .unwrap();
    let mut contents = String::new();
    file.read_to_string(&mut contents).unwrap();
    let config = parse_single(&contents).unwrap();

    let result = ConfigHierarchy::try_from_value(config, false);

    if let Err(err) = result {
        assert_eq!(err.to_string(), "Unexpected value \"2\" in \"client\".")
    } else {
        panic!("Expected configuration parsing error!")
    }
}

#[test]
fn test_conf_from_file_unnamed_record_top() {
    let mut file =
        fs::File::open("src/configuration/tests/resources/invalid/unnamed-record-top.recon")
            .unwrap();
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
        panic!("Expected configuration parsing error!")
    }
}

#[test]
fn test_conf_from_file_unnamed_record_nested() {
    let mut file =
        fs::File::open("src/configuration/tests/resources/invalid/unnamed-record-nested.recon")
            .unwrap();
    let mut contents = String::new();
    file.read_to_string(&mut contents).unwrap();
    let config = parse_single(&contents).unwrap();

    let result = ConfigHierarchy::try_from_value(config, false);

    if let Err(err) = result {
        assert_eq!(
            err.to_string(),
            "Unnamed record \"{back_pressure:@propagate,idle_timeout:60000,buffer_size:5,on_invalid:terminate,yield_after:256}\" in \"config\"."
        )
    } else {
        panic!("Expected configuration parsing error!")
    }
}

#[test]
fn test_conf_from_file_double_attr() {
    let mut file =
        fs::File::open("src/configuration/tests/resources/invalid/double-attr.recon").unwrap();
    let mut contents = String::new();
    file.read_to_string(&mut contents).unwrap();
    let config = parse_single(&contents).unwrap();

    let result = ConfigHierarchy::try_from_value(config, false);

    if let Err(err) = result {
        assert_eq!(
            err.to_string(),
            "Unexpected value \"@attribute@client{buffer_size:2,router:{retry_strategy:@exponential(max_interval:16,max_backoff:300),idle_timeout:60,conn_reaper_frequency:60,buffer_size:100}}\" in \"config\"."
        )
    } else {
        panic!("Expected configuration parsing error!")
    }
}
