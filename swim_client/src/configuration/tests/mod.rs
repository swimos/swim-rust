// Copyright 2015-2021 Swim Inc.
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

use std::fs;
use std::fs::File;
use std::sync::Arc;

use ratchet::deflate::{Compression, DeflateConfig, DeflateExtProvider};
use tokio::time::Duration;
use url::Url;

use swim_form::Form;
use swim_model::path::AbsolutePath;
use swim_recon::parser::parse_value;
use swim_runtime::configuration::{
    BackpressureMode, DownlinkConfig, DownlinkConnectionsConfig, DownlinksConfig, OnInvalidMessage,
    WebSocketConfig,
};
use swim_runtime::remote::config::RemoteConnectionsConfig;
use swim_runtime::ws::CompressionSwitcherProvider;
use swim_utilities::algebra::non_zero_usize;
use swim_utilities::future::retryable::{Quantity, RetryStrategy};

use crate::configuration::{ClientDownlinksConfig, SwimClientConfig};
use crate::interface::SwimClientBuilder;

#[test]
fn test_conf_from_file_default_manual() {
    let contents = include_str!("resources/valid/default-config-manual.recon");

    let config = parse_value(contents, true).unwrap();

    let config = SwimClientConfig::try_from_value(&config).unwrap();

    let expected = SwimClientConfig::default();

    assert_eq!(config, expected)
}

#[test]
fn test_conf_from_file_default_automatic() {
    let contents = include_str!("resources/valid/default-config-automatic.recon");

    let config = parse_value(contents, true).unwrap();
    let config = SwimClientConfig::try_from_value(&config).unwrap();

    let expected = SwimClientConfig::default();

    assert_eq!(config, expected)
}

#[test]
fn test_full_conf_from_file_with_comments() {
    let contents = include_str!("resources/valid/client-full-config-with-comments.recon");

    let config = parse_value(contents, true).unwrap();
    let config = SwimClientConfig::try_from_value(&config).unwrap();

    let expected = create_full_config();

    assert_eq!(config, expected)
}

#[test]
fn test_conf_from_file_default_mixed() {
    let contents = include_str!("resources/valid/default-config-mixed.recon");

    let config = parse_value(contents, true).unwrap();
    let config = SwimClientConfig::try_from_value(&config).unwrap();

    let expected = SwimClientConfig::default();

    assert_eq!(config, expected)
}

#[test]
fn test_conf_from_file_retry_exponential() {
    let contents = include_str!("resources/valid/client-config-retry-exponential.recon");

    let config = parse_value(contents, true).unwrap();
    let config = SwimClientConfig::try_from_value(&config).unwrap();

    let expected = SwimClientConfig::new(
        DownlinkConnectionsConfig::new(
            non_zero_usize!(8),
            non_zero_usize!(100),
            non_zero_usize!(256),
            RetryStrategy::exponential(Duration::from_secs(30), Quantity::Infinite),
        ),
        Default::default(),
        Default::default(),
        Default::default(),
    );

    assert_eq!(config, expected)
}

#[test]
fn test_conf_from_file_retry_immediate() {
    let contents = include_str!("resources/valid/client-config-retry-immediate.recon");

    let config = parse_value(contents, true).unwrap();
    let config = SwimClientConfig::try_from_value(&config).unwrap();

    let expected = SwimClientConfig::new(
        DownlinkConnectionsConfig::new(
            non_zero_usize!(8),
            non_zero_usize!(100),
            non_zero_usize!(256),
            RetryStrategy::immediate(non_zero_usize!(10)),
        ),
        Default::default(),
        Default::default(),
        Default::default(),
    );

    assert_eq!(config, expected)
}

#[test]
fn test_conf_from_file_retry_interval() {
    let contents = include_str!("resources/valid/client-config-retry-interval.recon");

    let config = parse_value(contents, true).unwrap();
    let config = SwimClientConfig::try_from_value(&config).unwrap();

    let expected = SwimClientConfig::new(
        DownlinkConnectionsConfig::new(
            non_zero_usize!(8),
            non_zero_usize!(100),
            non_zero_usize!(256),
            RetryStrategy::interval(Duration::from_secs(5), Quantity::Infinite),
        ),
        Default::default(),
        Default::default(),
        Default::default(),
    );

    assert_eq!(config, expected)
}

#[test]
fn test_conf_from_file_retry_none() {
    let contents = include_str!("resources/valid/client-config-retry-none.recon");

    let config = parse_value(contents, true).unwrap();
    let config = SwimClientConfig::try_from_value(&config).unwrap();

    let expected = SwimClientConfig::new(
        DownlinkConnectionsConfig::new(
            non_zero_usize!(8),
            non_zero_usize!(100),
            non_zero_usize!(256),
            RetryStrategy::none(),
        ),
        Default::default(),
        Default::default(),
        Default::default(),
    );

    assert_eq!(config, expected)
}

fn create_full_config() -> SwimClientConfig {
    let mut dl_config = ClientDownlinksConfig::new(DownlinkConfig::new(
        BackpressureMode::Release {
            input_buffer_size: non_zero_usize!(512),
            bridge_buffer_size: non_zero_usize!(512),
            max_active_keys: non_zero_usize!(512),
            yield_after: non_zero_usize!(512),
        },
        Duration::from_nanos(6666666),
        non_zero_usize!(10),
        OnInvalidMessage::Ignore,
        non_zero_usize!(512),
    ));

    dl_config.for_host(
        Url::parse("ws://127.0.0.1").unwrap(),
        DownlinkConfig::new(
            BackpressureMode::Propagate,
            Duration::from_secs(40000),
            non_zero_usize!(15),
            OnInvalidMessage::Ignore,
            non_zero_usize!(200),
        ),
    );

    dl_config.for_host(
        Url::parse("ws://127.0.0.2").unwrap(),
        DownlinkConfig::new(
            BackpressureMode::Propagate,
            Duration::from_secs(50000),
            non_zero_usize!(25),
            OnInvalidMessage::Terminate,
            non_zero_usize!(300),
        ),
    );

    dl_config.for_lane(
        &AbsolutePath::new(Url::parse("ws://192.168.0.1").unwrap(), "bar", "baz"),
        DownlinkConfig::new(
            BackpressureMode::Propagate,
            Duration::from_secs(90000),
            non_zero_usize!(40),
            OnInvalidMessage::Ignore,
            non_zero_usize!(100),
        ),
    );

    dl_config.for_lane(
        &AbsolutePath::new(Url::parse("ws://192.168.0.2").unwrap(), "qux", "quz"),
        DownlinkConfig::new(
            BackpressureMode::Release {
                input_buffer_size: non_zero_usize!(20),
                bridge_buffer_size: non_zero_usize!(20),
                max_active_keys: non_zero_usize!(20),
                yield_after: non_zero_usize!(20),
            },
            Duration::from_secs(100000),
            non_zero_usize!(50),
            OnInvalidMessage::Terminate,
            non_zero_usize!(600),
        ),
    );

    let config = SwimClientConfig::new(
        DownlinkConnectionsConfig::new(
            non_zero_usize!(8),
            non_zero_usize!(32),
            non_zero_usize!(200),
            RetryStrategy::interval(
                Duration::from_secs(54),
                Quantity::Finite(non_zero_usize!(13)),
            ),
        ),
        RemoteConnectionsConfig::new(
            non_zero_usize!(20),
            non_zero_usize!(20),
            Duration::from_secs(60),
            Duration::from_secs(40),
            RetryStrategy::none(),
            non_zero_usize!(512),
        ),
        WebSocketConfig {
            config: ratchet::WebSocketConfig {
                max_message_size: 60000000,
            },
            compression: CompressionSwitcherProvider::On(Arc::new(
                DeflateExtProvider::with_config(DeflateConfig::for_compression_level(
                    Compression::best(),
                )),
            )),
        },
        dl_config,
    );

    config
}

#[test]
fn test_conf_from_file_full_ordered() {
    let contents = include_str!("resources/valid/client-config-full-ordered.recon");

    let config = parse_value(contents, true).unwrap();
    let config = SwimClientConfig::try_from_value(&config).unwrap();

    let expected = create_full_config();

    assert_eq!(config, expected)
}

#[test]
fn test_conf_from_file_full_unordered() {
    let contents = include_str!("resources/valid/client-config-full-unordered.recon");

    let config = parse_value(contents, true).unwrap();
    let config = SwimClientConfig::try_from_value(&config).unwrap();

    let expected = create_full_config();

    assert_eq!(config, expected)
}

#[test]
fn test_conf_from_file_with_comments() {
    let file = fs::File::open(
        "src/configuration/tests/resources/valid/client-full-config-with-comments.recon",
    )
    .unwrap();

    let actual = SwimClientBuilder::new_from_file(file).unwrap();
    let expected = SwimClientBuilder::new(create_full_config());

    assert_eq!(actual, expected)
}

#[test]
fn test_client_file_conf_non_utf8_error() {
    let file =
        File::open("src/configuration/tests/resources/invalid/non-utf-8-config.recon").unwrap();
    let result = SwimClientBuilder::new_from_file(file);

    if let Err(err) = result {
        assert_eq!(
            err.to_string(),
            "Could not process client configuration: stream did not contain valid UTF-8"
        )
    } else {
        panic!("Expected file error!")
    }
}

#[test]
fn test_client_file_conf_recon_error() {
    let file =
        File::open("src/configuration/tests/resources/invalid/parse-err-config.recon").unwrap();
    let result = SwimClientBuilder::new_from_file(file);

    if let Err(err) = result {
        assert_eq!(
            err.to_string(),
            "Could not process client configuration: Failed to parse the input. rule = 'Char' at (4:17)."
        )
    } else {
        panic!("Expected file error!")
    }
}

#[test]
fn test_conf_from_file_invalid_key() {
    let file =
        fs::File::open("src/configuration/tests/resources/invalid/invalid-key.recon").unwrap();

    let result = SwimClientBuilder::new_from_file(file);

    if let Err(err) = result {
        assert_eq!(err.to_string(), "Could not process client configuration: Text value 'foo-url' is invalid: Not a valid URL.")
    } else {
        panic!("Expected configuration parsing error!")
    }
}

#[test]
fn test_conf_from_file_invalid_value() {
    let file =
        fs::File::open("src/configuration/tests/resources/invalid/invalid-value.recon").unwrap();

    let result = SwimClientBuilder::new_from_file(file);

    if let Err(err) = result {
        assert_eq!(
            err.to_string(),
            "Could not process client configuration: Unexpected value kind: Text, expected: One of: [A value of kind UInt32, A value of kind UInt64, A value of kind BigUint]."
        )
    } else {
        panic!("Expected configuration parsing error!")
    }
}

#[test]
fn test_conf_from_file_unexpected_attr_top() {
    let file =
        fs::File::open("src/configuration/tests/resources/invalid/unexpected-attr-top.recon")
            .unwrap();

    let result = SwimClientBuilder::new_from_file(file);

    if let Err(err) = result {
        assert_eq!(
            err.to_string(),
            "Could not process client configuration: Unexpected attribute: 'configuration'"
        )
    } else {
        panic!("Expected configuration parsing error!")
    }
}

#[test]
fn test_conf_from_file_unexpected_attr_nested() {
    let file =
        fs::File::open("src/configuration/tests/resources/invalid/unexpected-attr-nested.recon")
            .unwrap();

    let result = SwimClientBuilder::new_from_file(file);

    if let Err(err) = result {
        assert_eq!(
            err.to_string(),
            "Could not process client configuration: Unexpected field: 'hello'"
        )
    } else {
        panic!("Expected configuration parsing error!")
    }
}

#[test]
fn test_conf_from_file_unexpected_slot() {
    let file =
        fs::File::open("src/configuration/tests/resources/invalid/unexpected-slot.recon").unwrap();

    let result = SwimClientBuilder::new_from_file(file);

    if let Err(err) = result {
        assert_eq!(
            err.to_string(),
            "Could not process client configuration: Unexpected value kind: Text, expected: One of: [A value of kind Text, The end of the record body]."
        )
    } else {
        panic!("Expected configuration parsing error!")
    }
}

#[test]
fn test_conf_from_file_unexpected_key() {
    let file =
        fs::File::open("src/configuration/tests/resources/invalid/unexpected-key.recon").unwrap();

    let result = SwimClientBuilder::new_from_file(file);

    if let Err(err) = result {
        assert_eq!(
            err.to_string(),
            "Could not process client configuration: Unexpected field: 'hello'"
        )
    } else {
        panic!("Expected configuration parsing error!")
    }
}

#[test]
fn test_conf_from_file_unexpected_value_top() {
    let file =
        fs::File::open("src/configuration/tests/resources/invalid/unexpected-value-top.recon")
            .unwrap();

    let result = SwimClientBuilder::new_from_file(file);

    if let Err(err) = result {
        assert_eq!(err.to_string(), "Could not process client configuration: Unexpected value kind: Int64, expected: An attribute named 'config'.")
    } else {
        panic!("Expected configuration parsing error!")
    }
}

#[test]
fn test_conf_from_file_unexpected_value_nested() {
    let file =
        fs::File::open("src/configuration/tests/resources/invalid/unexpected-value-nested.recon")
            .unwrap();

    let result = SwimClientBuilder::new_from_file(file);

    if let Err(err) = result {
        assert_eq!(err.to_string(), "Could not process client configuration: Unexpected value kind: Int64, expected: One of: [A value of kind Text, The end of the record body].")
    } else {
        panic!("Expected configuration parsing error!")
    }
}

#[test]
fn test_conf_from_file_unnamed_record_top() {
    let file = fs::File::open("src/configuration/tests/resources/invalid/unnamed-record-top.recon")
        .unwrap();

    let result = SwimClientBuilder::new_from_file(file);

    if let Err(err) = result {
        assert_eq!(
            err.to_string(),
            "Could not process client configuration: Unexpected value kind: Record, expected: An attribute named 'config'."
        )
    } else {
        panic!("Expected configuration parsing error!")
    }
}

#[test]
fn test_conf_from_file_unnamed_record_nested() {
    let file =
        fs::File::open("src/configuration/tests/resources/invalid/unnamed-record-nested.recon")
            .unwrap();

    let result = SwimClientBuilder::new_from_file(file);

    if let Err(err) = result {
        assert_eq!(
            err.to_string(),
            "Could not process client configuration: Unexpected value kind: Record, expected: One of: [A value of kind Text, The end of the record body]."
        )
    } else {
        panic!("Expected configuration parsing error!")
    }
}

#[test]
fn test_conf_from_file_double_attr() {
    let file =
        fs::File::open("src/configuration/tests/resources/invalid/double-attr.recon").unwrap();

    let result = SwimClientBuilder::new_from_file(file);

    if let Err(err) = result {
        assert_eq!(
            err.to_string(),
            "Could not process client configuration: Unexpected field: 'attribute'"
        )
    } else {
        panic!("Expected configuration parsing error!")
    }
}
