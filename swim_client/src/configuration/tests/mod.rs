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

use crate::configuration::{
    BackpressureMode, ClientDownlinksConfig, DownlinkConfig, DownlinkConnectionsConfig,
    DownlinksConfig, OnInvalidMessage, SwimClientConfig,
};
use crate::interface::SwimClientBuilder;
use std::fs;
use std::fs::File;
use std::io::Read;
use std::num::NonZeroUsize;
use swim_common::form::Form;
use swim_common::model::parser::parse_single;
use swim_common::routing::remote::config::RemoteConnectionsConfig;
use swim_common::warp::path::AbsolutePath;
use tokio::time::Duration;
use tokio_tungstenite::tungstenite::extensions::compression::WsCompression;
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;
use url::Url;
use utilities::future::retryable::strategy::{Quantity, RetryStrategy};

#[test]
fn test_conf_from_file_default_manual() {
    let mut file =
        fs::File::open("src/configuration/tests/resources/valid/default-config-manual.recon")
            .unwrap();

    let mut contents = String::new();
    file.read_to_string(&mut contents).unwrap();
    let config = parse_single(&contents).unwrap();
    let config = SwimClientConfig::try_from_value(&config).unwrap();

    let expected = SwimClientConfig::default();

    assert_eq!(config, expected)
}

#[test]
fn test_conf_from_file_default_automatic() {
    let mut file =
        fs::File::open("src/configuration/tests/resources/valid/default-config-automatic.recon")
            .unwrap();

    let mut contents = String::new();
    file.read_to_string(&mut contents).unwrap();
    let config = parse_single(&contents).unwrap();
    let config = SwimClientConfig::try_from_value(&config).unwrap();

    let expected = SwimClientConfig::default();

    assert_eq!(config, expected)
}

#[test]
fn test_conf_from_file_default_mixed() {
    let mut file =
        fs::File::open("src/configuration/tests/resources/valid/default-config-mixed.recon")
            .unwrap();

    let mut contents = String::new();
    file.read_to_string(&mut contents).unwrap();
    let config = parse_single(&contents).unwrap();
    let config = SwimClientConfig::try_from_value(&config).unwrap();

    let expected = SwimClientConfig::default();

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
    let config = SwimClientConfig::try_from_value(&config).unwrap();

    let expected = SwimClientConfig::new(
        DownlinkConnectionsConfig::new(
            NonZeroUsize::new(8).unwrap(),
            NonZeroUsize::new(100).unwrap(),
            NonZeroUsize::new(256).unwrap(),
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
    let mut file = fs::File::open(
        "src/configuration/tests/resources/valid/client-config-retry-immediate.recon",
    )
        .unwrap();

    let mut contents = String::new();
    file.read_to_string(&mut contents).unwrap();
    let config = parse_single(&contents).unwrap();
    let config = SwimClientConfig::try_from_value(&config).unwrap();

    let expected = SwimClientConfig::new(
        DownlinkConnectionsConfig::new(
            NonZeroUsize::new(8).unwrap(),
            NonZeroUsize::new(100).unwrap(),
            NonZeroUsize::new(256).unwrap(),
            RetryStrategy::immediate(NonZeroUsize::new(10).unwrap()),
        ),
        Default::default(),
        Default::default(),
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
    let config = SwimClientConfig::try_from_value(&config).unwrap();

    let expected = SwimClientConfig::new(
        DownlinkConnectionsConfig::new(
            NonZeroUsize::new(8).unwrap(),
            NonZeroUsize::new(100).unwrap(),
            NonZeroUsize::new(256).unwrap(),
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
    let mut file =
        fs::File::open("src/configuration/tests/resources/valid/client-config-retry-none.recon")
            .unwrap();

    let mut contents = String::new();
    file.read_to_string(&mut contents).unwrap();
    let config = parse_single(&contents).unwrap();
    let config = SwimClientConfig::try_from_value(&config).unwrap();

    let expected = SwimClientConfig::new(
        DownlinkConnectionsConfig::new(
            NonZeroUsize::new(8).unwrap(),
            NonZeroUsize::new(100).unwrap(),
            NonZeroUsize::new(256).unwrap(),
            RetryStrategy::none(),
        ),
        Default::default(),
        Default::default(),
        Default::default(),
    );

    assert_eq!(config, expected)
}

fn create_full_config() -> SwimClientConfig {
    let mut dl_config = ClientDownlinksConfig::new(
        DownlinkConfig::new(
            BackpressureMode::Release {
                input_buffer_size: NonZeroUsize::new(512).unwrap(),
                bridge_buffer_size: NonZeroUsize::new(512).unwrap(),
                max_active_keys: NonZeroUsize::new(512).unwrap(),
                yield_after: NonZeroUsize::new(512).unwrap(),
            },
            Duration::from_nanos(6666666),
            10,
            OnInvalidMessage::Ignore,
            512,
        )
            .unwrap(),
    );

    dl_config.for_host(
        Url::parse("ws://127.0.0.1").unwrap(),
        DownlinkConfig::new(
            BackpressureMode::Propagate,
            Duration::from_secs(40000),
            15,
            OnInvalidMessage::Ignore,
            200,
        )
            .unwrap(),
    );

    dl_config.for_host(
        Url::parse("ws://127.0.0.2").unwrap(),
        DownlinkConfig::new(
            BackpressureMode::Propagate,
            Duration::from_secs(50000),
            25,
            OnInvalidMessage::Terminate,
            300,
        )
            .unwrap(),
    );

    dl_config.for_lane(
        &AbsolutePath::new(Url::parse("ws://192.168.0.1").unwrap(), "bar", "baz"),
        DownlinkConfig::new(
            BackpressureMode::Propagate,
            Duration::from_secs(90000),
            40,
            OnInvalidMessage::Ignore,
            100,
        )
            .unwrap(),
    );

    dl_config.for_lane(
        &AbsolutePath::new(Url::parse("ws://192.168.0.2").unwrap(), "qux", "quz"),
        DownlinkConfig::new(
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

    let config = SwimClientConfig::new(
        DownlinkConnectionsConfig::new(
            NonZeroUsize::new(8).unwrap(),
            NonZeroUsize::new(32).unwrap(),
            NonZeroUsize::new(200).unwrap(),
            RetryStrategy::interval(
                Duration::from_secs(54),
                Quantity::Finite(NonZeroUsize::new(13).unwrap()),
            ),
        ),
        RemoteConnectionsConfig::new(
            NonZeroUsize::new(20).unwrap(),
            NonZeroUsize::new(20).unwrap(),
            Duration::from_secs(60),
            Duration::from_secs(40),
            RetryStrategy::none(),
            NonZeroUsize::new(512).unwrap(),
        ),
        WebSocketConfig {
            max_send_queue: Some(15),
            max_message_size: Some(60000000),
            max_frame_size: Some(16000000),
            accept_unmasked_frames: true,
            compression: WsCompression::Deflate(Default::default()),
        },
        dl_config,
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
    let config = SwimClientConfig::try_from_value(&config).unwrap();

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
    let config = SwimClientConfig::try_from_value(&config).unwrap();

    let expected = create_full_config();

    assert_eq!(config, expected)
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
        assert_eq!(err.to_string(), "Could not process client configuration: Bad token at: 4:17")
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
        assert_eq!(err.to_string(), "Could not process client configuration: Unexpected field: 'hello'")
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
        assert_eq!(err.to_string(), "Could not process client configuration: Unexpected value kind: UInt64, expected: An attribute named 'config'.")
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
        assert_eq!(err.to_string(), "Could not process client configuration: Unexpected value kind: UInt64, expected: One of: [A value of kind Text, The end of the record body].")
    } else {
        panic!("Expected configuration parsing error!")
    }
}

#[test]
fn test_conf_from_file_unnamed_record_top() {
    let file =
        fs::File::open("src/configuration/tests/resources/invalid/unnamed-record-top.recon")
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
        fs::File::open("src/configuration/tests/resources/invalid/double-attr.recon")
            .unwrap();

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
