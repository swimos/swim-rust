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

use super::async_factory::*;
use crate::connections::factory::HostConfig;
use bytes::BytesMut;
use ratchet::{NegotiatedExtension, NoExt, Role, WebSocket, WebSocketConfig};
use swim_runtime::error::{ConnectionError, HttpError};
use swim_runtime::ws::{CompressionSwitcherProvider, Protocol};
use swim_utilities::io::void::{void, Void};

async fn open_conn(
    url: url::Url,
    _config: HostConfig,
) -> Result<WebSocket<Void, NoExt>, ConnectionError> {
    if url.scheme() == "fail" {
        Err(ConnectionError::Http(HttpError::invalid_url(
            url.to_string(),
            None,
        )))
    } else {
        Ok(ratchet::WebSocket::from_upgraded(
            WebSocketConfig::default(),
            void(),
            NegotiatedExtension::from(None),
            BytesMut::default(),
            Role::Client,
        ))
    }
}

async fn make_fac() -> AsyncFactory<Void, NoExt> {
    AsyncFactory::new(5, open_conn).await
}

fn good_url() -> url::Url {
    url::Url::parse("good://127.0.0.1").unwrap()
}

fn bad_url() -> url::Url {
    url::Url::parse("fail://127.0.0.1").unwrap()
}

#[tokio::test]
async fn successfully_open() {
    let url = good_url();
    let mut fac = make_fac().await;
    let result = fac
        .connect_using(
            url.clone(),
            HostConfig {
                protocol: Protocol::PlainText,
                compression_level: CompressionSwitcherProvider::Off,
            },
        )
        .await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn fail_to_open() {
    let url = bad_url();
    let mut fac = make_fac().await;
    let result = fac
        .connect_using(
            url.clone(),
            HostConfig {
                protocol: Protocol::PlainText,
                compression_level: CompressionSwitcherProvider::Off,
            },
        )
        .await;
    assert!(result.is_err());
    let err = result.err().unwrap();

    assert_eq!(
        err,
        ConnectionError::Http(HttpError::invalid_url("fail://127.0.0.1".into(), None))
    );
}
