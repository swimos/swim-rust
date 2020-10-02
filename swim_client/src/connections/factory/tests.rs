// Copyright 2015-2020 SWIM.AI inc.
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
use crate::connections::factory::tungstenite::{CompressionConfig, HostConfig};
use futures::task::{Context, Poll};
use futures::{Sink, Stream};
use std::pin::Pin;
use swim_common::ws::error::ConnectionError;
use swim_common::ws::{Protocol, WsMessage};

#[derive(Debug, PartialEq, Eq)]
struct TestSink(url::Url);

#[derive(Debug, PartialEq, Eq)]
struct TestStream(url::Url);

impl Stream for TestStream {
    type Item = Result<WsMessage, ConnectionError>;

    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Ready(None)
    }
}

impl Sink<WsMessage> for TestSink {
    type Error = ConnectionError;

    fn poll_ready(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn start_send(self: Pin<&mut Self>, _item: WsMessage) -> Result<(), Self::Error> {
        Ok(())
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
}

async fn open_conn(
    url: url::Url,
    _config: HostConfig,
) -> Result<(TestSink, TestStream), ConnectionError> {
    if url.scheme() == "fail" {
        Err(ConnectionError::ConnectError)
    } else {
        Ok((TestSink(url.clone()), TestStream(url)))
    }
}

async fn make_fac() -> AsyncFactory<TestSink, TestStream> {
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
                compression_config: CompressionConfig::Uncompressed,
            },
        )
        .await;
    assert!(result.is_ok());
    let (snk, stream) = result.unwrap();
    assert_eq!(snk.0, url);
    assert_eq!(stream.0, url);
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
                compression_config: CompressionConfig::Uncompressed,
            },
        )
        .await;
    assert!(result.is_err());
    let err = result.err().unwrap();
    assert_eq!(err, ConnectionError::ConnectError);
}
