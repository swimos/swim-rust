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

use futures::task::{Context, Poll};
use futures::{Sink, Stream};
use hamcrest2::assert_that;
use hamcrest2::prelude::*;
use tokio::macros::support::Pin;
use tokio_tungstenite::tungstenite::protocol::Message;

use crate::connections::ConnectionError;

use super::async_factory::*;
use super::WebsocketFactory;

#[derive(Debug, PartialEq, Eq)]
struct TestSink(url::Url);

#[derive(Debug, PartialEq, Eq)]
struct TestStream(url::Url);

impl Stream for TestStream {
    type Item = Result<Message, ConnectionError>;

    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Ready(None)
    }
}

impl Sink<Message> for TestSink {
    type Error = ConnectionError;

    fn poll_ready(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn start_send(self: Pin<&mut Self>, _item: Message) -> Result<(), Self::Error> {
        Ok(())
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
}

async fn open_conn(url: url::Url) -> Result<(TestSink, TestStream), ConnectionError> {
    if url.scheme() == "fail" {
        Err(ConnectionError::ConnectError(None))
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
    let result = fac.connect(url.clone()).await;
    assert_that!(&result, ok());
    let (snk, stream) = result.unwrap();
    assert_that!(&snk.0, eq(&url));
    assert_that!(&stream.0, eq(&url));
}

#[tokio::test]
async fn fail_to_open() {
    let url = bad_url();
    let mut fac = make_fac().await;
    let result = fac.connect(url.clone()).await;
    assert_that!(&result, err());
    let err = result.err().unwrap();
    assert_that!(err, eq(ConnectionError::ConnectError(None)));
}
