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

use crate::fixture::{mock, ReadError};
use crate::handshake::{UPGRADE_STR, WEBSOCKET_STR, WEBSOCKET_VERSION_STR};
use crate::{
    accept_with, Error, ErrorKind, HttpError, NoExtProvider, ProtocolRegistry, WebSocketConfig,
};
use http::header::HeaderName;
use http::{HeaderMap, HeaderValue, Request, Response, Version};
use httparse::Header;
use ratchet_ext::{Extension, ExtensionProvider};

impl From<ReadError<httparse::Error>> for Error {
    fn from(e: ReadError<httparse::Error>) -> Self {
        Error::with_cause(ErrorKind::Http, e)
    }
}

async fn exec_request(request: Request<()>) -> Result<Response<()>, Error> {
    let (mut client, server) = mock();

    client.write_request(request).await?;

    let upgrader = accept_with(
        server,
        WebSocketConfig::default(),
        NoExtProvider,
        ProtocolRegistry::default(),
    )
    .await?;

    let _upgraded = upgrader.upgrade().await?;
    client.read_response().await.map_err(Into::into)
}

#[tokio::test]
async fn valid_response() {
    let response = exec_request(valid_request()).await.unwrap();

    let expected = Response::builder()
        .status(101)
        .header(http::header::CONNECTION, "upgrade")
        .header(http::header::UPGRADE, WEBSOCKET_STR)
        .header(
            http::header::SEC_WEBSOCKET_ACCEPT,
            "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=",
        )
        .version(Version::HTTP_11)
        .body(())
        .unwrap();

    assert_response_eq(response, expected);
}

#[tokio::test]
async fn bad_request() {
    async fn t(request: Request<()>, name: HeaderName) {
        match exec_request(request).await {
            Ok(o) => panic!("Expected a test failure. Got: {:?}", o),
            Err(e) => match e.downcast_ref::<HttpError>() {
                Some(err) => {
                    assert_eq!(err, &HttpError::MissingHeader(name))
                }
                None => {
                    panic!("Expected a HTTP error. Got: {:?}", e)
                }
            },
        }
    }

    // request doesn't implement clone
    t(
        Request::builder()
            .uri("/test")
            .header(http::header::UPGRADE, WEBSOCKET_STR)
            .header(http::header::SEC_WEBSOCKET_VERSION, WEBSOCKET_VERSION_STR)
            .header(http::header::SEC_WEBSOCKET_KEY, "dGhlIHNhbXBsZSBub25jZQ==")
            .header(http::header::HOST, "localtoast")
            .body(())
            .unwrap(),
        http::header::CONNECTION,
    )
    .await;
    t(
        Request::builder()
            .uri("/test")
            .header(http::header::CONNECTION, UPGRADE_STR)
            .header(http::header::SEC_WEBSOCKET_VERSION, WEBSOCKET_VERSION_STR)
            .header(http::header::SEC_WEBSOCKET_KEY, "dGhlIHNhbXBsZSBub25jZQ==")
            .header(http::header::HOST, "localtoast")
            .body(())
            .unwrap(),
        http::header::UPGRADE,
    )
    .await;
    t(
        Request::builder()
            .uri("/test")
            .header(http::header::CONNECTION, UPGRADE_STR)
            .header(http::header::UPGRADE, WEBSOCKET_STR)
            .header(http::header::SEC_WEBSOCKET_KEY, "dGhlIHNhbXBsZSBub25jZQ==")
            .header(http::header::HOST, "localtoast")
            .body(())
            .unwrap(),
        http::header::SEC_WEBSOCKET_VERSION,
    )
    .await;
    t(
        Request::builder()
            .uri("/test")
            .header(http::header::CONNECTION, UPGRADE_STR)
            .header(http::header::UPGRADE, WEBSOCKET_STR)
            .header(http::header::SEC_WEBSOCKET_VERSION, WEBSOCKET_VERSION_STR)
            .header(http::header::HOST, "localtoast")
            .body(())
            .unwrap(),
        http::header::SEC_WEBSOCKET_KEY,
    )
    .await;
    t(
        Request::builder()
            .uri("/test")
            .header(http::header::CONNECTION, UPGRADE_STR)
            .header(http::header::UPGRADE, WEBSOCKET_STR)
            .header(http::header::SEC_WEBSOCKET_VERSION, WEBSOCKET_VERSION_STR)
            .header(http::header::SEC_WEBSOCKET_KEY, "dGhlIHNhbXBsZSBub25jZQ==")
            .body(())
            .unwrap(),
        http::header::HOST,
    )
    .await;

    let request = Request::builder()
        .uri("/test")
        .version(Version::HTTP_10)
        .header(http::header::CONNECTION, UPGRADE_STR)
        .header(http::header::UPGRADE, WEBSOCKET_STR)
        .header(http::header::SEC_WEBSOCKET_VERSION, WEBSOCKET_VERSION_STR)
        .header(http::header::SEC_WEBSOCKET_KEY, "dGhlIHNhbXBsZSBub25jZQ==")
        .header(http::header::HOST, "localtoast")
        .body(())
        .unwrap();

    match exec_request(request).await {
        Ok(o) => panic!("Expected a test failure. Got: {:?}", o),
        Err(e) => match e.downcast_ref::<HttpError>() {
            Some(err) => {
                assert_eq!(err, &HttpError::HttpVersion(Some(0)));
            }
            None => {
                panic!("Expected a HTTP error. Got: {:?}", e)
            }
        },
    }

    let request = Request::builder()
        .uri("/test")
        .method("post")
        .header(http::header::CONNECTION, UPGRADE_STR)
        .header(http::header::UPGRADE, WEBSOCKET_STR)
        .header(http::header::SEC_WEBSOCKET_VERSION, WEBSOCKET_VERSION_STR)
        .header(http::header::SEC_WEBSOCKET_KEY, "dGhlIHNhbXBsZSBub25jZQ==")
        .header(http::header::HOST, "localtoast")
        .body(())
        .unwrap();

    match exec_request(request).await {
        Ok(o) => panic!("Expected a test failure. Got: {:?}", o),
        Err(e) => match e.downcast_ref::<HttpError>() {
            Some(err) => {
                assert_eq!(err, &HttpError::HttpMethod(Some("post".to_string())));
            }
            None => {
                panic!("Expected a HTTP error. Got: {:?}", e)
            }
        },
    }
}

fn assert_response_eq(left: Response<()>, right: Response<()>) {
    assert!(left.version().eq(&right.version()));
    assert!(left.status().eq(&right.status()));
    assert!(left.headers().eq(&right.headers()));
}

#[derive(Debug, thiserror::Error)]
#[error("Error")]
struct ExtErr;
impl From<ExtErr> for Error {
    fn from(e: ExtErr) -> Self {
        Error::with_cause(ErrorKind::Extension, e)
    }
}

struct BadExtProvider;
impl ExtensionProvider for BadExtProvider {
    type Extension = Ext;
    type Error = ExtErr;

    fn apply_headers(&self, _headers: &mut HeaderMap) {}

    fn negotiate_client(&self, _headers: &[Header]) -> Result<Self::Extension, Self::Error> {
        panic!("Unexpected client negotitation request")
    }

    fn negotiate_server(
        &self,
        _headers: &[Header],
    ) -> Result<(Self::Extension, Option<HeaderValue>), Self::Error> {
        Err(ExtErr)
    }
}

#[derive(Debug)]
struct Ext;
impl Extension for Ext {
    fn encode<A>(&mut self, _payload: A)
    where
        A: AsMut<[u8]>,
    {
    }

    fn decode<A>(&mut self, _payload: A)
    where
        A: AsMut<[u8]>,
    {
    }
}

fn valid_request() -> Request<()> {
    Request::builder()
        .uri("/test")
        .header(http::header::CONNECTION, UPGRADE_STR)
        .header(http::header::UPGRADE, WEBSOCKET_STR)
        .header(http::header::SEC_WEBSOCKET_VERSION, WEBSOCKET_VERSION_STR)
        .header(http::header::SEC_WEBSOCKET_KEY, "dGhlIHNhbXBsZSBub25jZQ==")
        .header(http::header::HOST, "localtoast")
        .body(())
        .unwrap()
}

#[tokio::test]
async fn bad_extension() {
    let (mut client, server) = mock();
    client.write_request(valid_request()).await.unwrap();

    let result = accept_with(
        server,
        WebSocketConfig::default(),
        BadExtProvider,
        ProtocolRegistry::default(),
    )
    .await;

    match result {
        Ok(_) => {
            panic!("Expected the connection to fail")
        }
        Err(e) => {
            if e.downcast_ref::<ExtErr>().is_none() {
                panic!("{:?}", e);
            }
        }
    }
}
