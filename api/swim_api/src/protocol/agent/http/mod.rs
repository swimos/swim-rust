// Copyright 2015-2023 Swim Inc.
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

use bytes::{Buf, BufMut, Bytes, BytesMut};
use http::uri::InvalidUri;
use swim_model::http::{
    Header, HeaderNameDecodeError, HttpRequest, Method, MethodDecodeError, Uri, Version,
    VersionDecodeError,
};
use thiserror::Error;
use tokio_util::codec::{Decoder, Encoder};

pub struct HttpRequestMessage {
    pub request_id: u64,
    pub request: HttpRequest<Bytes>,
}

impl HttpRequestMessage {
    fn encoded_len(&self) -> (usize, usize) {
        let HttpRequestMessage {
            request:
                HttpRequest {
                    uri,
                    headers,
                    payload,
                    ..
                },
            ..
        } = self;
        let headers_total: usize = headers.iter().map(Header::encoded_len).sum();
        let total = ID_LEN
            + Method::ENCODED_LENGTH
            + Version::ENCODED_LENGTH
            + URI_LEN_LEN
            + uri_len(uri)
            + HEADERS_LEN_LEN
            + PAYLOAD_LEN_LEN
            + payload.len()
            + headers_total;
        (headers_total, total)
    }
}

pub struct HttpRequestMessageCodec;
const ID_LEN: usize = 8;
const URI_LEN_LEN: usize = 2;
const PAYLOAD_LEN_LEN: usize = 8;
const HEADERS_LEN_LEN: usize = 8;
const BASE_LEN: usize = ID_LEN
    + HEADERS_LEN_LEN
    + Method::ENCODED_LENGTH
    + Version::ENCODED_LENGTH
    + URI_LEN_LEN
    + PAYLOAD_LEN_LEN;

impl Encoder<HttpRequestMessage> for HttpRequestMessageCodec {
    type Error = std::io::Error;

    fn encode(&mut self, item: HttpRequestMessage, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let (headers_total, total) = item.encoded_len();
        dst.reserve(total);
        let HttpRequestMessage {
            request_id,
            request:
                HttpRequest {
                    method,
                    version,
                    uri,
                    headers,
                    payload,
                },
        } = item;
        dst.put_u64(request_id);
        method.encode(dst);
        version.encode(dst);
        let uri_len = uri_len(&uri) as u16;
        dst.put_u64(headers_total as u64);
        dst.put_u16(uri_len);
        dst.put_u64(payload.len() as u64);
        encode_uri(&uri, dst);
        for header in headers {
            header.encode(dst);
        }
        dst.put(payload);
        Ok(())
    }
}

#[derive(Debug, Error)]
pub enum HttpRequestMessageDecodeError {
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    BadMethod(#[from] MethodDecodeError),
    #[error(transparent)]
    BadVersion(#[from] VersionDecodeError),
    #[error(transparent)]
    InvalidUri(#[from] InvalidUri),
    #[error(transparent)]
    InvalidHeaders(#[from] HeaderNameDecodeError),
    #[error("Headers section of a request was incomplete.")]
    IncompleteHeaders,
}

impl Decoder for HttpRequestMessageCodec {
    type Item = HttpRequestMessage;

    type Error = HttpRequestMessageDecodeError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        let mut buf = src.as_ref();
        if buf.remaining() < BASE_LEN {
            return Ok(None);
        }
        let request_id = buf.get_u64();
        let method = Method::decode(&mut buf)?.expect("Insufficient bytes for method.");
        let version = Version::decode(&mut buf)?.expect("Insufficient bytes for method.");
        let headers_total = buf.get_u64() as usize;
        let uri_len = buf.get_u16() as usize;
        let payload_len = buf.get_u64() as usize;
        if buf.remaining() < headers_total + uri_len + payload_len {
            return Ok(None);
        }
        src.advance(BASE_LEN);
        let uri = decode_uri(src, uri_len)?;
        let mut headers_bytes = src.split_to(headers_total);
        let headers = decode_headers(&mut headers_bytes)?;
        let payload = src.split_to(payload_len).freeze();
        Ok(Some(HttpRequestMessage {
            request_id,
            request: HttpRequest {
                method,
                version,
                uri,
                headers,
                payload,
            },
        }))
    }
}

fn uri_len(uri: &Uri) -> usize {
    let mut n = 0;
    if let Some(s) = uri.scheme() {
        n += s.as_str().len() + 3;
    }
    if let Some(a) = uri.authority() {
        n += a.as_str().len();
    }
    if let Some(p) = uri.path_and_query() {
        n += p.as_str().len();
    }
    n
}

fn decode_uri(src: &mut BytesMut, uri_len: usize) -> Result<Uri, InvalidUri> {
    src.split_to(uri_len).freeze();
    Uri::from_maybe_shared(src.split_to(uri_len).freeze())
}

fn encode_uri(uri: &Uri, dst: &mut BytesMut) {
    if let Some(s) = uri.scheme() {
        dst.put(s.as_str().as_bytes());
        dst.put(b"://".as_slice());
    }
    if let Some(a) = uri.authority() {
        dst.put(a.as_str().as_bytes());
    }
    if let Some(p) = uri.path_and_query() {
        dst.put(p.as_str().as_bytes());
    }
}

fn decode_headers(src: &mut BytesMut) -> Result<Vec<Header>, HttpRequestMessageDecodeError> {
    let mut headers = vec![];
    while !src.is_empty() {
        let header =
            Header::decode(src)?.ok_or(HttpRequestMessageDecodeError::IncompleteHeaders)?;
        headers.push(header);
    }
    Ok(headers)
}
