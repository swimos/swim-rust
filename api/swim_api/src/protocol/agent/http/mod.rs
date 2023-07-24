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
    Header, HeaderNameDecodeError, HttpRequest, HttpResponse, InvalidStatusCode, Method,
    MethodDecodeError, StatusCode, Uri, Version, VersionDecodeError,
};
use thiserror::Error;
use tokio_util::codec::{Decoder, Encoder};

#[cfg(test)]
mod tests;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpRequestMessage {
    pub request_id: u64,
    pub request: HttpRequest<Bytes>,
}

pub struct HttpResponseMessage<T> {
    pub request_id: u64,
    pub response: HttpResponse<T>,
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

impl From<HeaderNameDecodeError> for HeadersDecodeError {
    fn from(value: HeaderNameDecodeError) -> Self {
        HeadersDecodeError::InvalidHeaders(value)
    }
}

impl From<HeadersDecodeError> for HttpRequestMessageDecodeError {
    fn from(value: HeadersDecodeError) -> Self {
        match value {
            HeadersDecodeError::InvalidHeaders(e) => {
                HttpRequestMessageDecodeError::InvalidHeaders(e)
            }
            HeadersDecodeError::IncompleteHeaders => {
                HttpRequestMessageDecodeError::IncompleteHeaders
            }
        }
    }
}

enum HeadersDecodeError {
    InvalidHeaders(HeaderNameDecodeError),
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

fn decode_headers(src: &mut BytesMut) -> Result<Vec<Header>, HeadersDecodeError> {
    let mut headers = vec![];
    while !src.is_empty() {
        let header = Header::decode(src)?.ok_or(HeadersDecodeError::IncompleteHeaders)?;
        headers.push(header);
    }
    Ok(headers)
}

#[derive(Default, Debug)]
pub struct HttpResponseMessageEncoder<Inner> {
    inner: Inner,
}

impl<Inner> HttpResponseMessageEncoder<Inner> {
    pub fn new(payload_encoder: Inner) -> Self {
        HttpResponseMessageEncoder {
            inner: payload_encoder,
        }
    }
}

impl<T, Inner> Encoder<HttpResponseMessage<T>> for HttpResponseMessageEncoder<Inner>
where
    Inner: Encoder<T>,
{
    type Error = Inner::Error;

    fn encode(
        &mut self,
        item: HttpResponseMessage<T>,
        dst: &mut BytesMut,
    ) -> Result<(), Self::Error> {
        let HttpResponseMessage {
            request_id,
            response:
                HttpResponse {
                    status_code,
                    version,
                    headers,
                    payload,
                },
        } = item;
        dst.put_u64(request_id);
        dst.put_u16(status_code.as_u16());
        version.encode(dst);
        let headers_total: usize = headers.iter().map(Header::encoded_len).sum();
        dst.put_u64(headers_total as u64);
        for header in headers {
            header.encode(dst);
        }
        self.inner.encode(payload, dst)
    }
}

const STATUS_CODE_LEN: usize = 2;
const META_SIZE: usize = ID_LEN + STATUS_CODE_LEN + Version::ENCODED_LENGTH + HEADERS_LEN_LEN;

#[derive(Default, Debug)]
enum ResponseDecoderState {
    #[default]
    Metadata,
    Headers {
        request_id: u64,
        status_code: StatusCode,
        version: Version,
        headers_total: usize,
    },
    Payload {
        request_id: u64,
        status_code: StatusCode,
        version: Version,
        headers: Vec<Header>,
    },
}

#[derive(Default, Debug)]
pub struct HttpResponseMessageDecoder<Inner> {
    inner: Inner,
    state: ResponseDecoderState,
}

impl<Inner> HttpResponseMessageDecoder<Inner> {
    pub fn new(payload_decoder: Inner) -> Self {
        HttpResponseMessageDecoder {
            inner: payload_decoder,
            state: Default::default(),
        }
    }
}

#[derive(Debug, Error)]
pub enum HttpResponseMessageDecodeError<E> {
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error("An error occurred decoding the payload: {0}")]
    Payload(E),
    #[error("The HTTP status code was invalid.")]
    BadStatusCode(#[from] InvalidStatusCode),
    #[error("The HTTP version was invalid.")]
    BadVersion(#[from] VersionDecodeError),
    #[error(transparent)]
    InvalidHeaders(#[from] HeaderNameDecodeError),
    #[error("Headers section of a request was incomplete.")]
    IncompleteHeaders,
}

impl<E> From<HeadersDecodeError> for HttpResponseMessageDecodeError<E> {
    fn from(value: HeadersDecodeError) -> Self {
        match value {
            HeadersDecodeError::InvalidHeaders(e) => {
                HttpResponseMessageDecodeError::InvalidHeaders(e)
            }
            HeadersDecodeError::IncompleteHeaders => {
                HttpResponseMessageDecodeError::IncompleteHeaders
            }
        }
    }
}

impl<Inner> Decoder for HttpResponseMessageDecoder<Inner>
where
    Inner: Decoder,
    Inner::Error: std::error::Error,
{
    type Item = HttpResponseMessage<Inner::Item>;

    type Error = HttpResponseMessageDecodeError<Inner::Error>;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        let HttpResponseMessageDecoder { inner, state } = self;
        loop {
            match std::mem::take(state) {
                ResponseDecoderState::Metadata => {
                    if src.remaining() < META_SIZE {
                        break Ok(None);
                    }
                    let request_id = src.get_u64();
                    let status_code = match StatusCode::try_from(src.get_u16()) {
                        Ok(status_code) => status_code,
                        Err(e) => break Err(e.into()),
                    };
                    let version = match Version::decode(src) {
                        Ok(Some(version)) => version,
                        Err(e) => break Err(e.into()),
                        _ => unreachable!(),
                    };
                    let headers_total = src.get_u64();
                    *state = ResponseDecoderState::Headers {
                        request_id,
                        status_code,
                        version,
                        headers_total: headers_total as usize,
                    };
                }
                ResponseDecoderState::Headers {
                    request_id,
                    status_code,
                    version,
                    headers_total,
                } => {
                    if src.remaining() < headers_total {
                        *state = ResponseDecoderState::Headers {
                            request_id,
                            status_code,
                            version,
                            headers_total,
                        };
                        break Ok(None);
                    } else {
                        let mut headers_bytes = src.split_to(headers_total);
                        let headers = decode_headers(&mut headers_bytes)?;
                        *state = ResponseDecoderState::Payload {
                            request_id,
                            status_code,
                            version,
                            headers,
                        };
                    }
                }
                ResponseDecoderState::Payload {
                    request_id,
                    status_code,
                    version,
                    headers,
                } => match inner.decode(src) {
                    Ok(Some(payload)) => {
                        break Ok(Some(HttpResponseMessage {
                            request_id,
                            response: HttpResponse {
                                status_code,
                                version,
                                headers,
                                payload,
                            },
                        }))
                    }
                    Ok(_) => {
                        *state = ResponseDecoderState::Payload {
                            request_id,
                            status_code,
                            version,
                            headers,
                        };
                        break Ok(None);
                    }
                    Err(err) => break Err(HttpResponseMessageDecodeError::Payload(err)),
                },
            }
        }
    }
}
