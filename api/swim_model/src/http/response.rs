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

use std::str::FromStr;

use thiserror::Error;

use super::{Header, HeaderName, HeaderValue, StatusCode, Version};

pub struct HttpResponse<T> {
    pub status_code: StatusCode,
    pub version: Version,
    pub headers: Vec<Header>,
    pub payload: T,
}

#[derive(Debug, Error)]
pub enum InvalidResponse {
    #[error("Invalid header name.")]
    BadHeaderName(HeaderName),
    #[error("Invalid header value.")]
    BadHeaderValue(HeaderValue),
}

impl<T> TryFrom<HttpResponse<T>> for http::Response<T> {
    type Error = InvalidResponse;

    fn try_from(value: HttpResponse<T>) -> Result<Self, Self::Error> {
        let HttpResponse {
            status_code,
            version,
            headers,
            payload,
        } = value;

        let mut response = http::Response::new(payload);
        *response.status_mut() = status_code.into();
        *response.version_mut() = version.into();
        for Header { name, value } in headers {
            let name = match http::header::HeaderName::from_str(name.as_str()) {
                Ok(name) => name,
                Err(_) => return Err(InvalidResponse::BadHeaderName(name)),
            };
            let value_bytes = value.into_bytes();
            let value = match http::HeaderValue::from_maybe_shared(value_bytes.clone()) {
                Ok(value) => value,
                Err(_) => {
                    return Err(InvalidResponse::BadHeaderValue(HeaderValue::new(
                        value_bytes,
                    )))
                }
            };
            response.headers_mut().append(name, value);
        }
        Ok(response)
    }
}
