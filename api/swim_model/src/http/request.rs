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

use http::request::Parts;
use thiserror::Error;

use super::{Uri, Version, Method, Header, UnsupportedMethod, UnsupportedVersion, HeaderName, HeaderValue};
pub struct HttpRequest<T> {
    pub method: Method,
    pub version: Version,
    pub uri: Uri,
    pub headers: Vec<Header>,
    pub payload: T,
}

impl HttpRequest<()> {

    pub fn get(uri: Uri) -> Self {
        HttpRequest { 
            method: Method::GET, 
            version: Version::default(), 
            uri, 
            headers: vec![], 
            payload: () 
        }
    }

    pub fn delete(uri: Uri) -> Self {
        HttpRequest { 
            method: Method::DELETE, 
            version: Version::default(), 
            uri, 
            headers: vec![], 
            payload: () 
        }
    }
    

}

impl<T> HttpRequest<T> {

    pub fn put(uri: Uri, payload: T) -> Self {
        HttpRequest { 
            method: Method::PUT, 
            version: Version::default(), 
            uri, 
            headers: vec![], 
            payload,
        }
    }

    pub fn post(uri: Uri, payload: T) -> Self {
        HttpRequest { 
            method: Method::POST, 
            version: Version::default(), 
            uri, 
            headers: vec![], 
            payload,
        }
    }

}

#[derive(Debug, Error)]
pub enum InvalidRequest {
    #[error("Invalid method: {0}")]
    BadMethod(#[from] UnsupportedMethod),
    #[error("Invalid version: {0}")]
    BadVersion(#[from] UnsupportedVersion)
}

impl<T> TryFrom<http::Request<T>> for HttpRequest<T> {
    type Error = InvalidRequest;

    fn try_from(value: http::Request<T>) -> Result<Self, Self::Error> {
        let (Parts { method, uri, version, headers, .. }, payload) = value.into_parts();
        let mut converted_headers = vec![];
        for (name, value) in &headers {
            let header = Header {
                name: HeaderName::from(name.as_str()),
                value: HeaderValue::from(value.as_bytes()),
            };
            converted_headers.push(header);
        }
        Ok(HttpRequest {
            method: method.try_into()?,
            version: version.try_into()?,
            uri,
            headers: converted_headers,
            payload,
        })
    }
}