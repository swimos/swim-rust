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

use lazy_static::lazy_static;
use std::{collections::HashMap, fmt::Formatter};
use thiserror::Error;

#[derive(Default, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Method(MethodInner);

#[derive(Default, Clone, Copy, PartialEq, Eq, Hash)]
enum MethodInner {
    #[default]
    Get,
    Head,
    Post,
    Put,
    Delete,
    Connect,
    Options,
    Trace,
}

impl Method {
    pub const GET: Method = Method(MethodInner::Get);
    pub const HEAD: Method = Method(MethodInner::Head);
    pub const POST: Method = Method(MethodInner::Post);
    pub const PUT: Method = Method(MethodInner::Put);
    pub const DELETE: Method = Method(MethodInner::Delete);
    pub const CONNECT: Method = Method(MethodInner::Connect);
    pub const OPTIONS: Method = Method(MethodInner::Options);
    pub const TRACE: Method = Method(MethodInner::Trace);
}

impl From<Method> for http::Method {
    fn from(value: Method) -> Self {
        match value.0 {
            MethodInner::Get => http::Method::GET,
            MethodInner::Head => http::Method::HEAD,
            MethodInner::Post => http::Method::POST,
            MethodInner::Put => http::Method::PUT,
            MethodInner::Delete => http::Method::DELETE,
            MethodInner::Connect => http::Method::CONNECT,
            MethodInner::Options => http::Method::OPTIONS,
            MethodInner::Trace => http::Method::TRACE,
        }
    }
}

lazy_static! {
    static ref METHODS: HashMap<http::Method, Method> = {
        let mut m = HashMap::new();
        m.insert(http::Method::GET, Method::GET);
        m.insert(http::Method::HEAD, Method::HEAD);
        m.insert(http::Method::POST, Method::POST);
        m.insert(http::Method::DELETE, Method::DELETE);
        m.insert(http::Method::CONNECT, Method::CONNECT);
        m.insert(http::Method::OPTIONS, Method::OPTIONS);
        m.insert(http::Method::TRACE, Method::TRACE);
        m
    };
}

impl TryFrom<http::Method> for Method {
    type Error = UnsupportedMethod;

    fn try_from(value: http::Method) -> Result<Self, Self::Error> {
        METHODS
            .get(&value)
            .copied()
            .ok_or_else(|| UnsupportedMethod(value.to_string()))
    }
}

#[derive(Debug, Error)]
#[error("HTTP method '{0}' is not supported.")]
pub struct UnsupportedMethod(String);

impl std::fmt::Display for Method {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self.0 {
            MethodInner::Get => write!(f, "GET"),
            MethodInner::Head => write!(f, "HEAD"),
            MethodInner::Post => write!(f, "POST"),
            MethodInner::Put => write!(f, "PUT"),
            MethodInner::Delete => write!(f, "DELETE"),
            MethodInner::Connect => write!(f, "CONNECT"),
            MethodInner::Options => write!(f, "OPTIONS"),
            MethodInner::Trace => write!(f, "TRACE"),
        }
    }
}

impl std::fmt::Debug for Method {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self, f)
    }
}
