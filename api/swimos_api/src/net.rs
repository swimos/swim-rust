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

use std::{
    fmt::{Display, Formatter},
    str::FromStr,
};

use http::{uri::InvalidUri, Uri};
use thiserror::Error;

#[derive(Debug, PartialEq, Eq, Error)]
pub enum BadUrl {
    #[error("Malformed URL: {0}")]
    BadUrl(String),
    #[error("A WARP URL must have a scheme.")]
    MissingScheme,
    #[error("{0} is not a valid WARP scheme.")]
    BadScheme(String),
    #[error("The URL did not contain a valid host.")]
    NoHost,
}

impl From<InvalidUri> for BadUrl {
    fn from(value: InvalidUri) -> Self {
        BadUrl::BadUrl(value.to_string())
    }
}

/// Supported websocket schemes
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Scheme {
    Ws,
    Wss,
}

impl TryFrom<&str> for Scheme {
    type Error = BadUrl;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "ws" | "swimos" | "warp" => Ok(Scheme::Ws),
            "wss" | "swims" | "warps" => Ok(Scheme::Wss),
            _ => Err(BadUrl::BadScheme(value.to_owned())),
        }
    }
}

impl Scheme {
    /// Get the default port for the schemes.
    pub const fn get_default_port(&self) -> u16 {
        match self {
            Scheme::Ws => 80,
            Scheme::Wss => 443,
        }
    }

    /// Return if the scheme is secure.
    pub const fn is_secure(&self) -> bool {
        match self {
            Scheme::Ws => false,
            Scheme::Wss => true,
        }
    }
}

impl Display for Scheme {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Scheme::Ws => {
                write!(f, "ws")
            }
            Scheme::Wss => {
                write!(f, "wss")
            }
        }
    }
}

/// A combination of host name and port to be used as a key into the routing table.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SchemeHostPort(pub Scheme, pub String, pub u16);

impl SchemeHostPort {
    pub fn new(scheme: Scheme, host: String, port: u16) -> Self {
        SchemeHostPort(scheme, host, port)
    }

    pub fn scheme(&self) -> &Scheme {
        &self.0
    }

    pub fn host(&self) -> &String {
        &self.1
    }

    pub fn port(&self) -> u16 {
        self.2
    }
}

impl Display for SchemeHostPort {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let SchemeHostPort(scheme, host, port) = self;
        write!(f, "{}://{}:{}", scheme, host, port)
    }
}

impl FromStr for SchemeHostPort {
    type Err = BadUrl;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let uri = s.parse::<Uri>()?;

        let scheme = if let Some(scheme_part) = uri.scheme_str() {
            Scheme::try_from(scheme_part).map_err(|_| BadUrl::BadScheme(scheme_part.to_string()))?
        } else {
            return Err(BadUrl::MissingScheme);
        };

        match (uri.host(), uri.port_u16()) {
            (Some(host_str), Some(port)) => {
                Ok(SchemeHostPort::new(scheme, host_str.to_owned(), port))
            }
            (Some(host_str), _) => {
                let default_port = scheme.get_default_port();
                Ok(SchemeHostPort::new(
                    scheme,
                    host_str.to_owned(),
                    default_port,
                ))
            }
            _ => Err(BadUrl::NoHost),
        }
    }
}
