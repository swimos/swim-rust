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

#[cfg(test)]
mod tests;

use http::uri::InvalidUri;
use http::Uri;
use std::borrow::Borrow;
use std::convert::TryFrom;
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::str::FromStr;
use std::str::Split;

/// A restricted URI type that can only represent relative URIs.
#[derive(Debug, Default, Clone, PartialEq, Eq, Hash)]
pub struct RelativeUri(Uri);

#[derive(Debug, PartialEq, Eq)]
pub struct UriIsAbsolute(pub Uri);

impl Display for UriIsAbsolute {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "'{}' is an absolute URI.", self.0)
    }
}

impl Error for UriIsAbsolute {}

impl RelativeUri {
    pub fn new(uri: Uri) -> Result<RelativeUri, UriIsAbsolute> {
        if uri.scheme().is_some() || uri.authority().is_some() {
            Err(UriIsAbsolute(uri))
        } else {
            Ok(RelativeUri(uri))
        }
    }

    pub fn as_uri(&self) -> &Uri {
        &self.0
    }

    pub fn path(&self) -> &str {
        self.0.path()
    }

    pub fn query(&self) -> Option<&str> {
        self.0.query()
    }
}

impl Display for RelativeUri {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug)]
pub enum BadRelativeUri {
    Invalid(InvalidUri),
    Absolute(UriIsAbsolute),
}

impl Display for BadRelativeUri {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            BadRelativeUri::Invalid(err) => err.fmt(f),
            BadRelativeUri::Absolute(err) => err.fmt(f),
        }
    }
}

impl Error for BadRelativeUri {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            BadRelativeUri::Invalid(err) => Some(err),
            BadRelativeUri::Absolute(err) => Some(err),
        }
    }
}

impl FromStr for RelativeUri {
    type Err = BadRelativeUri;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        s.parse()
            .map_err(BadRelativeUri::Invalid)
            .and_then(|uri| RelativeUri::new(uri).map_err(BadRelativeUri::Absolute))
    }
}

impl<'a> TryFrom<&'a str> for RelativeUri {
    type Error = BadRelativeUri;

    fn try_from(value: &'a str) -> Result<Self, Self::Error> {
        value.parse()
    }
}

impl<'a> TryFrom<String> for RelativeUri {
    type Error = BadRelativeUri;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Uri::try_from(value)
            .map_err(BadRelativeUri::Invalid)
            .and_then(|uri| RelativeUri::new(uri).map_err(BadRelativeUri::Absolute))
    }
}

impl TryFrom<Uri> for RelativeUri {
    type Error = UriIsAbsolute;

    fn try_from(value: Uri) -> Result<Self, Self::Error> {
        RelativeUri::new(value)
    }
}

impl PartialEq<str> for RelativeUri {
    fn eq(&self, other: &str) -> bool {
        self.0 == other
    }
}

impl<'a> PartialEq<&'a str> for RelativeUri {
    fn eq(&self, other: &&'a str) -> bool {
        self.0 == *other
    }
}

impl PartialEq<String> for RelativeUri {
    fn eq(&self, other: &String) -> bool {
        self.0 == other.as_str()
    }
}

impl PartialEq<Uri> for RelativeUri {
    fn eq(&self, other: &Uri) -> bool {
        self.0 == *other
    }
}

impl From<RelativeUri> for Uri {
    fn from(uri: RelativeUri) -> Self {
        uri.0
    }
}

impl Borrow<Uri> for RelativeUri {
    fn borrow(&self) -> &Uri {
        &self.0
    }
}

pub trait UriPathSegments {
    fn path_segments(&self) -> Option<Split<char>>;
}

impl UriPathSegments for Uri {
    fn path_segments(&self) -> Option<Split<char>> {
        let path = self.path();
        if path.starts_with('/') {
            Some(path[1..].split('/'))
        } else {
            None
        }
    }
}
