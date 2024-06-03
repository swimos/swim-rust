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
    hash::Hash,
    str::FromStr,
};

use nom::Finish;

use self::parser::RouteUriParts;

mod parser;

#[cfg(test)]
mod tests;

/// A URI defining a Swim route. This is a URI that lacks an authority and may or may not be relative.
#[derive(Debug, PartialEq, Eq, Hash, Clone, PartialOrd, Ord)]
pub struct RouteUri {
    representation: String,
    scheme_len: Option<usize>,
    path_len: usize,
}

impl Default for RouteUri {
    fn default() -> Self {
        Self {
            representation: ".".to_string(),
            scheme_len: None,
            path_len: 1,
        }
    }
}

impl Display for RouteUri {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.representation)
    }
}

impl RouteUri {
    fn new(original: String, scheme_len: Option<usize>, path_len: usize) -> Self {
        RouteUri {
            representation: original,
            scheme_len,
            path_len,
        }
    }

    /// The scheme of the URI, if it has one.
    pub fn scheme(&self) -> Option<&str> {
        let RouteUri {
            representation,
            scheme_len,
            ..
        } = self;
        scheme_len.map(|n| &representation.as_str()[0..n])
    }

    /// The complete path of the URI (including escaped characters).
    pub fn path(&self) -> &str {
        let RouteUri {
            representation,
            scheme_len,
            path_len,
        } = self;
        let offset = if let Some(n) = scheme_len { *n + 1 } else { 0 };
        let end = offset + *path_len;
        &representation[offset..end]
    }

    /// The query of the URI.
    /// TODO Implement this.
    pub fn query(&self) -> Option<&str> {
        None
    }

    /// The fragment of the URI.
    /// TODO Implement this.
    pub fn fragment(&self) -> Option<&str> {
        None
    }

    pub fn as_str(&self) -> &str {
        self.representation.as_str()
    }

    /// Returns an iterator that will yield each segment in the path.
    pub fn path_iter(&self) -> impl Iterator<Item = &str> {
        PathSegmentIterator(self.path())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InvalidRouteUri(String);

impl InvalidRouteUri {
    pub fn new(bad_uri: String) -> Self {
        InvalidRouteUri(bad_uri)
    }
}

impl Display for InvalidRouteUri {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "'{}' is not a valid route URI.", &self.0)
    }
}

impl std::error::Error for InvalidRouteUri {}

fn to_offsets(parts: RouteUriParts<'_>) -> (Option<usize>, usize) {
    let RouteUriParts { scheme, path } = parts;
    let scheme_len = scheme.map(|span| span.len());
    let path_len = path.len();
    (scheme_len, path_len)
}

impl FromStr for RouteUri {
    type Err = InvalidRouteUri;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let span = parser::Span::new(s);
        parser::route_uri(span)
            .finish()
            .map(|(_, parts)| {
                let (scheme_len, path_len) = to_offsets(parts);
                RouteUri::new(s.to_owned(), scheme_len, path_len)
            })
            .map_err(|_| InvalidRouteUri(s.to_owned()))
    }
}

impl TryFrom<&str> for RouteUri {
    type Error = InvalidRouteUri;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::from_str(value)
    }
}

impl TryFrom<String> for RouteUri {
    type Error = InvalidRouteUri;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        let span = parser::Span::new(value.as_str());
        let result = parser::route_uri(span)
            .finish()
            .map(|(_, parts)| to_offsets(parts));
        match result {
            Ok((scheme_len, path_len)) => Ok(RouteUri::new(value, scheme_len, path_len)),
            _ => Err(InvalidRouteUri(value)),
        }
    }
}

/// An iterator over segments in a relative URI. The iterator will skip empty segments in the path.
#[derive(Clone, Debug)]
pub struct PathSegmentIterator<'a>(&'a str);

impl<'a> Iterator for PathSegmentIterator<'a> {
    type Item = &'a str;

    fn next(&mut self) -> Option<Self::Item> {
        let lower = self.0.find(|c| c != '/')?;
        let upper = self.0[lower..]
            .find('/')
            .map_or(self.0.len(), |next_slash| lower + next_slash);

        let segment = Some(&self.0[lower..upper]);

        self.0 = &self.0[upper..];

        segment
    }
}

impl PartialEq<str> for RouteUri {
    fn eq(&self, other: &str) -> bool {
        self.representation == *other
    }
}

impl PartialEq<&str> for RouteUri {
    fn eq(&self, other: &&str) -> bool {
        self.representation == *other
    }
}
