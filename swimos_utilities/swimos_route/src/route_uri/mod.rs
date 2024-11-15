// Copyright 2015-2024 Swim Inc.
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

mod parser;

#[cfg(test)]
mod tests;

/// A URI defining a Swim route. This is a URI that lacks an authority and may or may not be relative.
#[derive(Debug, PartialEq, Eq, Hash, Clone, PartialOrd, Ord)]
pub struct RouteUri {
    representation: String,
    scheme_len: Option<usize>,
    path: (usize, usize),
    query: Option<(usize, usize)>,
    fragment: Option<usize>,
}

impl Default for RouteUri {
    fn default() -> Self {
        Self {
            representation: ".".to_string(),
            scheme_len: None,
            path: (0, 1),
            query: None,
            fragment: None,
        }
    }
}

impl Display for RouteUri {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.representation)
    }
}

impl RouteUri {
    fn new(
        original: String,
        scheme_len: Option<usize>,
        path: (usize, usize),
        query: Option<(usize, usize)>,
        fragment: Option<usize>,
    ) -> Self {
        RouteUri {
            representation: original,
            scheme_len,
            path,
            query,
            fragment,
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
            path,
            ..
        } = self;
        let (offset, len) = *path;
        &representation[offset..(offset + len)]
    }

    /// The query of the URI.
    pub fn query(&self) -> Option<&str> {
        let RouteUri {
            representation,
            query,
            ..
        } = self;
        (*query).map(|(offset, len)| &representation[offset..(offset + len)])
    }

    /// The fragment of the URI.
    pub fn fragment(&self) -> Option<&str> {
        let RouteUri {
            representation,
            fragment,
            ..
        } = self;
        (*fragment).map(|offset| &representation[offset..])
    }

    pub fn as_str(&self) -> &str {
        self.representation.as_str()
    }

    /// Returns an iterator that will yield each segment in the path.
    pub fn path_iter(&self) -> impl Iterator<Item = &str> {
        PathSegmentIterator(self.path())
    }
}

/// Error type that is produced by an attempt to parse an invalid [`RouteUri`] from a string.
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

impl FromStr for RouteUri {
    type Err = InvalidRouteUri;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let span = parser::Span::new(s);
        parser::route_uri(span)
            .finish()
            .map(|(_, parts)| {
                RouteUri::new(
                    s.to_owned(),
                    parts.scheme_len(),
                    parts.path(),
                    parts.query(),
                    parts.fragment_offset(),
                )
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
        let result = parser::route_uri(span).finish().map(|(_, parts)| parts);
        match result {
            Ok(parts) => {
                let scheme = parts.scheme_len();
                let path = parts.path();
                let query = parts.query();
                let fragment = parts.fragment_offset();
                Ok(RouteUri::new(value, scheme, path, query, fragment))
            }
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
