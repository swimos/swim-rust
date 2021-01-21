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

use crate::uri::UriPathSegments;
use http::uri::{InvalidUri, InvalidUriParts, PathAndQuery};
use percent_encoding::{percent_encode, NON_ALPHANUMERIC};
use std::convert::TryFrom;
use std::str::Split;

#[derive(Debug, Clone)]
pub struct SwimUri {
    inner: http::Uri,
}

impl TryFrom<&str> for SwimUri {
    type Error = SwimUriParseErr;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Ok(SwimUri {
            inner: http::Uri::try_from(value)?,
        })
    }
}

impl From<InvalidUri> for SwimUriParseErr {
    fn from(e: InvalidUri) -> Self {
        SwimUriParseErr(e.to_string())
    }
}

impl From<InvalidUriParts> for SwimUriParseErr {
    fn from(e: InvalidUriParts) -> Self {
        SwimUriParseErr(e.to_string())
    }
}

#[derive(Debug, Clone)]
pub struct SwimUriParseErr(String);

impl PartialEq<&str> for SwimUri {
    fn eq(&self, other: &&str) -> bool {
        self.inner.to_string().eq(other)
    }
}

impl SwimUri {
    pub fn appended_path(self, append_path: &str) -> Result<SwimUri, SwimUriParseErr> {
        let SwimUri { inner } = self;
        let append_path = percent_encode(append_path.as_bytes(), NON_ALPHANUMERIC).to_string();
        let mut parts = inner.into_parts();

        let new_path_and_query = match &parts.path_and_query {
            Some(path_and_query) => {
                let build = |path, query| match query {
                    Some(query) => PathAndQuery::try_from(&[path, "?", query].join("")),
                    None => PathAndQuery::try_from(path),
                };

                let path = path_and_query.path();
                let query = path_and_query.query();
                if path.ends_with("/") {
                    build(&[path, &append_path].join(""), query)?
                } else {
                    build(&[path, "/", &append_path].join(""), query)?
                }
            }
            None => PathAndQuery::try_from(append_path)?,
        };

        parts.path_and_query = Some(new_path_and_query);

        Ok(SwimUri {
            inner: http::Uri::from_parts(parts)?,
        })
    }

    pub fn into_inner(self) -> http::Uri {
        self.inner
    }
}

impl UriPathSegments for SwimUri {
    fn path_segments(&self) -> Option<Split<char>> {
        let path = self.inner.path();
        if path.starts_with('/') {
            Some(path[1..].split('/'))
        } else {
            None
        }
    }
}

// macro_rules! uri {
//     () => {};
// }

#[test]
fn test_build() {
    let parts = vec!["swim://localhost:9001/swim:meta:node", "unit/foo", "1"];
    let uri = match parts.first() {
        Some(host) => SwimUri::try_from(*host).unwrap(),
        None => {
            panic!()
        }
    };
    let result = parts
        .into_iter()
        .skip(1)
        .try_fold(uri, |uri, part| uri.appended_path(part));

    println!("{:?}", result);
}

#[test]
fn test_parse_uri() {
    let uri = SwimUri::try_from("swim://localhost:9001/swim:meta:node")
        .unwrap()
        .appended_path("unit/foo")
        .unwrap()
        .appended_path("1")
        .unwrap();

    assert_eq!(uri, "swim://localhost:9001/swim:meta:node/unit%2Ffoo/1");

    let uri = SwimUri::try_from("swim://localhost:9001/swim:meta:node/")
        .unwrap()
        .appended_path("unit/foo")
        .unwrap()
        .appended_path("1")
        .unwrap();

    assert_eq!(uri, "swim://localhost:9001/swim:meta:node/unit%2Ffoo/1");

    let uri = SwimUri::try_from("swim://localhost:9001/node/lane?name=some_lane&encoding=recon")
        .unwrap()
        .appended_path("prefix")
        .unwrap();

    assert_eq!(
        uri,
        "swim://localhost:9001/node/lane/prefix?name=some_lane&encoding=recon"
    );
}

#[test]
fn test_iter() {
    let uri = SwimUri::try_from("ws://localhost:9001/swim:meta:node/unit/foo/lanes").unwrap();
    let mut parts = uri.path_segments().unwrap();

    assert_eq!(parts.next(), Some("swim:meta:node"));
    assert_eq!(parts.next(), Some("unit"));
    assert_eq!(parts.next(), Some("foo"));
    assert_eq!(parts.next(), Some("lanes"));
    assert_eq!(parts.next(), None);

    let uri = SwimUri::try_from("ws://localhost:9001/swim:meta:node/")
        .unwrap()
        .appended_path("unit/foo")
        .unwrap()
        .appended_path("lanes")
        .unwrap();

    let mut parts = uri.path_segments().unwrap();

    assert_eq!(parts.next(), Some("swim:meta:node"));
    assert_eq!(parts.next(), Some("unit%2Ffoo"));
    assert_eq!(parts.next(), Some("lanes"));
    assert_eq!(parts.next(), None);
}
