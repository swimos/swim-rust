// Copyright 2015-2021 Swim Inc.
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

use crate::uri::{BadRelativeUri, RelativeUri, UriIsAbsolute};
use http::Uri;
use std::convert::TryFrom;
use std::error::Error;
use std::str::FromStr;

#[test]
fn uri_is_absolute_display() {
    let err = UriIsAbsolute("swim://localhost/example".parse().unwrap());
    assert_eq!(
        format!("{}", err),
        "'swim://localhost/example' is an absolute URI."
    )
}

#[test]
fn relative_uri_new() {
    let good = RelativeUri::new("/example".parse().unwrap());
    assert!(matches!(good, Ok(RelativeUri(uri)) if uri == "/example"));

    let bad = RelativeUri::new("swim://example".parse().unwrap());
    assert!(bad.is_err());
}

#[test]
fn relative_uri_accessors() {
    let uri: Uri = "/example?more".parse().unwrap();
    let relative = RelativeUri::new(uri.clone()).unwrap();

    assert_eq!(relative.path(), "/example");
    assert_eq!(relative.query(), Some("more"));
    assert_eq!(relative.as_uri(), &uri);
}

#[test]
fn relative_uri_display() {
    let uri: RelativeUri = "/example".parse().unwrap();
    assert_eq!(format!("{}", uri), "/example");
}

#[test]
fn parse_relative_uri() {
    let good = RelativeUri::from_str("/example");
    let bad1 = RelativeUri::from_str("swim://localhost/example");
    let bad2 = RelativeUri::from_str("^ndgkjka(");

    assert!(matches!(good, Ok(url) if url == "/example"));
    assert!(matches!(bad1, Err(BadRelativeUri::Absolute(_))));
    assert!(matches!(bad2, Err(BadRelativeUri::Invalid(_))));
}

#[test]
fn display_bad_relative_uri() {
    let invalid = Uri::try_from("^ndgkjka(").err().unwrap();
    let expected = format!("{}", invalid);
    let err = BadRelativeUri::Invalid(invalid);
    assert_eq!(format!("{}", err), expected);

    let abs: Uri = "swim://localhost/example".parse().unwrap();

    let err = BadRelativeUri::Absolute(UriIsAbsolute(abs.clone()));
    assert_eq!(format!("{}", err), format!("{}", UriIsAbsolute(abs)));
}

#[test]
fn source_bad_relative_uri() {
    let invalid = Uri::try_from("^ndgkjka(").err().unwrap();
    let expected = format!("{}", invalid);
    let err = BadRelativeUri::Invalid(invalid);
    assert_eq!(err.source().unwrap().to_string(), expected);

    let abs: Uri = "swim://localhost/example".parse().unwrap();

    let err = BadRelativeUri::Absolute(UriIsAbsolute(abs.clone()));
    assert_eq!(
        err.source().unwrap().to_string(),
        UriIsAbsolute(abs).to_string()
    );
}

#[test]
fn try_from_str_relative_uri() {
    let good = RelativeUri::try_from("/example");
    let bad1 = RelativeUri::try_from("swim://localhost/example");
    let bad2 = RelativeUri::try_from("^ndgkjka(");

    assert!(matches!(good, Ok(url) if url == "/example"));
    assert!(matches!(bad1, Err(BadRelativeUri::Absolute(_))));
    assert!(matches!(bad2, Err(BadRelativeUri::Invalid(_))));
}

#[test]
fn try_from_string_relative_uri() {
    let good = RelativeUri::try_from("/example".to_string());
    let bad1 = RelativeUri::try_from("swim://localhost/example".to_string());
    let bad2 = RelativeUri::try_from("^ndgkjka(".to_string());

    assert!(matches!(good, Ok(url) if url == "/example"));
    assert!(matches!(bad1, Err(BadRelativeUri::Absolute(_))));
    assert!(matches!(bad2, Err(BadRelativeUri::Invalid(_))));
}

#[test]
fn try_from_uri_relative_uri() {
    let relative: Uri = "/example".parse().unwrap();
    let absolute: Uri = "swim://localhost/example".parse().unwrap();

    let good = RelativeUri::try_from(relative);
    let bad = RelativeUri::try_from(absolute);
    assert!(matches!(good, Ok(url) if url == "/example"));
    assert!(bad.is_err());
}

#[test]
#[allow(clippy::eq_op)]
fn relative_uri_equality() {
    let string = "/example";
    let uri: Uri = string.parse().unwrap();

    let relative = RelativeUri::new(uri.clone()).unwrap();

    assert_eq!(relative, relative);
    assert_eq!(relative, *string);
    assert_eq!(relative, string);
    assert_eq!(relative, string.to_string());
    assert_eq!(relative, uri);
}

#[test]
fn segment_iter_encoded() {
    let uri = RelativeUri::from_str("/swim:meta:node/unit%2Ffoo/lane/bar").unwrap();
    let mut iter = uri.path_iter();

    assert_eq!(iter.next(), Some("swim:meta:node"));
    assert_eq!(iter.next(), Some("unit%2Ffoo"));
    assert_eq!(iter.next(), Some("lane"));
    assert_eq!(iter.next(), Some("bar"));
    assert_eq!(iter.next(), None);
}

#[test]
fn segment_iter() {
    let uri = RelativeUri::from_str("/swim/meta/node/lane/bar").unwrap();
    let mut iter = uri.path_iter();

    assert_eq!(iter.next(), Some("swim"));
    assert_eq!(iter.next(), Some("meta"));
    assert_eq!(iter.next(), Some("node"));
    assert_eq!(iter.next(), Some("lane"));
    assert_eq!(iter.next(), Some("bar"));
    assert_eq!(iter.next(), None);
}

#[test]
fn segment_iter_trailing() {
    let uri = RelativeUri::from_str("/swim/meta/").unwrap();
    let mut iter = uri.path_iter();

    assert_eq!(iter.next(), Some("swim"));
    assert_eq!(iter.next(), Some("meta"));
    assert_eq!(iter.next(), None);
}

#[test]
fn segment_iter_multiple_slashes() {
    let uri = RelativeUri::from_str("/////////a/b//c//d/////////////////e/////").unwrap();
    let mut iter = uri.path_iter();

    assert_eq!(iter.next(), Some("a"));
    assert_eq!(iter.next(), Some("b"));
    assert_eq!(iter.next(), Some("c"));
    assert_eq!(iter.next(), Some("d"));
    assert_eq!(iter.next(), Some("e"));
    assert_eq!(iter.next(), None);
}
