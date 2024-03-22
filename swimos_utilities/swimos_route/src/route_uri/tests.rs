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

use crate::route_uri::RouteUri;

use super::InvalidRouteUri;

#[test]
fn bad_route_uri_display() {
    let err = InvalidRouteUri("swimos://localhost/example".to_string());
    assert_eq!(
        format!("{}", err),
        "'swimos://localhost/example' is not a valid route URI."
    )
}

#[test]
fn route_uri_parse_good() {
    if let Ok(route_uri) = "/example".parse::<RouteUri>() {
        assert!(route_uri.scheme().is_none());
        assert_eq!(route_uri.path(), "/example");
        assert!(route_uri.query().is_none());
        assert!(route_uri.fragment().is_none());
    } else {
        panic!("Bad route URI.");
    }

    if let Ok(route_uri) = "swimos:/example".parse::<RouteUri>() {
        assert_eq!(route_uri.scheme(), Some("swimos"));
        assert_eq!(route_uri.path(), "/example");
        assert!(route_uri.query().is_none());
        assert!(route_uri.fragment().is_none());
    } else {
        panic!("Bad route URI.");
    }

    if let Ok(route_uri) = "swimos:meta:node/unit%2Ffoo/lane/bar".parse::<RouteUri>() {
        assert_eq!(route_uri.scheme(), Some("swimos"));
        assert_eq!(route_uri.path(), "meta:node/unit%2Ffoo/lane/bar");
        assert!(route_uri.query().is_none());
        assert!(route_uri.fragment().is_none());
    } else {
        panic!("Bad route URI.");
    }
}

#[test]
fn route_uri_parse_bad() {
    assert!("swimos://localhost/example".parse::<RouteUri>().is_err());
    assert!("^ndgkjka(".parse::<RouteUri>().is_err());
}

#[test]
fn relative_uri_display() {
    let uri: RouteUri = "/example".parse().unwrap();
    assert_eq!(format!("{}", uri), "/example");
}

#[test]
fn try_from_str_route_uri() {
    let good = RouteUri::try_from("/example");
    let bad1 = RouteUri::try_from("swimos://localhost/example");
    let bad2 = RouteUri::try_from("^ndgkjka(");

    assert!(matches!(good, Ok(uri) if uri == "/example"));
    assert!(bad1.is_err());
    assert!(bad2.is_err());
}

#[test]
fn try_from_string_route_uri() {
    let good = RouteUri::try_from("/example".to_owned());
    let bad1 = RouteUri::try_from("swimos://localhost/example".to_owned());
    let bad2 = RouteUri::try_from("^ndgkjka(".to_owned());

    assert!(matches!(good, Ok(uri) if uri == "/example"));
    assert!(bad1.is_err());
    assert!(bad2.is_err());
}

#[test]
fn segment_iter_encoded() {
    let uri = RouteUri::from_str("swimos:meta:node/unit%2Ffoo/lane/bar").unwrap();
    let mut iter = uri.path_iter();

    assert_eq!(iter.next(), Some("meta:node"));
    assert_eq!(iter.next(), Some("unit%2Ffoo"));
    assert_eq!(iter.next(), Some("lane"));
    assert_eq!(iter.next(), Some("bar"));
    assert_eq!(iter.next(), None);
}

#[test]
fn segment_iter() {
    let uri = RouteUri::from_str("/swimos/meta/node/lane/bar").unwrap();
    let mut iter = uri.path_iter();

    assert_eq!(iter.next(), Some("swimos"));
    assert_eq!(iter.next(), Some("meta"));
    assert_eq!(iter.next(), Some("node"));
    assert_eq!(iter.next(), Some("lane"));
    assert_eq!(iter.next(), Some("bar"));
    assert_eq!(iter.next(), None);
}

#[test]
fn segment_iter_trailing() {
    let uri = RouteUri::from_str("/swimos/meta/").unwrap();
    let mut iter = uri.path_iter();

    assert_eq!(iter.next(), Some("swimos"));
    assert_eq!(iter.next(), Some("meta"));
    assert_eq!(iter.next(), None);
}

#[test]
fn segment_iter_multiple_slashes() {
    let uri = RouteUri::from_str("/a/b//c//d/////////////////e/////").unwrap();
    let mut iter = uri.path_iter();

    assert_eq!(iter.next(), Some("a"));
    assert_eq!(iter.next(), Some("b"));
    assert_eq!(iter.next(), Some("c"));
    assert_eq!(iter.next(), Some("d"));
    assert_eq!(iter.next(), Some("e"));
    assert_eq!(iter.next(), None);
}
