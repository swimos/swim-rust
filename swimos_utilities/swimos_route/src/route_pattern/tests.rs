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

use crate::route_pattern::{ApplyError, ParseError, RoutePattern, Segment, UnapplyError};
use crate::route_uri::RouteUri;
use std::collections::HashMap;

#[test]
fn parse_error_display() {
    let err = ParseError(4);
    let string = err.to_string();
    assert_eq!(string, "Parsing route pattern failed at offset 4.")
}

#[test]
fn unapply_error_display() {
    let err = UnapplyError::new("/path/:id", "/path");
    let string = err.to_string();
    assert_eq!(string, "'/path' does not match pattern: '/path/:id'");
}

#[test]
fn apply_error_display() {
    let err = ApplyError::new("/path/:id", vec!["id".to_string()]);
    let string = err.to_string();
    assert_eq!(
        string,
        "Failed to populate '/path/:id', missing parameters: id."
    );

    let err = ApplyError::new("/path/:id/:sub", vec!["id".to_string(), "sub".to_string()]);
    let string = err.to_string();
    assert_eq!(
        string,
        "Failed to populate '/path/:id/:sub', missing parameters: id, sub."
    );
}

#[test]
fn route_pattern_display() {
    let pattern = RoutePattern::parse_str("/path/:id").unwrap();
    let string = pattern.to_string();
    assert_eq!(string, "/path/:id");
}

#[test]
fn pattern_segment_string() {
    let segment = Segment {
        start: 3,
        end: 6,
        parameter: false,
    };

    let pattern = "hello world";

    let seg_str = segment.segment_str(pattern);

    assert_eq!(seg_str, "lo ");
}

#[test]
fn bad_route_patterns() {
    assert!(RoutePattern::parse_str("").is_err());
    assert!(RoutePattern::parse_str("/").is_err());
    assert!(RoutePattern::parse_str("//").is_err());
    assert!(RoutePattern::parse_str("/first//second").is_err());
    assert!(RoutePattern::parse_str("first/:/second").is_err());
    assert!(RoutePattern::parse_str("/::").is_err());
}

#[test]
fn simple_static_route_pattern() {
    let pattern = "/path";
    let route_pattern = RoutePattern::parse_str(pattern);
    assert!(route_pattern.is_ok());

    let route_pattern = route_pattern.unwrap();

    assert!(route_pattern.scheme_str().is_none());
    assert!(route_pattern.has_absolute_path());

    let mut params = route_pattern.parameters();
    assert!(params.next().is_none());

    if let Ok(params) = route_pattern.unapply_str("/path") {
        assert!(params.is_empty());
    } else {
        panic!("Unapply failed.");
    }

    if let Ok(params) = route_pattern.unapply_str("swimos:/path") {
        assert!(params.is_empty());
    } else {
        panic!("Unapply failed.");
    }

    assert!(route_pattern.unapply_str("/other").is_err());
    assert!(route_pattern.unapply_str("/path2").is_err());
    assert!(route_pattern.unapply_str("/path/additional").is_err());

    let empty = HashMap::new();
    let mut with_param = HashMap::new();
    with_param.insert("id".to_string(), "hello".to_string());

    assert_eq!(route_pattern.apply(&empty), Ok("/path".to_string()));
    assert_eq!(route_pattern.apply(&with_param), Ok("/path".to_string()));
}

#[test]
fn simple_parameter_route_pattern() {
    let pattern = "/:id";
    let route_pattern = RoutePattern::parse_str(pattern);
    assert!(route_pattern.is_ok());

    let route_pattern = route_pattern.unwrap();

    assert!(route_pattern.scheme_str().is_none());
    assert!(route_pattern.has_absolute_path());

    let params = route_pattern.parameters().collect::<Vec<_>>();
    assert_eq!(params, vec!["id"]);

    if let Ok(params) = route_pattern.unapply_str("/path") {
        assert_eq!(params.len(), 1);
        assert_eq!(params.get("id"), Some(&"path".to_string()));
    } else {
        panic!("Unapply failed.");
    }

    if let Ok(params) = route_pattern.unapply_str("/other") {
        assert_eq!(params.len(), 1);
        assert_eq!(params.get("id"), Some(&"other".to_string()));
    } else {
        panic!("Unapply failed.");
    }

    if let Ok(params) = route_pattern.unapply_str("/aaa%2Fbbb") {
        assert_eq!(params.len(), 1);
        assert_eq!(params.get("id"), Some(&"aaa/bbb".to_string()));
    } else {
        panic!("Unapply failed.");
    }

    if let Ok(params) = route_pattern.unapply_str("swimos:/path") {
        assert_eq!(params.len(), 1);
        assert_eq!(params.get("id"), Some(&"path".to_string()));
    } else {
        panic!("Unapply failed.");
    }

    assert!(route_pattern.unapply_str("/path/additional").is_err());

    let empty = HashMap::new();
    let mut with_param = HashMap::new();
    with_param.insert("id".to_string(), "hello".to_string());

    assert_eq!(
        route_pattern.apply(&empty),
        Err(ApplyError::new("/:id", vec!["id".to_string()]))
    );
    assert_eq!(route_pattern.apply(&with_param), Ok("/hello".to_string()));
}

#[test]
fn mixed_route_pattern_second() {
    let pattern = "/path/:id";
    let route_pattern = RoutePattern::parse_str(pattern);
    assert!(route_pattern.is_ok());

    let route_pattern = route_pattern.unwrap();

    assert!(route_pattern.scheme_str().is_none());
    assert!(route_pattern.has_absolute_path());

    let params = route_pattern.parameters().collect::<Vec<_>>();
    assert_eq!(params, vec!["id"]);

    if let Ok(params) = route_pattern.unapply_str("/path/hello") {
        assert_eq!(params.len(), 1);
        assert_eq!(params.get("id"), Some(&"hello".to_string()));
    } else {
        panic!("Unapply failed.");
    }

    if let Ok(params) = route_pattern.unapply_str("/path/other") {
        assert_eq!(params.len(), 1);
        assert_eq!(params.get("id"), Some(&"other".to_string()));
    } else {
        panic!("Unapply failed.");
    }

    if let Ok(params) = route_pattern.unapply_str("swimos:/path/hello") {
        assert_eq!(params.len(), 1);
        assert_eq!(params.get("id"), Some(&"hello".to_string()));
    } else {
        panic!("Unapply failed.");
    }

    assert!(route_pattern.unapply_str("/path/hello/additional").is_err());
    assert!(route_pattern.unapply_str("/path/").is_err());
    assert!(route_pattern.unapply_str("/path").is_err());
    assert!(route_pattern.unapply_str("other").is_err());

    let empty = HashMap::new();
    let mut with_param = HashMap::new();
    with_param.insert("id".to_string(), "hello".to_string());

    assert_eq!(
        route_pattern.apply(&empty),
        Err(ApplyError::new("/path/:id", vec!["id".to_string()]))
    );
    assert_eq!(
        route_pattern.apply(&with_param),
        Ok("/path/hello".to_string())
    );
}

#[test]
fn mixed_route_pattern_first() {
    let pattern = "/:id/path";
    let route_pattern = RoutePattern::parse_str(pattern);
    assert!(route_pattern.is_ok());

    let route_pattern = route_pattern.unwrap();

    assert!(route_pattern.scheme_str().is_none());
    assert!(route_pattern.has_absolute_path());

    let params = route_pattern.parameters().collect::<Vec<_>>();
    assert_eq!(params, vec!["id"]);

    if let Ok(params) = route_pattern.unapply_str("/hello/path") {
        assert_eq!(params.len(), 1);
        assert_eq!(params.get("id"), Some(&"hello".to_string()));
    } else {
        panic!("Unapply failed.");
    }

    if let Ok(params) = route_pattern.unapply_str("/other/path") {
        assert_eq!(params.len(), 1);
        assert_eq!(params.get("id"), Some(&"other".to_string()));
    } else {
        panic!("Unapply failed.");
    }

    if let Ok(params) = route_pattern.unapply_str("swimos:/hello/path") {
        assert_eq!(params.len(), 1);
        assert_eq!(params.get("id"), Some(&"hello".to_string()));
    } else {
        panic!("Unapply failed.");
    }

    assert!(route_pattern.unapply_str("/path/hello/additional").is_err());
    assert!(route_pattern.unapply_str("/path/").is_err());
    assert!(route_pattern.unapply_str("other").is_err());

    let empty = HashMap::new();
    let mut with_param = HashMap::new();
    with_param.insert("id".to_string(), "hello".to_string());

    assert_eq!(
        route_pattern.apply(&empty),
        Err(ApplyError::new("/:id/path", vec!["id".to_string()]))
    );
    assert_eq!(
        route_pattern.apply(&with_param),
        Ok("/hello/path".to_string())
    );
}

#[test]
fn route_pattern_two_params() {
    let pattern = "/:id/:sub";
    let route_pattern = RoutePattern::parse_str(pattern);
    assert!(route_pattern.is_ok());

    let route_pattern = route_pattern.unwrap();

    assert!(route_pattern.scheme_str().is_none());
    assert!(route_pattern.has_absolute_path());

    let params = route_pattern.parameters().collect::<Vec<_>>();
    assert_eq!(params, vec!["id", "sub"]);

    if let Ok(params) = route_pattern.unapply_str("/path/hello") {
        assert_eq!(params.len(), 2);
        assert_eq!(params.get("id"), Some(&"path".to_string()));
        assert_eq!(params.get("sub"), Some(&"hello".to_string()));
    } else {
        panic!("Unapply failed.");
    }

    if let Ok(params) = route_pattern.unapply_str("/hello/path") {
        assert_eq!(params.len(), 2);
        assert_eq!(params.get("id"), Some(&"hello".to_string()));
        assert_eq!(params.get("sub"), Some(&"path".to_string()));
    } else {
        panic!("Unapply failed.");
    }

    if let Ok(params) = route_pattern.unapply_str("swimos:/path/hello") {
        assert_eq!(params.len(), 2);
        assert_eq!(params.get("id"), Some(&"path".to_string()));
        assert_eq!(params.get("sub"), Some(&"hello".to_string()));
    } else {
        panic!("Unapply failed.");
    }

    assert!(route_pattern.unapply_str("/path/hello/additional").is_err());
    assert!(route_pattern.unapply_str("/path/").is_err());
    assert!(route_pattern.unapply_str("/path").is_err());
    assert!(route_pattern.unapply_str("other").is_err());

    let empty = HashMap::new();
    let mut with_first = HashMap::new();
    with_first.insert("id".to_string(), "hello".to_string());
    let mut with_second = HashMap::new();
    with_second.insert("sub".to_string(), "world".to_string());
    let mut with_both = HashMap::new();
    with_both.insert("id".to_string(), "hello".to_string());
    with_both.insert("sub".to_string(), "world".to_string());

    assert_eq!(
        route_pattern.apply(&empty),
        Err(ApplyError::new(
            "/:id/:sub",
            vec!["id".to_string(), "sub".to_string()]
        ))
    );
    assert_eq!(
        route_pattern.apply(&with_first),
        Err(ApplyError::new("/:id/:sub", vec!["sub".to_string()]))
    );
    assert_eq!(
        route_pattern.apply(&with_second),
        Err(ApplyError::new("/:id/:sub", vec!["id".to_string()]))
    );
    assert_eq!(
        route_pattern.apply(&with_both),
        Ok("/hello/world".to_string())
    );
}

#[test]
fn route_pattern_two_params_split() {
    let pattern = "/:id/path/:sub";
    let route_pattern = RoutePattern::parse_str(pattern);
    assert!(route_pattern.is_ok());

    let route_pattern = route_pattern.unwrap();

    assert!(route_pattern.scheme_str().is_none());
    assert!(route_pattern.has_absolute_path());

    let params = route_pattern.parameters().collect::<Vec<_>>();
    assert_eq!(params, vec!["id", "sub"]);

    if let Ok(params) = route_pattern.unapply_str("/hello/path/world") {
        assert_eq!(params.len(), 2);
        assert_eq!(params.get("id"), Some(&"hello".to_string()));
        assert_eq!(params.get("sub"), Some(&"world".to_string()));
    } else {
        panic!("Unapply failed.");
    }

    if let Ok(params) = route_pattern.unapply_str("swimos:/hello/path/world") {
        assert_eq!(params.len(), 2);
        assert_eq!(params.get("id"), Some(&"hello".to_string()));
        assert_eq!(params.get("sub"), Some(&"world".to_string()));
    } else {
        panic!("Unapply failed.");
    }

    assert!(route_pattern
        .unapply_str("/hello/path/world/additional")
        .is_err());
    assert!(route_pattern.unapply_str("/path/hello/world").is_err());
    assert!(route_pattern.unapply_str("/hello/world").is_err());
    assert!(route_pattern.unapply_str("/path").is_err());
    assert!(route_pattern.unapply_str("hello/path/world").is_err());

    let empty = HashMap::new();
    let mut with_first = HashMap::new();
    with_first.insert("id".to_string(), "hello".to_string());
    let mut with_second = HashMap::new();
    with_second.insert("sub".to_string(), "world".to_string());
    let mut with_both = HashMap::new();
    with_both.insert("id".to_string(), "hello".to_string());
    with_both.insert("sub".to_string(), "world".to_string());

    assert_eq!(
        route_pattern.apply(&empty),
        Err(ApplyError::new(
            "/:id/path/:sub",
            vec!["id".to_string(), "sub".to_string()]
        ))
    );
    assert_eq!(
        route_pattern.apply(&with_first),
        Err(ApplyError::new("/:id/path/:sub", vec!["sub".to_string()]))
    );
    assert_eq!(
        route_pattern.apply(&with_second),
        Err(ApplyError::new("/:id/path/:sub", vec!["id".to_string()]))
    );
    assert_eq!(
        route_pattern.apply(&with_both),
        Ok("/hello/path/world".to_string())
    );
}

#[test]
fn route_pattern_ambiguity() {
    let pat1 = RoutePattern::parse_str("/path/:id").unwrap();
    let pat2 = RoutePattern::parse_str("/other/:id").unwrap();

    assert!(!RoutePattern::are_ambiguous(&pat1, &pat2));

    let pat3 = RoutePattern::parse_str("/path/other").unwrap();
    assert!(RoutePattern::are_ambiguous(&pat1, &pat3));

    let pat4 = RoutePattern::parse_str("/path/:foo").unwrap();
    assert!(RoutePattern::are_ambiguous(&pat1, &pat4));

    let pat5 = RoutePattern::parse_str("/other/:foo").unwrap();
    assert!(!RoutePattern::are_ambiguous(&pat1, &pat5));

    let pat6 = RoutePattern::parse_str("/path/:id/sub").unwrap();
    assert!(!RoutePattern::are_ambiguous(&pat1, &pat6));
}

#[test]
fn unapply_route_uri() {
    let pattern = RoutePattern::parse_str("/path/:id").unwrap();
    let uri: RouteUri = "/path/hello%20world%21".parse().unwrap();
    let result = pattern.unapply_route_uri(&uri);
    assert!(result.is_ok());
    let params = result.unwrap();
    assert_eq!(params.len(), 1);
    assert_eq!(params.get("id"), Some(&"hello world!".to_string()));
}

#[test]
fn pattern_with_scheme() {
    let pattern = "swimos:/path/:id";
    let route_pattern = RoutePattern::parse_str(pattern);
    assert!(route_pattern.is_ok());

    let route_pattern = route_pattern.unwrap();

    assert_eq!(route_pattern.scheme_str(), Some("swimos"));
    assert!(route_pattern.has_absolute_path());

    let params = route_pattern.parameters().collect::<Vec<_>>();
    assert_eq!(params, vec!["id"]);

    if let Ok(params) = route_pattern.unapply_str("swimos:/path/hello") {
        assert_eq!(params.len(), 1);
        assert_eq!(params.get("id"), Some(&"hello".to_string()));
    } else {
        panic!("Unapply failed.");
    }

    if let Ok(params) = route_pattern.unapply_str("swimos:/path/other") {
        assert_eq!(params.len(), 1);
        assert_eq!(params.get("id"), Some(&"other".to_string()));
    } else {
        panic!("Unapply failed.");
    }

    assert!(route_pattern.unapply_str("http:/path/hello").is_err());
    assert!(route_pattern.unapply_str("/path/hello/additional").is_err());
    assert!(route_pattern.unapply_str("/path/").is_err());
    assert!(route_pattern.unapply_str("/path").is_err());
    assert!(route_pattern.unapply_str("other").is_err());

    let empty = HashMap::new();
    let mut with_param = HashMap::new();
    with_param.insert("id".to_string(), "hello".to_string());

    assert_eq!(
        route_pattern.apply(&empty),
        Err(ApplyError::new("swimos:/path/:id", vec!["id".to_string()]))
    );
    assert_eq!(
        route_pattern.apply(&with_param),
        Ok("swimos:/path/hello".to_string())
    );
}

#[test]
fn relative_pattern_with_scheme() {
    let pattern = "swimos:meta:node/:node/pulse";
    let route_pattern = RoutePattern::parse_str(pattern);
    assert!(route_pattern.is_ok());

    let route_pattern = route_pattern.unwrap();

    assert_eq!(route_pattern.scheme_str(), Some("swimos"));
    assert!(!route_pattern.has_absolute_path());

    let params = route_pattern.parameters().collect::<Vec<_>>();
    assert_eq!(params, vec!["node"]);

    if let Ok(params) = route_pattern.unapply_str("swimos:meta:node/unit%2Ffoo/pulse") {
        assert_eq!(params.len(), 1);
        assert_eq!(params.get("node"), Some(&"unit/foo".to_string()));
    } else {
        panic!("Unapply failed.");
    }

    assert!(route_pattern
        .unapply_str("http:meta:node/unit%2Ffoo/pulse")
        .is_err());
    assert!(route_pattern
        .unapply_str("swimos:/meta:node/unit%2Ffoo/pulse")
        .is_err());
    assert!(route_pattern.unapply_str("/unit%2Ffoo/pulse").is_err());
    assert!(route_pattern.unapply_str("unit%2Ffoo/pulse").is_err());

    let empty = HashMap::new();
    let mut with_param = HashMap::new();
    with_param.insert("node".to_string(), "unit/foo".to_string());

    assert_eq!(
        route_pattern.apply(&empty),
        Err(ApplyError::new(
            "swimos:meta:node/:node/pulse",
            vec!["node".to_string()]
        ))
    );
    assert_eq!(
        route_pattern.apply(&with_param),
        Ok("swimos:meta:node/unit%2Ffoo/pulse".to_string())
    );
}

#[test]
fn simple_static_route_pattern_with_escape() {
    let pattern = "/path/abc%2Ddef";
    let route_pattern = RoutePattern::parse_str(pattern);

    let route_pattern = route_pattern.expect("Parse failed.");

    assert!(route_pattern.scheme_str().is_none());
    assert!(route_pattern.has_absolute_path());

    let mut params = route_pattern.parameters();
    assert!(params.next().is_none());

    let route_uri: RouteUri = "/path/abc%2Ddef".parse().expect("Bad route.");
    let params = route_pattern
        .unapply_route_uri(&route_uri)
        .expect("Unapply failed.");
    assert!(params.is_empty());

    let params = route_pattern
        .unapply_str("swimos:/path/abc%2Ddef")
        .expect("Unapply failed.");
    assert!(params.is_empty());

    let route_uri: RouteUri = "/path/abc-def".parse().expect("Bad route.");
    let params = route_pattern
        .unapply_route_uri(&route_uri)
        .expect("Unapply failed.");
    assert!(params.is_empty());

    assert!(route_pattern.unapply_str("/path").is_err());
    assert!(route_pattern.unapply_str("/path/additional").is_err());

    let empty = HashMap::new();
    let mut with_param = HashMap::new();
    with_param.insert("id".to_string(), "hello".to_string());

    assert_eq!(
        route_pattern.apply(&empty),
        Ok("/path/abc%2Ddef".to_string())
    );
    assert_eq!(
        route_pattern.apply(&with_param),
        Ok("/path/abc%2Ddef".to_string())
    );
}
