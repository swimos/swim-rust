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

use http::Uri;

const GOOD_URI: &str = "http://example:8080/path/to/node?lane=name";
const NOISY_URI: &str = "http://example:8080/path/to/node?a=67&lane=name&other=stuff";
const NO_QUERY: &str = "http://example:8080/path/to/node";
const LANE_ABSENT: &str = "http://example:8080/path/to/node?a=67&other=stuff";

#[test]
fn extract_good_name() {
    let uri = Uri::from_static(GOOD_URI);

    let result = super::extract_lane(&uri).map(|s| s.to_string());
    assert_eq!(result, Some("name".to_string()));
}

#[test]
fn extract_good_name_with_noise() {
    let uri = Uri::from_static(NOISY_URI);

    let result = super::extract_lane(&uri).map(|s| s.to_string());
    assert_eq!(result, Some("name".to_string()));
}

#[test]
fn extract_no_query() {
    let uri = Uri::from_static(NO_QUERY);

    let result = super::extract_lane(&uri).map(|s| s.to_string());
    assert!(result.is_none());
}

#[test]
fn extract_lane_absent() {
    let uri = Uri::from_static(LANE_ABSENT);

    let result = super::extract_lane(&uri).map(|s| s.to_string());
    assert!(result.is_none());
}
