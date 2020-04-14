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

use super::*;

#[test]
fn non_empty_string() {
    let schema = TextSchema::NonEmpty;
    assert!(!schema.matches_str(""));
    assert!(schema.matches_str("a"));
}

#[test]
fn exact_string() {
    let schema = TextSchema::exact("Hello");
    assert!(!schema.matches_str("hello"));
    assert!(schema.matches_str("Hello"));
}

#[test]
fn regex_match() {
    let schema = TextSchema::regex("^ab*a$").unwrap();
    assert!(schema.matches_str("aa"));
    assert!(schema.matches_str("abba"));
    assert!(!schema.matches_str("aaba"));
    assert!(!schema.matches_str("abca"));
}
