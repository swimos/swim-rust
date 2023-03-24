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

use super::Tokenizer;

#[test]
fn empty_string() {
    let tok = Tokenizer::new("");
    let parts = tok.collect::<Vec<_>>();
    assert!(parts.is_empty());
}

#[test]
fn single_token() {
    let tok = Tokenizer::new("one");
    let parts = tok.map(ToOwned::to_owned).collect::<Vec<_>>();
    assert_eq!(parts, vec!["one"]);
}

#[test]
fn single_token_padded() {
    let tok = Tokenizer::new("  one ");
    let parts = tok.map(ToOwned::to_owned).collect::<Vec<_>>();
    assert_eq!(parts, vec!["one"]);
}

#[test]
fn two_tokens() {
    let tok = Tokenizer::new("one two");
    let parts = tok.map(ToOwned::to_owned).collect::<Vec<_>>();
    assert_eq!(parts, vec!["one", "two"]);
}

#[test]
fn two_token_padded() {
    let tok = Tokenizer::new("   one    two ");
    let parts = tok.map(ToOwned::to_owned).collect::<Vec<_>>();
    assert_eq!(parts, vec!["one", "two"]);
}

#[test]
fn three_tokens() {
    let tok = Tokenizer::new("one two three");
    let parts = tok.map(ToOwned::to_owned).collect::<Vec<_>>();
    assert_eq!(parts, vec!["one", "two", "three"]);
}

#[test]
fn quoted_token() {
    let tok = Tokenizer::new("`multiple words`");
    let parts = tok.map(ToOwned::to_owned).collect::<Vec<_>>();
    assert_eq!(parts, vec!["multiple words"]);
}

#[test]
fn quoted_token_padded() {
    let tok = Tokenizer::new("  `  multiple words ` ");
    let parts = tok.map(ToOwned::to_owned).collect::<Vec<_>>();
    assert_eq!(parts, vec!["  multiple words "]);
}

#[test]
fn mixed_tokens1() {
    let tok = Tokenizer::new("one `multiple words`");
    let parts = tok.map(ToOwned::to_owned).collect::<Vec<_>>();
    assert_eq!(parts, vec!["one", "multiple words"]);
}

#[test]
fn mixed_tokens2() {
    let tok = Tokenizer::new("`multiple words`one");
    let parts = tok.map(ToOwned::to_owned).collect::<Vec<_>>();
    assert_eq!(parts, vec!["multiple words", "one"]);
}

#[test]
fn mixed_tokens3() {
    let tok = Tokenizer::new("one `multiple words` two");
    let parts = tok.map(ToOwned::to_owned).collect::<Vec<_>>();
    assert_eq!(parts, vec!["one", "multiple words", "two"]);
}
