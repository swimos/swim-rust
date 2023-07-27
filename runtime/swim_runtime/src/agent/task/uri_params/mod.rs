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

use std::borrow::Cow;

use http::Uri;
use nom::{
    branch::alt,
    bytes::complete::is_not,
    character::complete::{char, satisfy},
    combinator::recognize,
    multi::{many0_count, separated_list0},
    sequence::{pair, separated_pair},
    AsChar, Finish, IResult,
};
use percent_encoding::percent_decode_str;

fn hex(input: &str) -> IResult<&str, char> {
    satisfy(|c| c.is_hex_digit())(input)
}

fn escape(input: &str) -> IResult<&str, &str> {
    recognize(pair(char('%'), pair(hex, hex)))(input)
}

fn param_name_char(input: &str) -> IResult<&str, &str> {
    alt((recognize(satisfy(|c| c.is_ascii_alphanumeric())), escape))(input)
}

fn param_name(input: &str) -> IResult<&str, &str> {
    recognize(many0_count(param_name_char))(input)
}

fn param_value_char(input: &str) -> IResult<&str, &str> {
    is_not("=&")(input)
}

fn param_value(input: &str) -> IResult<&str, &str> {
    recognize(many0_count(param_value_char))(input)
}

fn param(input: &str) -> IResult<&str, (&str, &str)> {
    separated_pair(param_name, char('='), param_value)(input)
}

fn params(input: &str) -> IResult<&str, Vec<(&str, &str)>> {
    separated_list0(char('&'), param)(input)
}

const LANE: &str = "name";

pub fn extract_lane(uri: &Uri) -> Option<Cow<'_, str>> {
    uri.query().and_then(|query| match params(query).finish() {
        Ok((rem, uri_params)) if rem.is_empty() => uri_params
            .into_iter()
            .find(|(name, _)| name.eq_ignore_ascii_case(LANE))
            .map(|(_, value)| percent_decode_str(value).decode_utf8_lossy()),
        _ => None,
    })
}
