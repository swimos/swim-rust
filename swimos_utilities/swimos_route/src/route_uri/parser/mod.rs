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

use nom::branch::alt;
use nom::character::complete::{char, satisfy};
use nom::combinator::{map, opt, recognize};
use nom::multi::{many0_count, many1_count};
use nom::sequence::{pair, preceded, terminated, tuple};
use nom::{AsChar, IResult};
use nom_locate::LocatedSpan;

pub type Span<'a> = LocatedSpan<&'a str>;

fn schema_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '+' || c == '-' || c == '.'
}

fn scheme(input: Span<'_>) -> IResult<Span<'_>, Span<'_>> {
    recognize(tuple((
        satisfy(|c| c.is_ascii_alphabetic()),
        many0_count(satisfy(schema_char)),
    )))(input)
}

fn is_path_char(c: char) -> bool {
    c.is_ascii_alphanumeric()
        || c == '$'
        || c == '-'
        || c == '_'
        || c == '.'
        || c == '+'
        || c == '!'
        || c == '*'
        || c == '\''
        || c == '('
        || c == ')'
        || c == ','
        || c == ':'
        || c == '@'
        || c == '&'
        || c == '='
        || c == ';'
}

fn is_query_or_fragment_char(c: char) -> bool {
    is_path_char(c) || c == '/' || c == '?'
}

fn hex(input: Span<'_>) -> IResult<Span<'_>, char> {
    satisfy(|c| c.is_hex_digit())(input)
}

fn escape(input: Span<'_>) -> IResult<Span<'_>, Span<'_>> {
    recognize(pair(char('%'), pair(hex, hex)))(input)
}

fn path_char(input: Span<'_>) -> IResult<Span<'_>, Span<'_>> {
    alt((recognize(satisfy(is_path_char)), escape))(input)
}

fn query_or_fragment_char(input: Span<'_>) -> IResult<Span<'_>, Span<'_>> {
    alt((recognize(satisfy(is_query_or_fragment_char)), escape))(input)
}

fn path_segment(input: Span<'_>) -> IResult<Span<'_>, usize> {
    many0_count(path_char)(input)
}

fn query(input: Span<'_>) -> IResult<Span<'_>, Option<Span<'_>>> {
    opt(preceded(
        char('?'),
        recognize(many0_count(query_or_fragment_char)),
    ))(input)
}

fn fragment(input: Span<'_>) -> IResult<Span<'_>, Option<Span<'_>>> {
    opt(preceded(
        char('#'),
        recognize(many0_count(query_or_fragment_char)),
    ))(input)
}

fn first_path_segment(input: Span<'_>) -> IResult<Span<'_>, usize> {
    many1_count(path_char)(input)
}

fn path_segments(input: Span<'_>) -> IResult<Span<'_>, Span<'_>> {
    recognize(pair(
        first_path_segment,
        many0_count(preceded(char('/'), path_segment)),
    ))(input)
}

fn path(input: Span<'_>) -> IResult<Span<'_>, Span<'_>> {
    alt((recognize(pair(char('/'), path_segments)), path_segments))(input)
}

pub struct RouteUriParts<'a> {
    pub scheme: Option<Span<'a>>,
    pub path: Span<'a>,
    pub query: Option<Span<'a>>,
    pub fragment: Option<Span<'a>>,
}

impl<'a> RouteUriParts<'a> {
    pub fn scheme_len(&self) -> Option<usize> {
        self.scheme.map(|span| span.len())
    }

    pub fn path(&self) -> (usize, usize) {
        (self.path.location_offset(), self.path.len())
    }

    pub fn query(&self) -> Option<(usize, usize)> {
        self.query.map(|span| (span.location_offset(), span.len()))
    }

    pub fn fragment_offset(&self) -> Option<usize> {
        self.fragment.map(|span| span.location_offset())
    }
}

pub fn route_uri(input: Span<'_>) -> IResult<Span<'_>, RouteUriParts<'_>> {
    map(
        tuple((opt(terminated(scheme, char(':'))), path, query, fragment)),
        |(scheme, path, query, fragment)| RouteUriParts {
            scheme,
            path,
            query,
            fragment,
        },
    )(input)
}
