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

use percent_encoding::{percent_decode_str, utf8_percent_encode, NON_ALPHANUMERIC};
use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::fmt::{Display, Formatter, Write};

use crate::route_uri::RouteUri;

#[cfg(test)]
mod tests;

///Pattern to match a '/'-separated route specifier. Pattern components can either be literal
///strings or named parameters, starting with ':'. For example "/path/:id".
#[derive(Clone, Debug)]
pub struct RoutePattern {
    pattern: String,
    scheme: Option<usize>,
    absolute: bool,
    segments: Vec<Segment>,
}

#[derive(Clone, Copy, Debug)]
struct Segment {
    start: usize,
    end: usize,
    parameter: bool,
}

impl Segment {
    fn segment_str<'a>(&self, pattern: &'a str) -> &'a str {
        &pattern[self.start..self.end]
    }
}

/// Error indicating that a provided string does not parse to a valid route specifier, providing
/// the offset of the first invalid character.
#[derive(Debug, PartialEq, Eq)]
pub struct ParseError(usize);

impl Display for ParseError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Parsing route pattern failed at offset {}.", self.0)
    }
}

impl Error for ParseError {}

/// Error indicating that a route did not match a route pattern pattern.
#[derive(Debug, PartialEq, Eq)]
pub struct UnapplyError {
    pattern: String,
    route: String,
}

impl UnapplyError {
    fn new(pattern: &str, route: &str) -> Self {
        UnapplyError {
            pattern: pattern.to_string(),
            route: route.to_string(),
        }
    }
}

impl Display for UnapplyError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "'{}' does not match pattern: '{}'",
            self.route, self.pattern
        )
    }
}

impl Error for UnapplyError {}

/// Error indicating that insufficient parameter values were provided to populate a pattern.
#[derive(Debug, PartialEq, Eq)]
pub struct ApplyError {
    pattern: String,
    missing: Vec<String>,
}

impl Display for ApplyError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Failed to populate '{}', missing parameters: ",
            self.pattern
        )?;
        let mut it = self.missing.iter();
        if let Some(param) = it.next() {
            write!(f, "{}", param)?;
        }
        for param in it {
            write!(f, ", {}", param)?;
        }
        write!(f, ".")
    }
}

impl Error for ApplyError {}

impl ApplyError {
    fn new(pattern: &str, missing: Vec<String>) -> Self {
        ApplyError {
            pattern: pattern.to_string(),
            missing,
        }
    }
}

impl RoutePattern {
    /// Attempt to parse a pattern from its string representation.
    pub fn parse<I>(pattern: I) -> Result<RoutePattern, ParseError>
    where
        I: IntoIterator<Item = char>,
    {
        let it = pattern.into_iter();
        let (min, max) = it.size_hint();
        let cap = max.unwrap_or(min);
        let mut pat = String::with_capacity(cap);
        let mut state = ParseState::Start;
        let mut scheme = None;
        let mut absolute = false;
        let mut segments = vec![];
        let mut offset: usize = 0;

        for c in it {
            pat.push(c);
            state.transition(c, offset, &mut scheme, &mut absolute, &mut segments);
            state.check()?;
            offset += c.len_utf8();
        }

        if let Some(segment) = state.end(offset)? {
            segments.push(segment);
        }

        let mut names = HashSet::new();

        for segment in &segments {
            if segment.parameter {
                let segment_str = segment.segment_str(pat.as_str());
                if names.contains(segment_str) {
                    return Err(ParseError(segment.start));
                } else {
                    names.insert(segment_str);
                }
            }
        }

        Ok(RoutePattern {
            pattern: pat,
            scheme,
            absolute,
            segments,
        })
    }

    /// Attempt to parse a route pattern from its string representation.
    pub fn parse_str(pattern: &str) -> Result<RoutePattern, ParseError> {
        RoutePattern::parse(pattern.chars())
    }

    /// Get the parameter names present in a pattern.
    pub fn parameters(&self) -> impl Iterator<Item = &str> + '_ {
        let RoutePattern {
            pattern, segments, ..
        } = self;
        segments
            .iter()
            .filter(|s| s.parameter)
            .map(move |segment| segment.segment_str(pattern.as_str()))
    }

    /// Match a route against the route pattern, extracting the values of each named parameter.
    fn unapply_parts<'a, I>(&self, mut parts: I) -> Option<HashMap<String, String>>
    where
        I: Iterator,
        I::Item: Iterator<Item = char> + 'a,
    {
        let RoutePattern {
            pattern, segments, ..
        } = self;
        let mut segments = segments.iter();
        let mut param_map = HashMap::new();
        loop {
            let part = parts.next();
            let segment = segments.next();
            if let Some(part) = part {
                if let Some(segment) = segment {
                    let segment_str = segment.segment_str(pattern.as_str());
                    if segment.parameter {
                        let collected: String = part.collect();
                        if collected.is_empty() {
                            return None;
                        } else {
                            param_map.insert(segment_str.to_string(), collected);
                        }
                    } else if !part.eq(segment_str.chars()) {
                        return None;
                    }
                } else {
                    return None;
                }
            } else if segment.is_some() {
                return None;
            } else {
                break;
            }
        }
        Some(param_map)
    }

    /// Match a string route against the route pattern, extracting the values of each named
    /// parameter.
    pub fn unapply_str(&self, route: &str) -> Result<HashMap<String, String>, UnapplyError> {
        if let Ok(route_uri) = route.parse::<RouteUri>() {
            self.unapply_route_uri(&route_uri)
        } else {
            Err(UnapplyError::new(self.pattern.as_str(), route))
        }
    }

    /// Match a [`RouteUri`] route against the route pattern, extracting the values of each named
    /// parameter.
    pub fn unapply_route_uri(
        &self,
        uri: &RouteUri,
    ) -> Result<HashMap<String, String>, UnapplyError> {
        let make_err = || {
            Err(UnapplyError::new(
                self.pattern.as_str(),
                uri.to_string().as_str(),
            ))
        };
        match (self.scheme_str(), uri.scheme()) {
            (Some(s1), Some(s2)) if s1 != s2 => {
                return make_err();
            }
            _ => {}
        }
        let mut segments = uri.path().split('/');
        if self.absolute && !matches!(segments.next(), Some("")) {
            return make_err();
        }
        if let Some(part_map) =
            self.unapply_parts(segments.map(|s| percent_decode_str(s).map(|b| b as char)))
        {
            Ok(part_map)
        } else {
            make_err()
        }
    }

    pub fn scheme_str(&self) -> Option<&str> {
        self.scheme
            .map(|scheme_offset| &self.pattern[0..scheme_offset])
    }

    pub fn has_absolute_path(&self) -> bool {
        self.absolute
    }

    /// Attempt to craete a route from a route pattern by providing the values for each parameter.
    pub fn apply(&self, params: &HashMap<String, String>) -> Result<String, ApplyError> {
        let RoutePattern {
            pattern,
            segments,
            absolute,
            ..
        } = self;
        let mut route = String::new();
        let mut missing = None;
        if let Some(scheme_str) = self.scheme_str() {
            route.push_str(scheme_str);
            route.push(':');
        }
        let mut first = true;
        for segment in segments {
            if !first || *absolute {
                route.push('/');
            }
            first = false;
            let segment_str = segment.segment_str(pattern.as_str());
            if segment.parameter {
                match params.get(segment_str) {
                    Some(param_value) if !param_value.is_empty() => {
                        let encoded = utf8_percent_encode(param_value, NON_ALPHANUMERIC);
                        write!(&mut route, "{}", encoded).expect("Formatting should not fail.");
                    }
                    _ => {
                        missing
                            .get_or_insert_with(Vec::new)
                            .push(segment_str.to_string());
                    }
                }
            } else {
                route.push_str(segment_str);
            }
        }
        if let Some(missing) = missing {
            Err(ApplyError::new(pattern.as_str(), missing))
        } else {
            Ok(route)
        }
    }

    /// Determine if two router patterns are ambiguous (some routes could match both).
    pub fn are_ambiguous(left: &Self, right: &Self) -> bool {
        if left.segments.len() != right.segments.len() {
            false
        } else {
            let RoutePattern {
                pattern: pat_left,
                segments: segs_left,
                ..
            } = left;
            let RoutePattern {
                pattern: pat_right,
                segments: segs_right,
                ..
            } = right;

            for (left, right) in segs_left.iter().zip(segs_right.iter()) {
                if !left.parameter
                    && !right.parameter
                    && left.segment_str(pat_left.as_str()) != right.segment_str(pat_right.as_str())
                {
                    return false;
                }
            }
            true
        }
    }
}

impl Display for RoutePattern {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.pattern.fmt(f)
    }
}

impl PartialEq for RoutePattern {
    fn eq(&self, other: &Self) -> bool {
        self.pattern.eq(&other.pattern)
    }
}

impl Eq for RoutePattern {}

enum ParseState {
    Start,
    SchemeOrLiteral(usize),
    SegmentStart,
    AfterScheme,
    Literal(usize),
    Parameter(usize),
    Failed(usize),
}

impl ParseState {
    fn transition(
        &mut self,
        c: char,
        offset: usize,
        scheme: &mut Option<usize>,
        absolute: &mut bool,
        segments: &mut Vec<Segment>,
    ) {
        let new_state = match (&*self, c) {
            (ParseState::Start, '/') => {
                *absolute = true;
                ParseState::SegmentStart
            }
            (ParseState::Start, ':') => {
                *absolute = false;
                ParseState::Parameter(offset)
            }
            (ParseState::Start, c) => {
                if c.is_ascii_alphabetic() {
                    ParseState::SchemeOrLiteral(offset)
                } else {
                    *absolute = false;
                    ParseState::Literal(offset)
                }
            }
            (ParseState::SegmentStart, ':') => ParseState::Parameter(offset),
            (ParseState::SegmentStart, '/') => ParseState::Failed(offset),
            (ParseState::SegmentStart, _) => ParseState::Literal(offset),
            (ParseState::SchemeOrLiteral(_), ':') => {
                *scheme = Some(offset);
                ParseState::AfterScheme
            }
            (ParseState::SchemeOrLiteral(start), '/') => {
                *absolute = false;
                let length = offset - *start;
                if length > 0 {
                    segments.push(Segment {
                        start: *start,
                        end: offset,
                        parameter: false,
                    });
                    ParseState::SegmentStart
                } else {
                    ParseState::Failed(offset)
                }
            }
            (ParseState::SchemeOrLiteral(o), _) => ParseState::SchemeOrLiteral(*o),
            (ParseState::AfterScheme, '/') => {
                *absolute = true;
                ParseState::SegmentStart
            }
            (ParseState::AfterScheme, ':') => {
                *absolute = false;
                ParseState::Parameter(offset)
            }
            (ParseState::AfterScheme, _) => {
                *absolute = false;
                ParseState::Literal(offset)
            }
            (ParseState::Literal(start), '/') => {
                let length = offset - *start;
                if length > 0 {
                    segments.push(Segment {
                        start: *start,
                        end: offset,
                        parameter: false,
                    });
                    ParseState::SegmentStart
                } else {
                    ParseState::Failed(offset)
                }
            }
            (ParseState::Literal(o), _) => ParseState::Literal(*o),
            (ParseState::Parameter(start), '/') => {
                let length = offset - *start - 1;
                if length > 0 {
                    segments.push(Segment {
                        start: *start + 1,
                        end: offset,
                        parameter: true,
                    });
                    ParseState::SegmentStart
                } else {
                    ParseState::Failed(offset)
                }
            }
            (ParseState::Parameter(_), ':') => ParseState::Failed(offset),
            (ParseState::Parameter(o), _) => ParseState::Parameter(*o),
            _ => ParseState::Failed(offset),
        };
        *self = new_state;
    }

    fn check(&self) -> Result<(), ParseError> {
        if let ParseState::Failed(offset) = self {
            Err(ParseError(*offset))
        } else {
            Ok(())
        }
    }

    fn end(self, offset: usize) -> Result<Option<Segment>, ParseError> {
        match self {
            ParseState::Start | ParseState::SegmentStart => Err(ParseError(offset)),
            ParseState::Literal(start) | ParseState::SchemeOrLiteral(start) => {
                let length = offset - start;
                if length > 0 {
                    Ok(Some(Segment {
                        start,
                        end: offset,
                        parameter: false,
                    }))
                } else {
                    Err(ParseError(offset))
                }
            }
            ParseState::Parameter(start) => {
                let length = offset - start - 1;
                if length > 0 {
                    Ok(Some(Segment {
                        start: start + 1,
                        end: offset,
                        parameter: true,
                    }))
                } else {
                    Err(ParseError(offset))
                }
            }
            _ => Ok(None),
        }
    }
}
