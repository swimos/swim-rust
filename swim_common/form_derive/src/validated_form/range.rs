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

use std::iter;
use std::str::FromStr;

#[derive(Default, Debug, PartialEq)]
pub struct Range<T> {
    lower: Option<T>,
    upper: Option<T>,
    inclusive: bool,
}

#[derive(Debug)]
enum RangeParseState {
    ReadingStart,
    ReadingDots,
    ReadingInclusive,
    ReadingEnd(usize),
}

#[derive(Debug, PartialEq)]
pub struct RangeParseErr(String, usize);

impl RangeParseErr {
    fn new<S>(msg: S, at: usize) -> RangeParseErr
    where
        S: ToString,
    {
        RangeParseErr(msg.to_string(), at)
    }
}

const ERR_MALFORMATTED: &str = "Malformatted input";
const ERR_UNEXPECTED: &str = "Unexpected end of input";

pub fn parse_range_str<T, E>(input_str: &str) -> Result<Range<T>, RangeParseErr>
where
    E: ToString,
    T: FromStr<Err = E> + Default,
{
    let following = input_str
        .char_indices()
        .skip(1)
        .map(Some)
        .chain(iter::once(None));

    let mut parse_state = RangeParseState::ReadingStart;

    input_str.char_indices().zip(following).try_fold(
        Range::default(),
        move |mut range, ((current_idx, current_char), next)| {
            match parse_state {
                RangeParseState::ReadingStart => match next {
                    Some((next_idx, next_char)) => {
                        if current_char == '.' && next_char == '.' {
                            let slice = &input_str[0..current_idx];
                            range.lower = Some(
                                slice
                                    .parse::<T>()
                                    .map_err(|e| RangeParseErr(e.to_string(), next_idx))?,
                            );
                            parse_state = RangeParseState::ReadingDots;
                        }
                    }
                    None => {
                        return Err(RangeParseErr::new(ERR_UNEXPECTED, current_idx));
                    }
                },
                RangeParseState::ReadingDots => match next {
                    Some((next_idx, next_char)) => match next_char {
                        '=' => {
                            parse_state = RangeParseState::ReadingInclusive;
                        }
                        n if n.is_numeric() => {
                            parse_state = RangeParseState::ReadingEnd(next_idx);
                        }
                        '.' => {
                            return Err(RangeParseErr::new(ERR_MALFORMATTED, next_idx));
                        }
                        _ => return Err(RangeParseErr::new(ERR_MALFORMATTED, next_idx)),
                    },
                    None => return Err(RangeParseErr::new(ERR_UNEXPECTED, current_idx)),
                },
                RangeParseState::ReadingInclusive => match next {
                    Some((next_idx, next_char)) => {
                        if next_char.is_numeric() || next_char == '.' {
                            range.inclusive = true;
                            parse_state = RangeParseState::ReadingEnd(next_idx);
                        } else {
                            return Err(RangeParseErr::new(ERR_MALFORMATTED, next_idx));
                        }
                    }
                    _ => return Err(RangeParseErr::new(ERR_UNEXPECTED, current_idx)),
                },
                RangeParseState::ReadingEnd(start_idx) => match next {
                    Some((next_idx, next_char)) => {
                        if !(next_char.is_numeric() || next_char == '.') {
                            return Err(RangeParseErr::new(ERR_MALFORMATTED, next_idx));
                        }
                    }
                    None => {
                        if !current_char.is_numeric() {
                            return Err(RangeParseErr::new(ERR_MALFORMATTED, current_idx));
                        }

                        let slice = &input_str[start_idx..current_idx + 1];
                        range.upper = Some(
                            slice
                                .parse::<T>()
                                .map_err(|e| RangeParseErr(e.to_string(), start_idx))?,
                        );
                    }
                },
            }

            Ok(range)
        },
    )
}

#[test]
fn test_valid_i32() {
    assert_eq!(
        parse_range_str::<i32, _>("0..=10").unwrap(),
        Range {
            lower: Some(0),
            upper: Some(10),
            inclusive: true
        }
    );
    assert_eq!(
        parse_range_str::<i32, _>("0..10").unwrap(),
        Range {
            lower: Some(0),
            upper: Some(10),
            inclusive: false
        }
    );
}

#[test]
fn test_valid_f64() {
    assert_eq!(
        parse_range_str::<f64, _>("0..=10").unwrap(),
        Range {
            lower: Some(0.0),
            upper: Some(10.0),
            inclusive: true
        }
    );
    assert_eq!(
        parse_range_str::<f64, _>("0.0..=10.0").unwrap(),
        Range {
            lower: Some(0.0),
            upper: Some(10.0),
            inclusive: true
        }
    );
    assert_eq!(
        parse_range_str::<f64, _>(".1..=.3").unwrap(),
        Range {
            lower: Some(0.1),
            upper: Some(0.3),
            inclusive: true
        }
    );
    assert_eq!(
        parse_range_str::<f64, _>("0.0..10.0").unwrap(),
        Range {
            lower: Some(0.0),
            upper: Some(10.0),
            inclusive: false
        }
    );
    assert_eq!(
        parse_range_str::<f64, _>("0..=10").unwrap(),
        Range {
            lower: Some(0.0),
            upper: Some(10.0),
            inclusive: true
        }
    );
}

#[test]
fn test_invalid() {
    assert_eq!(
        parse_range_str::<i32, _>(&format!("{}..={}", i64::min_value(), i64::max_value()))
            .err()
            .unwrap(),
        RangeParseErr::new("number too small to fit in target type", 21)
    );
    assert_eq!(
        parse_range_str::<f64, _>("0......=10").err().unwrap(),
        RangeParseErr(String::from(ERR_MALFORMATTED), 3)
    );
    assert_eq!(
        parse_range_str::<f64, _>("0......=10").err().unwrap(),
        RangeParseErr(String::from(ERR_MALFORMATTED), 3)
    );
    assert_eq!(
        parse_range_str::<f64, _>("10..=1.0.").err().unwrap(),
        RangeParseErr(String::from(ERR_MALFORMATTED), 8)
    );
    assert_eq!(
        parse_range_str::<f64, _>("10..==1").err().unwrap(),
        RangeParseErr(String::from(ERR_MALFORMATTED), 5)
    );
    assert_eq!(
        parse_range_str::<f64, _>("10").err().unwrap(),
        RangeParseErr(String::from(ERR_UNEXPECTED), 1)
    );
    assert_eq!(
        parse_range_str::<f64, _>("10..").err().unwrap(),
        RangeParseErr(String::from(ERR_UNEXPECTED), 3)
    );
    assert_eq!(
        parse_range_str::<f64, _>("10..=").err().unwrap(),
        RangeParseErr(String::from(ERR_UNEXPECTED), 4)
    );
}
