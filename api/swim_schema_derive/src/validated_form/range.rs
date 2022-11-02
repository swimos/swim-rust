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

use std::iter;
use std::str::FromStr;

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Range<T> {
    pub lower: T,
    pub upper: T,
    pub inclusive: bool,
}

#[derive(Default)]
struct RangeOpt<T> {
    pub lower: Option<T>,
    pub upper: Option<T>,
    pub inclusive: bool,
}

#[derive(Debug)]
enum RangeParseState {
    Start,
    Dots,
    Inclusive,
    End(usize),
}

#[derive(Debug, PartialEq, Eq)]
pub struct RangeParseErr(pub String, pub usize);

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

    let mut parse_state = RangeParseState::Start;

    let result = input_str.char_indices().zip(following).try_fold(
        RangeOpt::default(),
        move |mut range, ((current_idx, current_char), next)| {
            match parse_state {
                RangeParseState::Start => match next {
                    Some((next_idx, next_char)) => {
                        if current_char == '.' && next_char == '.' {
                            let slice = &input_str[0..current_idx];
                            range.lower = Some(
                                slice
                                    .parse::<T>()
                                    .map_err(|e| RangeParseErr(e.to_string(), next_idx))?,
                            );
                            parse_state = RangeParseState::Dots;
                        }
                    }
                    None => {
                        return Err(RangeParseErr::new(ERR_UNEXPECTED, current_idx));
                    }
                },
                RangeParseState::Dots => match next {
                    Some((next_idx, next_char)) => match next_char {
                        '=' => {
                            parse_state = RangeParseState::Inclusive;
                        }
                        n if n.is_numeric() || n == '-' => {
                            parse_state = RangeParseState::End(next_idx);
                        }
                        _ => return Err(RangeParseErr::new(ERR_MALFORMATTED, next_idx)),
                    },
                    None => return Err(RangeParseErr::new(ERR_UNEXPECTED, current_idx)),
                },
                RangeParseState::Inclusive => match next {
                    Some((next_idx, next_char)) => {
                        if next_char.is_numeric() || next_char == '.' || next_char == '-' {
                            range.inclusive = true;
                            parse_state = RangeParseState::End(next_idx);
                        } else {
                            return Err(RangeParseErr::new(ERR_MALFORMATTED, next_idx));
                        }
                    }
                    _ => return Err(RangeParseErr::new(ERR_UNEXPECTED, current_idx)),
                },
                RangeParseState::End(start_idx) => match next {
                    Some((next_idx, next_char)) => {
                        if !(next_char.is_numeric() || next_char == '.' || next_char == '-') {
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
    );

    match result {
        Ok(r) => Ok(Range {
            lower: r.lower.unwrap(),
            upper: r.upper.unwrap(),
            inclusive: r.inclusive,
        }),
        Err(e) => Err(e),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use num_bigint::{BigInt, BigUint, RandBigInt};
    use std::convert::TryFrom;

    #[test]
    fn test_valid_i32() {
        assert_eq!(
            parse_range_str::<i32, _>("0..=10").unwrap(),
            Range {
                lower: 0,
                upper: 10,
                inclusive: true
            }
        );
        assert_eq!(
            parse_range_str::<i32, _>("-1..=10").unwrap(),
            Range {
                lower: -1,
                upper: 10,
                inclusive: true
            }
        );
        assert_eq!(
            parse_range_str::<i32, _>("-1..=-10").unwrap(),
            Range {
                lower: -1,
                upper: -10,
                inclusive: true
            }
        );
        assert_eq!(
            parse_range_str::<i32, _>("1..=-10").unwrap(),
            Range {
                lower: 1,
                upper: -10,
                inclusive: true
            }
        );
        assert_eq!(
            parse_range_str::<i32, _>("0..10").unwrap(),
            Range {
                lower: 0,
                upper: 10,
                inclusive: false
            }
        );
    }

    #[test]
    fn test_valid_f64() {
        assert_eq!(
            parse_range_str::<f64, _>("0..=10").unwrap(),
            Range {
                lower: 0.0,
                upper: 10.0,
                inclusive: true
            }
        );
        assert_eq!(
            parse_range_str::<f64, _>("0..=-10").unwrap(),
            Range {
                lower: 0.0,
                upper: -10.0,
                inclusive: true
            }
        );
        assert_eq!(
            parse_range_str::<f64, _>("-10..=10").unwrap(),
            Range {
                lower: -10.0,
                upper: 10.0,
                inclusive: true
            }
        );
        assert_eq!(
            parse_range_str::<f64, _>("-100..=-10").unwrap(),
            Range {
                lower: -100.0,
                upper: -10.0,
                inclusive: true
            }
        );
        assert_eq!(
            parse_range_str::<f64, _>("0.0..=10.0").unwrap(),
            Range {
                lower: 0.0,
                upper: 10.0,
                inclusive: true
            }
        );
        assert_eq!(
            parse_range_str::<f64, _>(".1..=.3").unwrap(),
            Range {
                lower: 0.1,
                upper: 0.3,
                inclusive: true
            }
        );
        assert_eq!(
            parse_range_str::<f64, _>("0.0..10.0").unwrap(),
            Range {
                lower: 0.0,
                upper: 10.0,
                inclusive: false
            }
        );
        assert_eq!(
            parse_range_str::<f64, _>("0..=10").unwrap(),
            Range {
                lower: 0.0,
                upper: 10.0,
                inclusive: true
            }
        );
    }

    #[test]
    fn test_bigint() {
        assert_eq!(
            parse_range_str::<BigInt, _>("0..=10").unwrap(),
            Range {
                lower: BigInt::from(0),
                upper: BigInt::from(10),
                inclusive: true
            }
        );
        assert_eq!(
            parse_range_str::<BigUint, _>("0..=10").unwrap(),
            Range {
                lower: BigUint::try_from(0).unwrap(),
                upper: BigUint::try_from(10).unwrap(),
                inclusive: true
            }
        );
        {
            let mut rng = rand::thread_rng();
            let rand_lower = rng.gen_bigint(1000);
            let rand_upper = rng.gen_bigint(1000);
            let rng_str = &format!("{}..={}", rand_lower, rand_upper);

            assert_eq!(
                parse_range_str::<BigInt, _>(rng_str).unwrap(),
                Range {
                    lower: rand_lower,
                    upper: rand_upper,
                    inclusive: true
                }
            );
        }
        {
            let mut rng = rand::thread_rng();
            let rand_lower = rng.gen_bigint(1000);
            let rand_upper = rng.gen_bigint(1000);
            let rng_str = &format!("{}..{}", rand_lower, rand_upper);

            assert_eq!(
                parse_range_str::<BigInt, _>(rng_str).unwrap(),
                Range {
                    lower: rand_lower,
                    upper: rand_upper,
                    inclusive: false
                }
            );
        }
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
}
