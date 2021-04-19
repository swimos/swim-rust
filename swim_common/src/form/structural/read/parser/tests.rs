// Copyright 2015-2021 SWIM.AI inc.
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

use super::tokens::{complete, streaming, string_literal};
use super::Span;
use crate::form::structural::read::parser::NumericLiteral;
use either::Either;
use nom::IResult;
use num_bigint::{BigInt, BigUint};
use std::ops::{Add, Neg, Sub};

fn span(input: &str) -> Span<'_> {
    Span::new(input)
}

fn check_output<S, T: PartialEq<S>>(result: IResult<Span<'_>, T>, offset: usize, expected: S) {
    match result {
        Ok((rem, value)) => {
            assert_eq!(rem.location_offset(), offset);
            assert!(value.eq(&expected));
        }
        Err(_) => panic!("Unexpected failure."),
    }
}

#[test]
fn parse_identifier() {
    let input = span("name");
    assert!(matches!(
        streaming::identifier(input),
        Err(nom::Err::Incomplete(_))
    ));

    let input = span("name ");
    check_output(streaming::identifier(input), 4, "name");
}

#[test]
fn parse_identifier_final() {
    let input = span("name");
    check_output(complete::identifier(input), 4, "name");

    let input = span("name ");
    check_output(complete::identifier(input), 4, "name");
}

#[test]
fn parse_empty_string_lit() {
    let input = span(r#""""#);
    check_output(string_literal(input), 2, "");
}

#[test]
fn parse_simple_string_lit() {
    let input = span(r#""two words!""#);
    check_output(string_literal(input), 12, "two words!");
}

#[test]
fn parse_escaped_string_lit() {
    let input = span(r#""two\nlines!""#);
    check_output(string_literal(input), 13, "two\nlines!");
}

#[test]
fn parse_identifier_or_bool() {
    let input = span("true");
    assert!(matches!(
        streaming::identifier_or_bool(input),
        Err(nom::Err::Incomplete(_))
    ));

    let input = span("true ");
    check_output(streaming::identifier_or_bool(input), 4, Either::Right(true));

    let input = span("false ");
    check_output(
        streaming::identifier_or_bool(input),
        5,
        Either::Right(false),
    );

    let input = span("other ");
    check_output(
        streaming::identifier_or_bool(input),
        5,
        Either::Left("other"),
    );
}

#[test]
fn parse_identifier_or_bool_final() {
    let input = span("true");
    check_output(complete::identifier_or_bool(input), 4, Either::Right(true));

    let input = span("false");
    check_output(complete::identifier_or_bool(input), 5, Either::Right(false));

    let input = span("other");
    check_output(
        complete::identifier_or_bool(input),
        5,
        Either::Left("other"),
    );
}

#[test]
fn parse_decimal_int() {
    let input = span("0");
    assert!(matches!(
        streaming::numeric_literal(input),
        Err(nom::Err::Incomplete(_))
    ));

    let input = span("0 ");
    check_output(
        streaming::numeric_literal(input),
        1,
        NumericLiteral::UInt(0),
    );

    let input = span("1 ");
    check_output(
        streaming::numeric_literal(input),
        1,
        NumericLiteral::UInt(1),
    );

    let input = span("124 ");
    check_output(
        streaming::numeric_literal(input),
        3,
        NumericLiteral::UInt(124),
    );

    let input = span("-1 ");
    check_output(
        streaming::numeric_literal(input),
        2,
        NumericLiteral::Int(-1),
    );

    let input = span("-5677 ");
    check_output(
        streaming::numeric_literal(input),
        5,
        NumericLiteral::Int(-5677),
    );

    let big = BigUint::from(u64::max_value()).add(1u64);
    let big_str = format!("{} ", big);
    let input = span(big_str.as_str());
    check_output(
        streaming::numeric_literal(input),
        big_str.len() - 1,
        NumericLiteral::BigUint(big),
    );

    let big_neg = BigInt::from(i64::min_value()).sub(1);
    let big_neg_str = format!("{} ", big_neg);
    let input = span(big_neg_str.as_str());
    check_output(
        streaming::numeric_literal(input),
        big_neg_str.len() - 1,
        NumericLiteral::BigInt(big_neg),
    );
}

#[test]
fn parse_decimal_int_final() {
    let input = span("0");
    check_output(complete::numeric_literal(input), 1, NumericLiteral::UInt(0));

    let input = span("1");
    check_output(complete::numeric_literal(input), 1, NumericLiteral::UInt(1));

    let input = span("124");
    check_output(
        complete::numeric_literal(input),
        3,
        NumericLiteral::UInt(124),
    );

    let input = span("-1");
    check_output(complete::numeric_literal(input), 2, NumericLiteral::Int(-1));

    let input = span("-5677");
    check_output(
        complete::numeric_literal(input),
        5,
        NumericLiteral::Int(-5677),
    );

    let big = BigUint::from(u64::max_value()).add(1u64);
    let big_str = format!("{}", big);
    let input = span(big_str.as_str());
    check_output(
        complete::numeric_literal(input),
        big_str.len(),
        NumericLiteral::BigUint(big),
    );

    let big_neg = BigInt::from(i64::min_value()).sub(1);
    let big_neg_str = format!("{}", big_neg);
    let input = span(big_neg_str.as_str());
    check_output(
        complete::numeric_literal(input),
        big_neg_str.len(),
        NumericLiteral::BigInt(big_neg),
    );
}

#[test]
fn parse_hex_int() {
    let input = span("0x0 ");
    assert!(matches!(
        streaming::numeric_literal(input),
        Err(nom::Err::Incomplete(_))
    ));

    let input = span("0x0 ");
    check_output(
        streaming::numeric_literal(input),
        3,
        NumericLiteral::UInt(0),
    );

    let input = span("0xA ");
    check_output(
        streaming::numeric_literal(input),
        3,
        NumericLiteral::UInt(0xA),
    );

    let input = span("0x0a5c ");
    check_output(
        streaming::numeric_literal(input),
        6,
        NumericLiteral::UInt(0x0a5c),
    );

    let input = span("-0x1 ");
    check_output(
        streaming::numeric_literal(input),
        4,
        NumericLiteral::Int(-1),
    );

    let input = span("-0xAB00 ");
    check_output(
        streaming::numeric_literal(input),
        7,
        NumericLiteral::Int(-0xAB00),
    );

    let big = BigUint::from(u64::max_value()).add(1u64);
    let big_str = format!("0x{} ", big.to_str_radix(16));
    let input = span(big_str.as_str());
    check_output(
        streaming::numeric_literal(input),
        big_str.len() - 1,
        NumericLiteral::BigUint(big.clone()),
    );

    let big_neg = BigInt::from(big.clone()).neg();
    let big_neg_str = format!("-0x{} ", big.to_str_radix(16));
    let input = span(big_neg_str.as_str());
    check_output(
        streaming::numeric_literal(input),
        big_neg_str.len() - 1,
        NumericLiteral::BigInt(big_neg),
    );
}

#[test]
fn parse_hex_int_final() {
    let input = span("0x0");
    check_output(complete::numeric_literal(input), 3, NumericLiteral::UInt(0));

    let input = span("0xA");
    check_output(
        complete::numeric_literal(input),
        3,
        NumericLiteral::UInt(0xA),
    );

    let input = span("0x0a5c");
    check_output(
        complete::numeric_literal(input),
        6,
        NumericLiteral::UInt(0x0a5c),
    );

    let input = span("-0x1");
    check_output(complete::numeric_literal(input), 4, NumericLiteral::Int(-1));

    let input = span("-0xAB00");
    check_output(
        complete::numeric_literal(input),
        7,
        NumericLiteral::Int(-0xAB00),
    );

    let big = BigUint::from(u64::max_value()).add(1u64);
    let big_str = format!("0x{}", big.to_str_radix(16));
    let input = span(big_str.as_str());
    check_output(
        complete::numeric_literal(input),
        big_str.len(),
        NumericLiteral::BigUint(big.clone()),
    );

    let big_neg = BigInt::from(big.clone()).neg();
    let big_neg_str = format!("-0x{}", big.to_str_radix(16));
    let input = span(big_neg_str.as_str());
    check_output(
        complete::numeric_literal(input),
        big_neg_str.len(),
        NumericLiteral::BigInt(big_neg),
    );
}

#[test]
fn parse_bin_int() {
    let input = span("0b0");
    assert!(matches!(
        streaming::numeric_literal(input),
        Err(nom::Err::Incomplete(_))
    ));

    let input = span("0b0 ");
    check_output(
        streaming::numeric_literal(input),
        3,
        NumericLiteral::UInt(0),
    );

    let input = span("0b1 ");
    check_output(
        streaming::numeric_literal(input),
        3,
        NumericLiteral::UInt(0b1),
    );

    let input = span("0b0110 ");
    check_output(
        streaming::numeric_literal(input),
        6,
        NumericLiteral::UInt(0b0110),
    );

    let input = span("-0b1 ");
    check_output(
        streaming::numeric_literal(input),
        4,
        NumericLiteral::Int(-1),
    );

    let input = span("-0b1100 ");
    check_output(
        streaming::numeric_literal(input),
        7,
        NumericLiteral::Int(-0b1100),
    );

    let big = BigUint::from(u64::max_value()).add(1u64);
    let big_str = format!("0b{} ", big.to_str_radix(2));
    let input = span(big_str.as_str());
    check_output(
        streaming::numeric_literal(input),
        big_str.len() - 1,
        NumericLiteral::BigUint(big.clone()),
    );

    let big_neg = BigInt::from(big.clone()).neg();
    let big_neg_str = format!("-0b{} ", big.to_str_radix(2));
    let input = span(big_neg_str.as_str());
    check_output(
        streaming::numeric_literal(input),
        big_neg_str.len() - 1,
        NumericLiteral::BigInt(big_neg),
    );
}

#[test]
fn parse_bin_int_final() {
    let input = span("0b0");
    check_output(complete::numeric_literal(input), 3, NumericLiteral::UInt(0));

    let input = span("0b1");
    check_output(
        complete::numeric_literal(input),
        3,
        NumericLiteral::UInt(0b1),
    );

    let input = span("0b0110");
    check_output(
        complete::numeric_literal(input),
        6,
        NumericLiteral::UInt(0b0110),
    );

    let input = span("-0b1");
    check_output(complete::numeric_literal(input), 4, NumericLiteral::Int(-1));

    let input = span("-0b1100");
    check_output(
        complete::numeric_literal(input),
        7,
        NumericLiteral::Int(-0b1100),
    );

    let big = BigUint::from(u64::max_value()).add(1u64);
    let big_str = format!("0b{}", big.to_str_radix(2));
    let input = span(big_str.as_str());
    check_output(
        complete::numeric_literal(input),
        big_str.len(),
        NumericLiteral::BigUint(big.clone()),
    );

    let big_neg = BigInt::from(big.clone()).neg();
    let big_neg_str = format!("-0b{}", big.to_str_radix(2));
    let input = span(big_neg_str.as_str());
    check_output(
        complete::numeric_literal(input),
        big_neg_str.len(),
        NumericLiteral::BigInt(big_neg),
    );
}

#[test]
fn parse_float() {
    let input = span("0.0");
    assert!(matches!(
        streaming::numeric_literal(input),
        Err(nom::Err::Incomplete(_))
    ));

    let input = span("0.0 ");
    check_output(
        streaming::numeric_literal(input),
        3,
        NumericLiteral::Float(0.0),
    );

    let input = span("1.0 ");
    check_output(
        streaming::numeric_literal(input),
        3,
        NumericLiteral::Float(1.0),
    );

    let input = span("-0.5 ");
    check_output(
        streaming::numeric_literal(input),
        4,
        NumericLiteral::Float(-0.5),
    );

    let input = span("3.135e12 ");
    check_output(
        streaming::numeric_literal(input),
        8,
        NumericLiteral::Float(3.135e12),
    );
    let input = span("-0.135e-12 ");
    check_output(
        streaming::numeric_literal(input),
        10,
        NumericLiteral::Float(-0.135e-12),
    );
}

#[test]
fn parse_float_final() {
    let input = span("0.0");
    check_output(
        complete::numeric_literal(input),
        3,
        NumericLiteral::Float(0.0),
    );

    let input = span("1.0");
    check_output(
        complete::numeric_literal(input),
        3,
        NumericLiteral::Float(1.0),
    );

    let input = span("-0.5");
    check_output(
        complete::numeric_literal(input),
        4,
        NumericLiteral::Float(-0.5),
    );

    let input = span("3.135e12");
    check_output(
        complete::numeric_literal(input),
        8,
        NumericLiteral::Float(3.135e12),
    );
    let input = span("-0.135e-12");
    check_output(
        complete::numeric_literal(input),
        10,
        NumericLiteral::Float(-0.135e-12),
    );
}

#[test]
fn parse_blob() {
    let input = span("%YW55IGNhcm5hbCBwbGVhc3Vy");
    assert!(matches!(
        streaming::blob(input),
        Err(nom::Err::Incomplete(_))
    ));

    let input = span("%YW55IGNhcm5hbCBwbGVhc3VyZQ==");
    let expected = "any carnal pleasure";

    let (rem, result) = streaming::blob(input).unwrap();
    assert_eq!(*rem, "");
    assert_eq!(result.as_slice(), expected.as_bytes());
}

#[test]
fn parse_blob_final() {
    let input = span("%YW55IGNhcm5hbCBwbGVhc3Vy");
    let expected = "any carnal pleasur";
    let (rem, result) = complete::blob(input).unwrap();
    assert_eq!(*rem, "");
    assert_eq!(result.as_slice(), expected.as_bytes());

    let input = span("%YW55IGNhcm5hbCBwbGVhc3VyZQ==");
    let expected = "any carnal pleasure";

    let (rem, result) = complete::blob(input).unwrap();
    assert_eq!(*rem, "");
    assert_eq!(result.as_slice(), expected.as_bytes());
}
