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
use crate::parser::record::ParseIterator;
use crate::parser::ParseError;
use either::Either;
use nom::IResult;
use std::borrow::Cow;
use std::ops::{Add, Neg, Sub};
use swim_form::structural::read::event::{NumericValue, ReadEvent};
use swim_model::bigint::{BigInt, BigUint};
use swim_model::{Attr, Item, Text, Value};

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
    check_output(streaming::numeric_literal(input), 1, NumericValue::UInt(0));

    let input = span("1 ");
    check_output(streaming::numeric_literal(input), 1, NumericValue::UInt(1));

    let input = span("124 ");
    check_output(
        streaming::numeric_literal(input),
        3,
        NumericValue::UInt(124),
    );

    let input = span("-1 ");
    check_output(streaming::numeric_literal(input), 2, NumericValue::Int(-1));

    let input = span("-5677 ");
    check_output(
        streaming::numeric_literal(input),
        5,
        NumericValue::Int(-5677),
    );

    let big = BigUint::from(u64::MAX).add(1u64);
    let big_str = format!("{} ", big);
    let input = span(big_str.as_str());
    check_output(
        streaming::numeric_literal(input),
        big_str.len() - 1,
        NumericValue::BigUint(big),
    );

    let big_neg = BigInt::from(i64::MIN).sub(1);
    let big_neg_str = format!("{} ", big_neg);
    let input = span(big_neg_str.as_str());
    check_output(
        streaming::numeric_literal(input),
        big_neg_str.len() - 1,
        NumericValue::BigInt(big_neg),
    );
}

#[test]
fn parse_decimal_int_final() {
    let input = span("0");
    check_output(complete::numeric_literal(input), 1, NumericValue::UInt(0));

    let input = span("1");
    check_output(complete::numeric_literal(input), 1, NumericValue::UInt(1));

    let input = span("124");
    check_output(complete::numeric_literal(input), 3, NumericValue::UInt(124));

    let input = span("-1");
    check_output(complete::numeric_literal(input), 2, NumericValue::Int(-1));

    let input = span("-5677");
    check_output(
        complete::numeric_literal(input),
        5,
        NumericValue::Int(-5677),
    );

    let big = BigUint::from(u64::MAX).add(1u64);
    let big_str = format!("{}", big);
    let input = span(big_str.as_str());
    check_output(
        complete::numeric_literal(input),
        big_str.len(),
        NumericValue::BigUint(big),
    );

    let big_neg = BigInt::from(i64::MIN).sub(1);
    let big_neg_str = format!("{}", big_neg);
    let input = span(big_neg_str.as_str());
    check_output(
        complete::numeric_literal(input),
        big_neg_str.len(),
        NumericValue::BigInt(big_neg),
    );
}

#[test]
fn parse_hex_int() {
    let input = span("0x0");
    assert!(matches!(
        streaming::numeric_literal(input),
        Err(nom::Err::Incomplete(_))
    ));

    let input = span("0x0 ");
    check_output(streaming::numeric_literal(input), 3, NumericValue::UInt(0));

    let input = span("0xA ");
    check_output(
        streaming::numeric_literal(input),
        3,
        NumericValue::UInt(0xA),
    );

    let input = span("0x0a5c ");
    check_output(
        streaming::numeric_literal(input),
        6,
        NumericValue::UInt(0x0a5c),
    );

    let input = span("-0x1 ");
    check_output(streaming::numeric_literal(input), 4, NumericValue::Int(-1));

    let input = span("-0xAB00 ");
    check_output(
        streaming::numeric_literal(input),
        7,
        NumericValue::Int(-0xAB00),
    );

    let big = BigUint::from(u64::MAX).add(1u64);
    let big_str = format!("0x{} ", big.to_str_radix(16));
    let input = span(big_str.as_str());
    check_output(
        streaming::numeric_literal(input),
        big_str.len() - 1,
        NumericValue::BigUint(big.clone()),
    );

    let big_neg = BigInt::from(big.clone()).neg();
    let big_neg_str = format!("-0x{} ", big.to_str_radix(16));
    let input = span(big_neg_str.as_str());
    check_output(
        streaming::numeric_literal(input),
        big_neg_str.len() - 1,
        NumericValue::BigInt(big_neg),
    );
}

#[test]
fn parse_hex_int_final() {
    let input = span("0x0");
    check_output(complete::numeric_literal(input), 3, NumericValue::UInt(0));

    let input = span("0xA");
    check_output(complete::numeric_literal(input), 3, NumericValue::UInt(0xA));

    let input = span("0x0a5c");
    check_output(
        complete::numeric_literal(input),
        6,
        NumericValue::UInt(0x0a5c),
    );

    let input = span("-0x1");
    check_output(complete::numeric_literal(input), 4, NumericValue::Int(-1));

    let input = span("-0xAB00");
    check_output(
        complete::numeric_literal(input),
        7,
        NumericValue::Int(-0xAB00),
    );

    let big = BigUint::from(u64::MAX).add(1u64);
    let big_str = format!("0x{}", big.to_str_radix(16));
    let input = span(big_str.as_str());
    check_output(
        complete::numeric_literal(input),
        big_str.len(),
        NumericValue::BigUint(big.clone()),
    );

    let big_neg = BigInt::from(big.clone()).neg();
    let big_neg_str = format!("-0x{}", big.to_str_radix(16));
    let input = span(big_neg_str.as_str());
    check_output(
        complete::numeric_literal(input),
        big_neg_str.len(),
        NumericValue::BigInt(big_neg),
    );
}

#[test]
fn parse_big_int() {
    let input = span("0b0");
    assert!(matches!(
        streaming::numeric_literal(input),
        Err(nom::Err::Incomplete(_))
    ));

    let input = span("0b0 ");
    check_output(streaming::numeric_literal(input), 3, NumericValue::UInt(0));

    let input = span("0b1 ");
    check_output(
        streaming::numeric_literal(input),
        3,
        NumericValue::UInt(0b1),
    );

    let input = span("0b0110 ");
    check_output(
        streaming::numeric_literal(input),
        6,
        NumericValue::UInt(0b0110),
    );

    let input = span("-0b1 ");
    check_output(streaming::numeric_literal(input), 4, NumericValue::Int(-1));

    let input = span("-0b1100 ");
    check_output(
        streaming::numeric_literal(input),
        7,
        NumericValue::Int(-0b1100),
    );

    let big = BigUint::from(u64::MAX).add(1u64);
    let big_str = format!("0b{} ", big.to_str_radix(2));
    let input = span(big_str.as_str());
    check_output(
        streaming::numeric_literal(input),
        big_str.len() - 1,
        NumericValue::BigUint(big.clone()),
    );

    let big_neg = BigInt::from(big.clone()).neg();
    let big_neg_str = format!("-0b{} ", big.to_str_radix(2));
    let input = span(big_neg_str.as_str());
    check_output(
        streaming::numeric_literal(input),
        big_neg_str.len() - 1,
        NumericValue::BigInt(big_neg),
    );
}

#[test]
fn parse_big_int_final() {
    let input = span("0b0");
    check_output(complete::numeric_literal(input), 3, NumericValue::UInt(0));

    let input = span("0b1");
    check_output(complete::numeric_literal(input), 3, NumericValue::UInt(0b1));

    let input = span("0b0110");
    check_output(
        complete::numeric_literal(input),
        6,
        NumericValue::UInt(0b0110),
    );

    let input = span("-0b1");
    check_output(complete::numeric_literal(input), 4, NumericValue::Int(-1));

    let input = span("-0b1100");
    check_output(
        complete::numeric_literal(input),
        7,
        NumericValue::Int(-0b1100),
    );

    let big = BigUint::from(u64::MAX).add(1u64);
    let big_str = format!("0b{}", big.to_str_radix(2));
    let input = span(big_str.as_str());
    check_output(
        complete::numeric_literal(input),
        big_str.len(),
        NumericValue::BigUint(big.clone()),
    );

    let big_neg = BigInt::from(big.clone()).neg();
    let big_neg_str = format!("-0b{}", big.to_str_radix(2));
    let input = span(big_neg_str.as_str());
    check_output(
        complete::numeric_literal(input),
        big_neg_str.len(),
        NumericValue::BigInt(big_neg),
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
        NumericValue::Float(0.0),
    );

    let input = span("1.0 ");
    check_output(
        streaming::numeric_literal(input),
        3,
        NumericValue::Float(1.0),
    );

    let input = span("-0.5 ");
    check_output(
        streaming::numeric_literal(input),
        4,
        NumericValue::Float(-0.5),
    );

    let input = span("3.135e12 ");
    check_output(
        streaming::numeric_literal(input),
        8,
        NumericValue::Float(3.135e12),
    );
    let input = span("-0.135e-12 ");
    check_output(
        streaming::numeric_literal(input),
        10,
        NumericValue::Float(-0.135e-12),
    );
}

#[test]
fn parse_float_final() {
    let input = span("0.0");
    check_output(
        complete::numeric_literal(input),
        3,
        NumericValue::Float(0.0),
    );

    let input = span("1.0");
    check_output(
        complete::numeric_literal(input),
        3,
        NumericValue::Float(1.0),
    );

    let input = span("-0.5");
    check_output(
        complete::numeric_literal(input),
        4,
        NumericValue::Float(-0.5),
    );

    let input = span("3.135e12");
    check_output(
        complete::numeric_literal(input),
        8,
        NumericValue::Float(3.135e12),
    );
    let input = span("-0.135e-12");
    check_output(
        complete::numeric_literal(input),
        10,
        NumericValue::Float(-0.135e-12),
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

fn run_parser_iterator(input: &str) -> Result<Vec<ReadEvent<'_>>, nom::error::Error<Span<'_>>> {
    let it = ParseIterator::new(Span::new(input));
    let mut v = Vec::new();
    for r in it {
        v.push(r?);
    }
    Ok(v)
}

#[test]
fn single_int() {
    let result = run_parser_iterator("1").unwrap();
    assert!(matches!(
        result.as_slice(),
        [ReadEvent::Number(NumericValue::UInt(1))]
    ));

    let result = run_parser_iterator(" 1").unwrap();
    assert!(matches!(
        result.as_slice(),
        [ReadEvent::Number(NumericValue::UInt(1))]
    ));

    let result = run_parser_iterator("1 ").unwrap();
    assert!(matches!(
        result.as_slice(),
        [ReadEvent::Number(NumericValue::UInt(1))]
    ));

    let result = run_parser_iterator("\n1").unwrap();
    assert!(matches!(
        result.as_slice(),
        [ReadEvent::Number(NumericValue::UInt(1))]
    ));

    let result = run_parser_iterator("\r\n1").unwrap();
    assert!(matches!(
        result.as_slice(),
        [ReadEvent::Number(NumericValue::UInt(1))]
    ));
}

#[test]
fn single_string() {
    let result = run_parser_iterator(r#""two words""#).unwrap();
    assert!(matches!(result.as_slice(), [ReadEvent::TextValue(t)] if t == "two words"));

    let result = run_parser_iterator(r#" "two words""#).unwrap();
    assert!(matches!(result.as_slice(), [ReadEvent::TextValue(t)] if t == "two words"));

    let result = run_parser_iterator(r#""two words" "#).unwrap();
    assert!(matches!(result.as_slice(), [ReadEvent::TextValue(t)] if t == "two words"));

    let result = run_parser_iterator("\n\"two words\"").unwrap();
    assert!(matches!(result.as_slice(), [ReadEvent::TextValue(t)] if t == "two words"));
}

#[test]
fn single_identifier() {
    let result = run_parser_iterator("text").unwrap();
    assert!(matches!(result.as_slice(), [ReadEvent::TextValue(t)] if t == "text"));

    let result = run_parser_iterator(" text").unwrap();
    assert!(matches!(result.as_slice(), [ReadEvent::TextValue(t)] if t == "text"));

    let result = run_parser_iterator("text ").unwrap();
    assert!(matches!(result.as_slice(), [ReadEvent::TextValue(t)] if t == "text"));

    let result = run_parser_iterator("\ntext").unwrap();
    assert!(matches!(result.as_slice(), [ReadEvent::TextValue(t)] if t == "text"));
}

#[test]
fn single_float() {
    let result = run_parser_iterator("-1.5e67").unwrap();
    assert!(
        matches!(result.as_slice(), [ReadEvent::Number(NumericValue::Float(x))] if x.eq(&-1.5e67))
    );

    let result = run_parser_iterator(" -1.5e67").unwrap();
    assert!(
        matches!(result.as_slice(), [ReadEvent::Number(NumericValue::Float(x))] if x.eq(&-1.5e67))
    );

    let result = run_parser_iterator("-1.5e67 ").unwrap();
    assert!(
        matches!(result.as_slice(), [ReadEvent::Number(NumericValue::Float(x))] if x.eq(&-1.5e67))
    );

    let result = run_parser_iterator("\n-1.5e67").unwrap();
    assert!(
        matches!(result.as_slice(), [ReadEvent::Number(NumericValue::Float(x))] if x.eq(&-1.5e67))
    );

    let result = run_parser_iterator("\r\n-1.5e67").unwrap();
    assert!(
        matches!(result.as_slice(), [ReadEvent::Number(NumericValue::Float(x))] if x.eq(&-1.5e67))
    );
}

#[test]
fn empty() {
    let result = run_parser_iterator("").unwrap();
    assert!(matches!(result.as_slice(), [ReadEvent::Extant]));

    let result = run_parser_iterator(" ").unwrap();
    assert!(matches!(result.as_slice(), [ReadEvent::Extant]));

    let result = run_parser_iterator("\n").unwrap();
    assert!(matches!(result.as_slice(), [ReadEvent::Extant]));
}

#[test]
fn empty_record() {
    let result = run_parser_iterator("{}").unwrap();
    assert!(matches!(
        result.as_slice(),
        [ReadEvent::StartBody, ReadEvent::EndRecord]
    ));

    let result = run_parser_iterator("{ }").unwrap();
    assert!(matches!(
        result.as_slice(),
        [ReadEvent::StartBody, ReadEvent::EndRecord]
    ));

    let result = run_parser_iterator("{\n}").unwrap();
    assert!(matches!(
        result.as_slice(),
        [ReadEvent::StartBody, ReadEvent::EndRecord]
    ));

    let result = run_parser_iterator("{\r\n}").unwrap();
    assert!(matches!(
        result.as_slice(),
        [ReadEvent::StartBody, ReadEvent::EndRecord]
    ));
}

fn uint_event<'a>(n: u64) -> ReadEvent<'a> {
    ReadEvent::Number(NumericValue::UInt(n))
}

fn string_event(string: &str) -> ReadEvent<'_> {
    ReadEvent::TextValue(Cow::Borrowed(string))
}

#[test]
fn singleton_record() {
    let expected = vec![ReadEvent::StartBody, uint_event(1), ReadEvent::EndRecord];

    let result = run_parser_iterator("{1}").unwrap();
    assert_eq!(result, expected);

    let result = run_parser_iterator("{ 1 }").unwrap();
    assert_eq!(result, expected);

    let result = run_parser_iterator("{\n 1 }").unwrap();
    assert_eq!(result, expected);

    let result = run_parser_iterator("{\r\n 1 }").unwrap();
    assert_eq!(result, expected);
}

#[test]
fn simple_record() {
    let expected = vec![
        ReadEvent::StartBody,
        uint_event(1),
        string_event("two"),
        uint_event(3),
        ReadEvent::EndRecord,
    ];

    let result = run_parser_iterator("{1,two,3}").unwrap();
    assert_eq!(result, expected);

    let result = run_parser_iterator("{ 1, two, 3 }").unwrap();
    assert_eq!(result, expected);
}

#[test]
fn missing_items() {
    let result = run_parser_iterator("{,two,3}").unwrap();
    assert_eq!(
        result,
        vec![
            ReadEvent::StartBody,
            ReadEvent::Extant,
            string_event("two"),
            uint_event(3),
            ReadEvent::EndRecord
        ]
    );

    let result = run_parser_iterator("{1,,3}").unwrap();
    assert_eq!(
        result,
        vec![
            ReadEvent::StartBody,
            uint_event(1),
            ReadEvent::Extant,
            uint_event(3),
            ReadEvent::EndRecord
        ]
    );

    let result = run_parser_iterator("{1,two,}").unwrap();
    assert_eq!(
        result,
        vec![
            ReadEvent::StartBody,
            uint_event(1),
            string_event("two"),
            ReadEvent::Extant,
            ReadEvent::EndRecord
        ]
    );
}

#[test]
fn newline_seperators() {
    let expected = vec![
        ReadEvent::StartBody,
        uint_event(1),
        string_event("two"),
        uint_event(3),
        ReadEvent::EndRecord,
    ];

    let result = run_parser_iterator(
        r#"{1
            two
            3}"#,
    )
    .unwrap();
    assert_eq!(result, expected);

    let result = run_parser_iterator(
        r#"{
                1
                two
                3
            }"#,
    )
    .unwrap();
    assert_eq!(result, expected);
}

#[test]
fn singleton_slot() {
    let expected = vec![
        ReadEvent::StartBody,
        string_event("name"),
        ReadEvent::Slot,
        uint_event(1),
        ReadEvent::EndRecord,
    ];

    let result = run_parser_iterator("{name:1}").unwrap();
    assert_eq!(result, expected);

    let result = run_parser_iterator("{ name: 1 }").unwrap();
    assert_eq!(result, expected);

    let result = run_parser_iterator("{\n name: 1 }").unwrap();
    assert_eq!(result, expected);
}

#[test]
fn missing_slot_value() {
    let expected = vec![
        ReadEvent::StartBody,
        string_event("name"),
        ReadEvent::Slot,
        ReadEvent::Extant,
        ReadEvent::EndRecord,
    ];

    let result = run_parser_iterator("{name:}").unwrap();
    assert_eq!(result, expected);

    let result = run_parser_iterator("{ name: }").unwrap();
    assert_eq!(result, expected);

    let result = run_parser_iterator("{\n name:\n }").unwrap();
    assert_eq!(result, expected);
}

#[test]
fn missing_slot_key() {
    let expected = vec![
        ReadEvent::StartBody,
        ReadEvent::Extant,
        ReadEvent::Slot,
        uint_event(1),
        ReadEvent::EndRecord,
    ];

    let result = run_parser_iterator("{:1}").unwrap();
    assert_eq!(result, expected);

    let result = run_parser_iterator("{ : 1 }").unwrap();
    assert_eq!(result, expected);

    let result = run_parser_iterator("{\n : 1 }").unwrap();
    assert_eq!(result, expected);
}

#[test]
fn simple_slots_record() {
    let expected = vec![
        ReadEvent::StartBody,
        string_event("first"),
        ReadEvent::Slot,
        uint_event(1),
        string_event("second"),
        ReadEvent::Slot,
        string_event("two"),
        string_event("third"),
        ReadEvent::Slot,
        uint_event(3),
        ReadEvent::EndRecord,
    ];

    let result = run_parser_iterator("{first:1,second:two,third:3}").unwrap();
    assert_eq!(result, expected);

    let result = run_parser_iterator("{ first: 1, second: two, third: 3 }").unwrap();
    assert_eq!(result, expected);
}

#[test]
fn missing_slot_parts() {
    let expected = vec![
        ReadEvent::StartBody,
        string_event("first"),
        ReadEvent::Slot,
        uint_event(1),
        string_event("second"),
        ReadEvent::Slot,
        ReadEvent::Extant,
        ReadEvent::Extant,
        ReadEvent::Slot,
        uint_event(3),
        ReadEvent::EndRecord,
    ];

    let result = run_parser_iterator("{first:1,second:,:3}").unwrap();
    assert_eq!(result, expected);

    let result = run_parser_iterator("{ first: 1, second: , : 3 }").unwrap();
    assert_eq!(result, expected);
}

fn attr_event(name: &str) -> ReadEvent<'_> {
    ReadEvent::StartAttribute(Cow::Borrowed(name))
}

#[test]
fn tag_attribute() {
    let expected = vec![
        attr_event("tag"),
        ReadEvent::EndAttribute,
        ReadEvent::StartBody,
        ReadEvent::EndRecord,
    ];

    let result = run_parser_iterator("@tag").unwrap();
    assert_eq!(result, expected);

    let result = run_parser_iterator("@tag {}").unwrap();
    assert_eq!(result, expected);
}

#[test]
fn attr_simple_body() {
    let expected = vec![
        attr_event("name"),
        uint_event(2),
        ReadEvent::EndAttribute,
        ReadEvent::StartBody,
        ReadEvent::EndRecord,
    ];

    let result = run_parser_iterator("@name(2)").unwrap();
    assert_eq!(result, expected);

    let result = run_parser_iterator("@name(2) {}").unwrap();
    assert_eq!(result, expected);
}

#[test]
fn attr_slot_body() {
    let expected = vec![
        attr_event("name"),
        string_event("a"),
        ReadEvent::Slot,
        ReadEvent::Boolean(true),
        ReadEvent::EndAttribute,
        ReadEvent::StartBody,
        ReadEvent::EndRecord,
    ];

    let result = run_parser_iterator("@name(a:true)").unwrap();
    assert_eq!(result, expected);

    let result = run_parser_iterator("@name(a:true) {}").unwrap();
    assert_eq!(result, expected);
}

#[test]
fn attr_multiple_item_body() {
    let expected = vec![
        attr_event("name"),
        uint_event(1),
        string_event("a"),
        ReadEvent::Slot,
        ReadEvent::Boolean(true),
        ReadEvent::Extant,
        ReadEvent::EndAttribute,
        ReadEvent::StartBody,
        ReadEvent::EndRecord,
    ];

    let result = run_parser_iterator("@name(1, a: true,)").unwrap();
    assert_eq!(result, expected);

    let result = run_parser_iterator("@name(1, a: true,) {}").unwrap();
    assert_eq!(result, expected);
}

#[test]
fn multiple_attributes() {
    let expected = vec![
        attr_event("first"),
        ReadEvent::EndAttribute,
        attr_event("second"),
        ReadEvent::EndAttribute,
        ReadEvent::StartBody,
        ReadEvent::EndRecord,
    ];

    let result = run_parser_iterator("@first@second").unwrap();
    assert_eq!(result, expected);

    let result = run_parser_iterator("@first@second {}").unwrap();
    assert_eq!(result, expected);
}

#[test]
fn multiple_attributes_with_bodies() {
    let expected = vec![
        attr_event("first"),
        uint_event(1),
        ReadEvent::EndAttribute,
        attr_event("second"),
        uint_event(2),
        ReadEvent::EndAttribute,
        ReadEvent::StartBody,
        ReadEvent::EndRecord,
    ];

    let result = run_parser_iterator("@first(1)@second(2)").unwrap();
    assert_eq!(result, expected);

    let result = run_parser_iterator("@first(1)@second(2) {}").unwrap();
    assert_eq!(result, expected);
}

#[test]
fn empty_nested() {
    let expected = vec![
        ReadEvent::StartBody,
        ReadEvent::StartBody,
        ReadEvent::EndRecord,
        ReadEvent::StartBody,
        ReadEvent::EndRecord,
        ReadEvent::StartBody,
        ReadEvent::EndRecord,
        ReadEvent::EndRecord,
    ];

    let result = run_parser_iterator("{{},{},{}}").unwrap();
    assert_eq!(result, expected);
}

#[test]
fn simple_nested() {
    let expected = vec![
        ReadEvent::StartBody,
        ReadEvent::StartBody,
        uint_event(4),
        string_event("slot"),
        ReadEvent::Slot,
        string_event("word"),
        ReadEvent::EndRecord,
        uint_event(1),
        ReadEvent::EndRecord,
    ];

    let result = run_parser_iterator(
        r#"{
            { 4, slot: word }
            1
        }"#,
    )
    .unwrap();
    assert_eq!(result, expected);
}

#[test]
fn nested_with_attr() {
    let expected = vec![
        ReadEvent::StartBody,
        attr_event("inner"),
        ReadEvent::EndAttribute,
        ReadEvent::StartBody,
        ReadEvent::EndRecord,
        ReadEvent::EndRecord,
    ];

    let result = run_parser_iterator("{ @inner }").unwrap();
    assert_eq!(result, expected);

    let result = run_parser_iterator("{ @inner {} }").unwrap();
    assert_eq!(result, expected);
}

#[test]
fn nested_with_attr_with_body() {
    let expected = vec![
        ReadEvent::StartBody,
        attr_event("inner"),
        uint_event(0),
        ReadEvent::EndAttribute,
        ReadEvent::StartBody,
        ReadEvent::EndRecord,
        ReadEvent::EndRecord,
    ];

    let result = run_parser_iterator("{ @inner(0) }").unwrap();
    assert_eq!(result, expected);

    let result = run_parser_iterator("{ @inner(0) {} }").unwrap();
    assert_eq!(result, expected);
}

#[test]
fn nested_with_attr_with_body_followed() {
    let expected = vec![
        ReadEvent::StartBody,
        attr_event("inner"),
        uint_event(0),
        ReadEvent::EndAttribute,
        ReadEvent::StartBody,
        ReadEvent::EndRecord,
        string_event("after"),
        ReadEvent::EndRecord,
    ];

    let result = run_parser_iterator("{ @inner(0), after }").unwrap();
    assert_eq!(result, expected);

    let result = run_parser_iterator("{ @inner(0) {}, after }").unwrap();
    assert_eq!(result, expected);
}

#[test]
fn empty_nested_in_attr() {
    let expected = vec![
        attr_event("outer"),
        ReadEvent::StartBody,
        ReadEvent::EndRecord,
        ReadEvent::EndAttribute,
        ReadEvent::StartBody,
        ReadEvent::EndRecord,
    ];

    let result = run_parser_iterator("@outer({})").unwrap();
    assert_eq!(result, expected);
}

#[test]
fn simple_nested_in_attr() {
    let expected = vec![
        attr_event("outer"),
        ReadEvent::StartBody,
        uint_event(4),
        string_event("slot"),
        ReadEvent::Slot,
        string_event("word"),
        ReadEvent::EndRecord,
        ReadEvent::EndAttribute,
        ReadEvent::StartBody,
        ReadEvent::EndRecord,
    ];

    let result = run_parser_iterator("@outer({ 4, slot: word })").unwrap();
    assert_eq!(result, expected);
}

#[test]
fn nested_with_attr_in_attr() {
    let expected = vec![
        attr_event("outer"),
        attr_event("inner"),
        ReadEvent::EndAttribute,
        ReadEvent::StartBody,
        ReadEvent::EndRecord,
        ReadEvent::EndAttribute,
        ReadEvent::StartBody,
        ReadEvent::EndRecord,
    ];

    let result = run_parser_iterator("@outer(@inner)").unwrap();
    assert_eq!(result, expected);

    let result = run_parser_iterator("@outer(@inner {})").unwrap();
    assert_eq!(result, expected);
}

#[test]
fn nested_with_attr_with_body_in_attr() {
    let expected = vec![
        attr_event("outer"),
        attr_event("inner"),
        uint_event(0),
        ReadEvent::EndAttribute,
        ReadEvent::StartBody,
        ReadEvent::EndRecord,
        ReadEvent::EndAttribute,
        ReadEvent::StartBody,
        ReadEvent::EndRecord,
    ];

    let result = run_parser_iterator("@outer(@inner(0))").unwrap();
    assert_eq!(result, expected);

    let result = run_parser_iterator("@outer(@inner(0) {})").unwrap();
    assert_eq!(result, expected);
}

#[test]
fn nested_with_attr_with_body_followed_in_attr() {
    let expected = vec![
        attr_event("outer"),
        attr_event("inner"),
        uint_event(0),
        ReadEvent::EndAttribute,
        ReadEvent::StartBody,
        ReadEvent::EndRecord,
        uint_event(3),
        ReadEvent::EndAttribute,
        ReadEvent::StartBody,
        ReadEvent::EndRecord,
    ];

    let result = run_parser_iterator("@outer(@inner(0), 3)").unwrap();
    assert_eq!(result, expected);

    let result = run_parser_iterator("@outer(@inner(0) {}, 3)").unwrap();
    assert_eq!(result, expected);
}

#[test]
fn double_nested() {
    let expected = vec![
        ReadEvent::StartBody,
        uint_event(1),
        ReadEvent::StartBody,
        uint_event(2),
        ReadEvent::StartBody,
        uint_event(3),
        uint_event(4),
        ReadEvent::EndRecord,
        ReadEvent::EndRecord,
        uint_event(5),
        ReadEvent::EndRecord,
    ];

    let result = run_parser_iterator("{1, {2, {3, 4}}, 5}").unwrap();
    assert_eq!(result, expected);
}

#[test]
fn complex_slot() {
    let expected = vec![
        ReadEvent::StartBody,
        attr_event("key"),
        ReadEvent::EndAttribute,
        ReadEvent::StartBody,
        uint_event(1),
        ReadEvent::EndRecord,
        ReadEvent::Slot,
        attr_event("value"),
        ReadEvent::EndAttribute,
        ReadEvent::StartBody,
        uint_event(2),
        ReadEvent::EndRecord,
        ReadEvent::EndRecord,
    ];

    let result = run_parser_iterator("{@key {1}: @value {2}}").unwrap();
    assert_eq!(result, expected);
}

fn value_from_string(rep: &str) -> Result<Value, ParseError> {
    let span = Span::new(rep);
    super::parse_recognize(span)
}

#[test]
fn primitive_values_from_string() {
    assert_eq!(value_from_string("").unwrap(), Value::Extant);
    assert!(matches!(
        value_from_string("5").unwrap(),
        Value::Int32Value(5)
    ));
    assert!(matches!(
        value_from_string("4000000000").unwrap(),
        Value::Int64Value(4000000000i64)
    ));
    let n = u64::MAX - 1;
    let n_str = n.to_string();
    assert!(matches!(value_from_string(n_str.as_str()).unwrap(), Value::UInt64Value(m) if m == n));
    assert_eq!(
        value_from_string("true").unwrap(),
        Value::BooleanValue(true)
    );
    assert_eq!(
        value_from_string("false").unwrap(),
        Value::BooleanValue(false)
    );
    assert_eq!(
        value_from_string("name").unwrap(),
        Value::Text(Text::new("name"))
    );
    assert_eq!(
        value_from_string(r#""two words""#).unwrap(),
        Value::Text(Text::new("two words"))
    );
    assert_eq!(
        value_from_string(r#""true""#).unwrap(),
        Value::Text(Text::new("true"))
    );
    assert_eq!(
        value_from_string(r#""false""#).unwrap(),
        Value::Text(Text::new("false"))
    );
    if let Ok(Value::Data(blob)) = value_from_string("%YW55IGNhcm5hbCBwbGVhc3VyZQ==") {
        assert_eq!(blob.as_ref(), "any carnal pleasure".as_bytes());
    } else {
        panic!("Incorrect blob.")
    }
    assert_eq!(value_from_string("0.5").unwrap(), Value::Float64Value(0.5));
}

#[test]
fn simple_record_from_string() {
    assert_eq!(
        value_from_string("{1, 2, 3}").unwrap(),
        Value::from_vec(vec![1, 2, 3])
    );
    assert_eq!(
        value_from_string("{a: 1, b: 2, c: 3}").unwrap(),
        Value::from_vec(vec![("a", 1), ("b", 2), ("c", 3)])
    );
}

#[test]
fn record_with_attrs_from_string() {
    assert_eq!(
        value_from_string("@first").unwrap(),
        Value::of_attr("first")
    );
    assert_eq!(
        value_from_string("@first(1)").unwrap(),
        Value::of_attr(("first", 1))
    );
    assert_eq!(
        value_from_string("@\"two words\"(1)").unwrap(),
        Value::of_attr(("two words", 1))
    );
    assert_eq!(
        value_from_string("@first({})").unwrap(),
        Value::of_attr(("first", Value::empty_record()))
    );
    assert_eq!(
        value_from_string("@first@second").unwrap(),
        Value::of_attrs(vec![Attr::of("first"), Attr::of("second")])
    );
    assert_eq!(
        value_from_string("@first(a:1)").unwrap(),
        Value::of_attr(("first", Value::from_vec(vec![("a", 1)])))
    );
    assert_eq!(
        value_from_string("@first(1, 2, 3)").unwrap(),
        Value::of_attr(("first", Value::from_vec(vec![1, 2, 3])))
    );
}

#[test]
fn nested_record_from_string() {
    let string = "{ {1, {2, 3} }, name: {4} }";
    let value = value_from_string(string).unwrap();
    assert_eq!(
        value,
        Value::record(vec![
            Item::ValueItem(Value::record(vec![
                Item::of(1),
                Item::of(Value::from_vec(vec![2, 3])),
            ])),
            Item::slot("name", Value::from_vec(vec![4])),
        ])
    );
}

#[test]
fn complex_slot_from_string() {
    let string = "{@key {1}: @value {2}}";
    let key = Value::Record(vec![Attr::of("key")], vec![Item::of(1)]);
    let value = Value::Record(vec![Attr::of("value")], vec![Item::of(2)]);
    let expected = Value::from_vec(vec![(key, value)]);

    let value = value_from_string(string).unwrap();
    assert_eq!(value, expected);
}
