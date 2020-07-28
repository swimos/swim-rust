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

use hamcrest2::assert_that;
use hamcrest2::prelude::*;

use super::*;
use num_bigint::RandBigInt;
use num_traits::Signed;
use std::ops::Neg;

mod documents;

fn read_single_token(repr: &str) -> Result<ReconToken<&str>, Option<BadToken>> {
    match tokenize_str(repr).next() {
        Some(Ok(LocatedReconToken(token, _))) => Ok(token),
        Some(Err(failed)) => Err(Some(failed)),
        _ => Err(None),
    }
}

type ReadSingleToken = fn(&str) -> Result<ReconToken<String>, Option<BadToken>>;
type ReadSingleValue = fn(&str) -> Result<Value, ParseFailure>;

fn read_single_token_owned(repr: &str) -> Result<ReconToken<String>, Option<BadToken>> {
    read_single_token(repr).map(|t| t.owned_token())
}

fn consume_to_single_token(repr: &str) -> Result<ReconToken<String>, Option<BadToken>> {
    match tokenize_iteratee()
        .transduce_into(repr.char_indices())
        .next()
    {
        Some(Ok(LocatedReconToken(token, _))) => Ok(token),
        Some(Err(failed)) => Err(Some(failed)),
        _ => Err(None),
    }
}

fn consume_to_value(repr: &str) -> Result<Value, ParseFailure> {
    match parse_iteratee().transduce_into(repr.char_indices()).next() {
        Some(result) => result,
        _ => Err(ParseFailure::IncompleteRecord),
    }
}

#[test]
fn unescape_strings() {
    assert_that!(unescape("no escapes").unwrap(), eq("no escapes"));
    assert_that!(unescape(r"\r\n\t").unwrap(), eq("\r\n\t"));
    assert_that!(
        unescape(r"A string with \n two lines.").unwrap(),
        eq("A string with \n two lines.")
    );
    assert_that!(
        unescape(r#"A string with a \"quoted\" section."#).unwrap(),
        eq("A string with a \"quoted\" section.")
    );
    assert_that!(
        unescape(r"Escaped escapes \\r\\t.").unwrap(),
        eq(r"Escaped escapes \r\t.")
    );
    assert_that!(unescape(r"\u0015").unwrap(), eq("\u{15}"));
}

#[test]
fn symbol_tokens() {
    assert_that!(read_single_token("@").unwrap(), eq(ReconToken::AttrMarker));
    assert_that!(read_single_token(":").unwrap(), eq(ReconToken::SlotDivider));
    assert_that!(read_single_token(",").unwrap(), eq(ReconToken::EntrySep));
    assert_that!(read_single_token(";").unwrap(), eq(ReconToken::EntrySep));
    assert_that!(
        read_single_token("(").unwrap(),
        eq(ReconToken::AttrBodyStart)
    );
    assert_that!(read_single_token(")").unwrap(), eq(ReconToken::AttrBodyEnd));
    assert_that!(
        read_single_token("{").unwrap(),
        eq(ReconToken::RecordBodyStart)
    );
    assert_that!(
        read_single_token("}").unwrap(),
        eq(ReconToken::RecordBodyEnd)
    );
}

#[test]
fn identifiers() {
    assert_that!(is_identifier("name"), is(true));
    assert_that!(is_identifier("اسم"), is(true));
    assert_that!(is_identifier("name2"), is(true));
    assert_that!(is_identifier("first_second"), is(true));
    assert_that!(is_identifier("_name"), is(true));

    assert_that!(is_identifier(""), is(false));
    assert_that!(is_identifier("2name"), is(false));
    assert_that!(is_identifier("true"), is(false));
    assert_that!(is_identifier("false"), is(false));
    assert_that!(is_identifier("two words"), is(false));
    assert_that!(is_identifier("£%^$&*"), is(false));
    assert_that!(is_identifier("\r\n\t"), is(false));
    assert_that!(is_identifier("\"\\\""), is(false));
}

#[test]
fn parse_integer_tokens() {
    integer_tokens(read_single_token_owned);
}

#[test]
fn iteratee_integer_tokens() {
    integer_tokens(consume_to_single_token);
}

fn integer_tokens(read_single: ReadSingleToken) {
    assert_that!(
        read_single("-2147483648").unwrap(),
        eq(ReconToken::Int32Literal(-2147483648))
    );
    assert_that!(
        read_single("-2147483649").unwrap(),
        eq(ReconToken::Int64Literal(-2147483649))
    );
    assert_that!(
        read_single("-9223372036854775808").unwrap(),
        eq(ReconToken::Int64Literal(-9223372036854775808))
    );
    assert_that!(
        read_single("4294967295").unwrap(),
        eq(ReconToken::UInt32Literal(4294967295))
    );
    assert_that!(
        read_single("4294967296").unwrap(),
        eq(ReconToken::UInt64Literal(4294967296))
    );
    assert_that!(
        read_single("18446744073709551615").unwrap(),
        eq(ReconToken::UInt64Literal(18446744073709551615))
    );

    assert_that!(read_single("0").unwrap(), eq(ReconToken::UInt32Literal(0)));
    assert_that!(read_single("1").unwrap(), eq(ReconToken::UInt32Literal(1)));
    assert_that!(
        read_single("42").unwrap(),
        eq(ReconToken::UInt32Literal(42))
    );
    assert_that!(
        read_single("1076").unwrap(),
        eq(ReconToken::UInt32Literal(1076))
    );
    assert_that!(read_single("-1").unwrap(), eq(ReconToken::Int32Literal(-1)));
    assert_that!(
        read_single("-04").unwrap(),
        eq(ReconToken::Int32Literal(-4))
    );

    let big_n = i64::from(std::i32::MAX) * 2i64;
    let big = big_n.to_string();

    assert_that!(
        read_single(big.borrow()).unwrap(),
        eq(ReconToken::UInt32Literal(big_n as u32))
    );

    let big_n_neg = -big_n;
    let big_neg = big_n_neg.to_string();

    assert_that!(
        read_single(big_neg.borrow()).unwrap(),
        eq(ReconToken::Int64Literal(big_n_neg))
    );
}

#[test]
fn parse_bool_tokens() {
    bool_tokens(read_single_token_owned);
}

#[test]
fn iteratee_bool_tokens() {
    bool_tokens(consume_to_single_token);
}

fn bool_tokens(read_single: ReadSingleToken) {
    assert_that!(
        read_single("true").unwrap(),
        eq(ReconToken::BoolLiteral(true))
    );
    assert_that!(
        read_single("false").unwrap(),
        eq(ReconToken::BoolLiteral(false))
    );
}

#[test]
fn parse_identifier_tokens() {
    identifier_tokens(read_single_token_owned);
}

#[test]
fn iteratee_identifier_tokens() {
    identifier_tokens(consume_to_single_token);
}

fn identifier_tokens(read_single: ReadSingleToken) {
    assert_that!(
        read_single("name").unwrap(),
        eq(ReconToken::Identifier("name").owned_token())
    );
    assert_that!(
        read_single("اسم").unwrap(),
        eq(ReconToken::Identifier("اسم").owned_token())
    );
    assert_that!(
        read_single("name2").unwrap(),
        eq(ReconToken::Identifier("name2").owned_token())
    );
    assert_that!(
        read_single("_name").unwrap(),
        eq(ReconToken::Identifier("_name").owned_token())
    );
    assert_that!(
        read_single("first_second").unwrap(),
        eq(ReconToken::Identifier("first_second").owned_token())
    );
}

#[test]
fn parse_string_literal_tokens() {
    string_literal_tokens(read_single_token_owned);
}

#[test]
fn iteratee_string_literal_tokens() {
    string_literal_tokens(consume_to_single_token);
}

fn string_literal_tokens(read_single: ReadSingleToken) {
    assert_that!(
        read_single(r#""name""#).unwrap(),
        eq(ReconToken::StringLiteral("name").owned_token())
    );
    assert_that!(
        read_single(r#""اسم""#).unwrap(),
        eq(ReconToken::StringLiteral("اسم").owned_token())
    );
    assert_that!(
        read_single(r#""two words""#).unwrap(),
        eq(ReconToken::StringLiteral("two words").owned_token())
    );
    assert_that!(
        read_single(r#""2name""#).unwrap(),
        eq(ReconToken::StringLiteral("2name").owned_token())
    );
    assert_that!(
        read_single(r#""true""#).unwrap(),
        eq(ReconToken::StringLiteral("true").owned_token())
    );
    assert_that!(
        read_single(r#""false""#).unwrap(),
        eq(ReconToken::StringLiteral("false").owned_token())
    );
    assert_that!(
        read_single(r#""£%^$&*""#).unwrap(),
        eq(ReconToken::StringLiteral("£%^$&*").owned_token())
    );
    assert_that!(
        read_single("\"\r\n\t\"").unwrap(),
        eq(ReconToken::StringLiteral("\r\n\t").owned_token())
    );
    assert_that!(
        read_single(r#""\r\n\t""#).unwrap(),
        eq(ReconToken::StringLiteral(r"\r\n\t").owned_token())
    );
    assert_that!(
        read_single(r#""a \"quote\" z""#).unwrap(),
        eq(ReconToken::StringLiteral(r#"a \"quote\" z"#).owned_token())
    );
    assert_that!(
        read_single(r#""a \\ z""#).unwrap(),
        eq(ReconToken::StringLiteral(r#"a \\ z"#).owned_token())
    );
}

#[test]
fn parse_floating_point_tokens() {
    floating_point_tokens(read_single_token_owned);
}

#[test]
fn iteratee_floating_point_tokens() {
    floating_point_tokens(consume_to_single_token);
}

fn floating_point_tokens(read_single: ReadSingleToken) {
    assert_that!(
        read_single("0.0").unwrap(),
        eq(ReconToken::Float64Literal(0.0))
    );
    assert_that!(
        read_single(".0").unwrap(),
        eq(ReconToken::Float64Literal(0.0))
    );
    assert_that!(
        read_single("3.5").unwrap(),
        eq(ReconToken::Float64Literal(3.5))
    );
    assert_that!(
        read_single("-1.0").unwrap(),
        eq(ReconToken::Float64Literal(-1.0))
    );
    assert_that!(
        read_single("3e2").unwrap(),
        eq(ReconToken::Float64Literal(3e2))
    );
    assert_that!(
        read_single("50.06e8").unwrap(),
        eq(ReconToken::Float64Literal(50.06e8))
    );
    assert_that!(
        read_single(".2e0").unwrap(),
        eq(ReconToken::Float64Literal(0.2e0))
    );
    assert_that!(
        read_single("3E2").unwrap(),
        eq(ReconToken::Float64Literal(3e2))
    );
    assert_that!(
        read_single("50.06E8").unwrap(),
        eq(ReconToken::Float64Literal(50.06e8))
    );
    assert_that!(
        read_single(".2E0").unwrap(),
        eq(ReconToken::Float64Literal(0.2e0))
    );
    assert_that!(
        read_single("3e-9").unwrap(),
        eq(ReconToken::Float64Literal(3e-9))
    );
    assert_that!(
        read_single("3E-9").unwrap(),
        eq(ReconToken::Float64Literal(3e-9))
    );
    assert_that!(
        read_single("-.76e-12").unwrap(),
        eq(ReconToken::Float64Literal(-0.76e-12))
    );
}

#[test]
fn token_sequence() {
    let source = "@name(2){ x : -3, \"y\": true \n 7 } ";
    let tokens = tokenize_str(source)
        .map(|r| r.unwrap().0)
        .collect::<Vec<_>>();

    assert_that!(
        tokens,
        eq(vec![
            ReconToken::AttrMarker,
            ReconToken::Identifier("name"),
            ReconToken::AttrBodyStart,
            ReconToken::UInt32Literal(2),
            ReconToken::AttrBodyEnd,
            ReconToken::RecordBodyStart,
            ReconToken::Identifier("x"),
            ReconToken::SlotDivider,
            ReconToken::Int32Literal(-3),
            ReconToken::EntrySep,
            ReconToken::StringLiteral("y"),
            ReconToken::SlotDivider,
            ReconToken::BoolLiteral(true),
            ReconToken::NewLine,
            ReconToken::UInt32Literal(7),
            ReconToken::RecordBodyEnd,
        ])
    );
}

#[test]
fn iteratee_token_sequence() {
    let source = "@name(2){ x : -3, \"y\": true \n 7 } ";
    let tokens = tokenize_iteratee()
        .fuse_on_error()
        .transduce_into(source.char_indices())
        .map(|r| r.unwrap().0)
        .collect::<Vec<_>>();

    assert_that!(
        tokens,
        eq(vec![
            ReconToken::AttrMarker,
            ReconToken::Identifier("name".to_owned()),
            ReconToken::AttrBodyStart,
            ReconToken::UInt32Literal(2),
            ReconToken::AttrBodyEnd,
            ReconToken::RecordBodyStart,
            ReconToken::Identifier("x".to_owned()),
            ReconToken::SlotDivider,
            ReconToken::Int32Literal(-3),
            ReconToken::EntrySep,
            ReconToken::StringLiteral("y".to_owned()),
            ReconToken::SlotDivider,
            ReconToken::BoolLiteral(true),
            ReconToken::NewLine,
            ReconToken::UInt32Literal(7),
            ReconToken::RecordBodyEnd,
        ])
    );
}

#[test]
fn parse_simple_values() {
    simple_values(parse_single);
}

#[test]
fn iteratee_simple_values() {
    simple_values(consume_to_value);
}

fn simple_values(read_single: ReadSingleValue) {
    assert_that!(read_single("1").unwrap(), eq(Value::UInt32Value(1)));
    assert_that!(read_single("123").unwrap(), eq(Value::UInt32Value(123)));
    assert_that!(read_single("-77").unwrap(), eq(Value::Int32Value(-77)));

    assert_that!(read_single("name").unwrap(), eq(Value::text("name")));
    assert_that!(read_single("اسم").unwrap(), eq(Value::text("اسم")));

    assert_that!(read_single(r#""name""#).unwrap(), eq(Value::text("name")));
    assert_that!(
        read_single(r#""two words""#).unwrap(),
        eq(Value::text("two words"))
    );
    assert_that!(
        read_single(r#""two \n lines""#).unwrap(),
        eq(Value::text("two \n lines"))
    );
    assert_that!(
        read_single(r#""\"quoted\"""#).unwrap(),
        eq(Value::text(r#""quoted""#))
    );

    assert_that!(read_single("true").unwrap(), eq(Value::BooleanValue(true)));
    assert_that!(
        read_single("false").unwrap(),
        eq(Value::BooleanValue(false))
    );

    assert_that!(read_single("1.25").unwrap(), eq(Value::Float64Value(1.25)));
    assert_that!(
        read_single("-1.25e-7").unwrap(),
        eq(Value::Float64Value(-1.25e-7))
    );
}

#[test]
fn parse_simple_attributes() {
    simple_attributes(parse_single);
}

#[test]
fn iteratee_simple_attributes() {
    simple_attributes(consume_to_value);
}

fn simple_attributes(read_single: ReadSingleValue) {
    assert_that!(
        read_single("@name").unwrap(),
        eq(Value::of_attr(Attr::of("name")))
    );
    assert_that!(
        read_single("@name1@name2").unwrap(),
        eq(Value::of_attrs(vec![Attr::of("name1"), Attr::of("name2")]))
    );
    assert_that!(
        read_single("@name(1)").unwrap(),
        eq(Value::of_attr(Attr::of(("name", 1u32))))
    );
    assert_that!(
        read_single(r#"@"two words""#).unwrap(),
        eq(Value::of_attr(Attr::of("two words")))
    );
    assert_that!(
        read_single(r#"@"@name""#).unwrap(),
        eq(Value::of_attr(Attr::of("@name")))
    );
}

#[test]
fn parse_simple_records() {
    simple_records(parse_single);
}

#[test]
fn iteratee_simple_records() {
    simple_records(consume_to_value);
}

fn simple_records(read_single: ReadSingleValue) {
    assert_that!(read_single("{}").unwrap(), eq(Value::empty_record()));
    assert_that!(read_single("{1}").unwrap(), eq(Value::singleton(1u32)));
    assert_that!(
        read_single("{a:1}").unwrap(),
        eq(Value::singleton(("a", 1u32)))
    );
    assert_that!(
        read_single("{1,2,3}").unwrap(),
        eq(Value::from_vec(vec![1u32, 2u32, 3u32]))
    );
    assert_that!(
        read_single("{1;2;3}").unwrap(),
        eq(Value::from_vec(vec![1u32, 2u32, 3u32]))
    );
    assert_that!(
        read_single("{1\n2\n3}").unwrap(),
        eq(Value::from_vec(vec![1u32, 2u32, 3u32]))
    );
    assert_that!(
        read_single("{a: 1, b: 2, c: 3}").unwrap(),
        eq(Value::from_vec(vec![("a", 1u32), ("b", 2u32), ("c", 3u32)]))
    );
    assert_that!(
        read_single("{a: 1; b: 2; c: 3}").unwrap(),
        eq(Value::from_vec(vec![("a", 1u32), ("b", 2u32), ("c", 3u32)]))
    );
    assert_that!(
        read_single("{a: 1\n\n b: 2\r\n c: 3}").unwrap(),
        eq(Value::from_vec(vec![("a", 1u32), ("b", 2u32), ("c", 3u32)]))
    );
    assert_that!(
        read_single(r#"{first: 1, 2: second, "3": 3}"#).unwrap(),
        eq(Value::Record(
            vec![],
            vec![
                Item::slot("first", 1u32),
                Item::slot(2u32, "second"),
                Item::slot("3", 3u32)
            ]
        ))
    );
    assert_that!(
        read_single("{a:}").unwrap(),
        eq(Value::singleton(("a", Value::Extant)))
    );
    assert_that!(
        read_single("{:1}").unwrap(),
        eq(Value::singleton((Value::Extant, 1u32)))
    );
    assert_that!(
        read_single("{:}").unwrap(),
        eq(Value::singleton((Value::Extant, Value::Extant)))
    );
    assert_that!(
        read_single("{a:1,:2,3:,:,}").unwrap(),
        eq(Value::Record(
            vec![],
            vec![
                Item::slot("a", 1u32),
                Item::slot(Value::Extant, 2u32),
                Item::slot(3u32, Value::Extant),
                Item::slot(Value::Extant, Value::Extant),
                Item::of(Value::Extant),
            ]
        ))
    );
    assert_that!(
        read_single("{,}").unwrap(),
        eq(Value::from_vec(vec![Value::Extant, Value::Extant]))
    );
    assert_that!(
        read_single("{1,,,2}").unwrap(),
        eq(Value::from_vec(vec![
            Item::of(1u32),
            Item::of(Value::Extant),
            Item::of(Value::Extant),
            Item::of(2u32)
        ]))
    );
}

#[test]
fn parse_complex_attributes() {
    complex_attributes(parse_single);
}

#[test]
fn iteratee_complex_attributes() {
    complex_attributes(consume_to_value);
}

fn complex_attributes(read_single: ReadSingleValue) {
    assert_that!(
        read_single("@name()").unwrap(),
        eq(Value::of_attr(Attr::of("name")))
    );
    assert_that!(
        read_single("@name({})").unwrap(),
        eq(Value::of_attr(Attr::of(("name", Value::empty_record()))))
    );
    assert_that!(
        read_single("@name(single: -2)").unwrap(),
        eq(Value::of_attr(Attr::of((
            "name",
            Value::singleton(("single", -2))
        ))))
    );
    assert_that!(
        read_single("@name(first: 1, second: 2, third : 3)").unwrap(),
        eq(Value::of_attr(Attr::of((
            "name",
            Value::from_vec(vec![("first", 1u32), ("second", 2u32), ("third", 3u32)])
        ))))
    );
    assert_that!(
        read_single("@name(first: 1; second: 2; third : 3)").unwrap(),
        eq(Value::of_attr(Attr::of((
            "name",
            Value::from_vec(vec![("first", 1u32), ("second", 2u32), ("third", 3u32)])
        ))))
    );
    assert_that!(
        read_single("@name(first: 1\n second: 2\n third : 3)").unwrap(),
        eq(Value::of_attr(Attr::of((
            "name",
            Value::from_vec(vec![("first", 1u32), ("second", 2u32), ("third", 3u32)])
        ))))
    );
    assert_that!(
        read_single("@name(,)").unwrap(),
        eq(Value::of_attr(Attr::of((
            "name",
            Value::from_vec(vec![Value::Extant, Value::Extant])
        ))))
    );
}

#[test]
fn parse_nested_records() {
    nested_records(parse_single);
}

#[test]
fn iteratee_nested_records() {
    nested_records(consume_to_value);
}

#[allow(clippy::cognitive_complexity)]
fn nested_records(read_single: ReadSingleValue) {
    assert_that!(
        read_single("{{}}").unwrap(),
        eq(Value::singleton(Value::empty_record()))
    );
    assert_that!(
        read_single("{{{}}}").unwrap(),
        eq(Value::singleton(Value::singleton(Value::empty_record())))
    );
    assert_that!(
        read_single("{@name}").unwrap(),
        eq(Value::singleton(Value::of_attr("name")))
    );
    assert_that!(
        read_single("{@name(1)}").unwrap(),
        eq(Value::singleton(Value::of_attr(("name", 1u32))))
    );

    assert_that!(
        read_single("{0, {}}").unwrap(),
        eq(Value::from_vec(vec![
            Item::of(0u32),
            Item::of(Value::empty_record())
        ]))
    );
    assert_that!(
        read_single("{0, @name}").unwrap(),
        eq(Value::from_vec(vec![
            Item::of(0u32),
            Item::of(Value::of_attr("name"))
        ]))
    );
    assert_that!(
        read_single("{0, @name(1)}").unwrap(),
        eq(Value::from_vec(vec![
            Item::of(0u32),
            Item::of(Value::of_attr(("name", 1u32)))
        ]))
    );

    assert_that!(
        read_single("{a: {b:1}}").unwrap(),
        eq(Value::from_vec(vec![Item::slot(
            "a",
            Value::singleton(("b", 1u32))
        )]))
    );
    assert_that!(
        read_single("{0, a: {b:1}}").unwrap(),
        eq(Value::from_vec(vec![
            Item::of(0u32),
            Item::slot("a", Value::singleton(("b", 1u32)))
        ]))
    );

    assert_that!(
        read_single("{{a:1}: b}").unwrap(),
        eq(Value::from_vec(vec![Item::slot(
            Value::singleton(("a", 1u32)),
            "b"
        )]))
    );
    assert_that!(
        read_single("{0, {a:1}: b}").unwrap(),
        eq(Value::from_vec(vec![
            Item::of(0u32),
            Item::slot(Value::singleton(("a", 1u32)), "b")
        ]))
    );

    assert_that!(
        read_single("{a: {b:1,c:2}}").unwrap(),
        eq(Value::from_vec(vec![Item::slot(
            "a",
            Value::from_vec(vec![("b", 1u32), ("c", 2u32)])
        )]))
    );
    assert_that!(
        read_single("{0, a: {b:1,c:2}}").unwrap(),
        eq(Value::from_vec(vec![
            Item::of(0u32),
            Item::slot("a", Value::from_vec(vec![("b", 1u32), ("c", 2u32)]))
        ]))
    );

    assert_that!(
        read_single("@name({{}})").unwrap(),
        eq(Value::of_attr((
            "name",
            Value::singleton(Value::empty_record())
        )))
    );
    assert_that!(
        read_single("@name(@inner)").unwrap(),
        eq(Value::of_attr(("name", Value::of_attr("inner"))))
    );
    assert_that!(
        read_single("@name(@inner(1))").unwrap(),
        eq(Value::of_attr(("name", Value::of_attr(("inner", 1u32)))))
    );
    assert_that!(
        read_single("@name(0, {})").unwrap(),
        eq(Value::of_attr((
            "name",
            Value::from_vec(vec![Item::of(0u32), Item::of(Value::empty_record())])
        )))
    );
    assert_that!(
        read_single("@name(0, @inner)").unwrap(),
        eq(Value::of_attr((
            "name",
            Value::from_vec(vec![Item::of(0u32), Item::of(Value::of_attr("inner"))])
        )))
    );
    assert_that!(
        read_single("@name(0, @inner(1))").unwrap(),
        eq(Value::of_attr((
            "name",
            Value::from_vec(vec![
                Item::of(0u32),
                Item::of(Value::of_attr(("inner", 1u32)))
            ])
        )))
    );
    assert_that!(
        read_single("@name(a: {b:1})").unwrap(),
        eq(Value::of_attr((
            "name",
            Value::singleton(("a", Value::singleton(("b", 1u32))))
        )))
    );
    assert_that!(
        read_single("@name(0, a: {b:1})").unwrap(),
        eq(Value::of_attr((
            "name",
            Value::from_vec(vec![
                Item::of(0u32),
                Item::of(("a", Value::singleton(("b", 1u32))))
            ])
        )))
    );
    assert_that!(
        read_single("@name({a:1}: b)").unwrap(),
        eq(Value::of_attr((
            "name",
            Value::singleton((Value::singleton(("a", 1u32)), "b"))
        )))
    );
    assert_that!(
        read_single("@name(0, {a:1}: b)").unwrap(),
        eq(Value::of_attr((
            "name",
            Value::from_vec(vec![
                Item::of(0u32),
                Item::of((Value::singleton(("a", 1u32)), "b"))
            ])
        )))
    );
    assert_that!(
        read_single("@name(a: {b:1,c:2})").unwrap(),
        eq(Value::of_attr((
            "name",
            Value::singleton(("a", Value::from_vec(vec![("b", 1u32), ("c", 2u32)])))
        )))
    );
    assert_that!(
        read_single("@name(0, a: {b:1,c:2})").unwrap(),
        eq(Value::of_attr((
            "name",
            Value::from_vec(vec![
                Item::from(0u32),
                Item::from(("a", Value::from_vec(vec![("b", 1u32), ("c", 2u32)])))
            ])
        )))
    );
}

#[test]
fn parse_tagged_records() {
    tagged_records(parse_single);
}

#[test]
fn iteratee_tagged_records() {
    tagged_records(consume_to_value);
}

fn tagged_records(read_single: ReadSingleValue) {
    assert_that!(read_single("@name {}").unwrap(), eq(Value::of_attr("name")));
    assert_that!(
        read_single("@first@second {}").unwrap(),
        eq(Value::of_attrs(vec![Attr::of("first"), Attr::of("second")]))
    );
    assert_that!(
        read_single("@name id").unwrap(),
        eq(Value::Record(vec![Attr::of("name")], vec![Item::of("id")]))
    );
    assert_that!(
        read_single(r#"@name"@value""#).unwrap(),
        eq(Value::Record(
            vec![Attr::of("name")],
            vec![Item::of("@value")]
        ))
    );
    assert_that!(
        read_single("@name -4").unwrap(),
        eq(Value::Record(vec![Attr::of("name")], vec![Item::of(-4)]))
    );

    assert_that!(
        read_single("@name{id}").unwrap(),
        eq(Value::Record(vec![Attr::of("name")], vec![Item::of("id")]))
    );
    assert_that!(
        read_single(r#"@name { "@value" }"#).unwrap(),
        eq(Value::Record(
            vec![Attr::of("name")],
            vec![Item::of("@value")]
        ))
    );
    assert_that!(
        read_single("@name{ -4}").unwrap(),
        eq(Value::Record(vec![Attr::of("name")], vec![Item::of(-4)]))
    );
    assert_that!(
        read_single("@name {1, 2, 3}").unwrap(),
        eq(Value::Record(
            vec![Attr::of("name")],
            vec![Item::of(1u32), Item::of(2u32), Item::of(3u32)]
        ))
    );
    assert_that!(
        read_single("@name {a:1,b:2}").unwrap(),
        eq(Value::Record(
            vec![Attr::of("name")],
            vec![Item::of(("a", 1u32)), Item::of(("b", 2u32))]
        ))
    );
    assert_that!(
        read_single("@name {,}").unwrap(),
        eq(Value::Record(
            vec![Attr::of("name")],
            vec![Item::of(Value::Extant), Item::of(Value::Extant)]
        ))
    );
}

#[test]
fn parse_nested_tagged_records() {
    nested_tagged_records(parse_single);
}

#[test]
fn iteratee_nested_tagged_records() {
    nested_tagged_records(consume_to_value);
}

fn nested_tagged_records(read_single: ReadSingleValue) {
    assert_that!(
        read_single("@name(@inner1 {}, @inner2 {})").unwrap(),
        eq(Value::of_attr((
            "name",
            Value::from_vec(vec![Value::of_attr("inner1"), Value::of_attr("inner2")])
        )))
    );
    assert_that!(
        read_single("@name(@inner {1, 2})").unwrap(),
        eq(Value::of_attr((
            "name",
            Value::Record(
                vec![Attr::of("inner")],
                vec![Item::of(1u32), Item::of(2u32)]
            )
        )))
    );
    assert_that!(
        read_single("@name {@inner1 {}, @inner2 {} }").unwrap(),
        eq(Value::Record(
            vec![Attr::of("name")],
            vec![
                Item::of(Value::of_attr("inner1")),
                Item::of(Value::of_attr("inner2"))
            ]
        ))
    );
    assert_that!(
        read_single("@name {@inner {1, 2}}").unwrap(),
        eq(Value::Record(
            vec![Attr::of("name")],
            vec![Item::of(Value::Record(
                vec![Attr::of("inner")],
                vec![Item::of(1u32), Item::of(2u32)]
            ))]
        ))
    );
    assert_that!(
        read_single("@name {0, @inner {1, 2}}").unwrap(),
        eq(Value::Record(
            vec![Attr::of("name")],
            vec![
                Item::of(0u32),
                Item::of(Value::Record(
                    vec![Attr::of("inner")],
                    vec![Item::of(1u32), Item::of(2u32)]
                ))
            ]
        ))
    );
    assert_that!(
        read_single("@name {first: {1, 2}, {second}: 3}").unwrap(),
        eq(Value::Record(
            vec![Attr::of("name")],
            vec![
                Item::slot("first", Value::from_vec(vec![1u32, 2u32])),
                Item::slot(Value::singleton("second"), 3u32)
            ]
        ))
    );
}

#[test]
fn bigint_tests() {
    let mut rng = rand::thread_rng();
    let big_str = {
        let mut bi = rng.gen_bigint(1000);
        if bi.is_positive() {
            bi = bi.neg();
        }
        bi.to_string()
    };

    assert_that!(
        parse_single(&format!("@name({})", big_str)).unwrap(),
        eq(Value::of_attr(Attr::of((
            "name",
            BigInt::from_str(&big_str).unwrap()
        ))))
    );

    let u64_max = u64::max_value();
    let u64_max_str = u64_max.to_string();

    assert_that!(
        parse_single(&format!("@name({})", u64_max_str)).unwrap(),
        eq(Value::of_attr(Attr::of((
            "name",
            Value::UInt64Value(u64_max)
        ))))
    );
}

#[test]
fn biguint_tests() {
    let mut rng = rand::thread_rng();
    let big_str = rng.gen_biguint(1000).to_string();

    assert_that!(
        parse_single(&format!("@name({})", big_str)).unwrap(),
        eq(Value::of_attr(Attr::of((
            "name",
            BigUint::from_str(&big_str).unwrap()
        ))))
    );

    let u64_oob_str = (u64::max_value() as i128 + 1).to_string();

    assert_that!(
        parse_single(&format!("@name({})", u64_oob_str)).unwrap(),
        eq(Value::of_attr(Attr::of((
            "name",
            Value::BigUint(BigUint::from_str(&u64_oob_str).unwrap())
        ))))
    );
}
