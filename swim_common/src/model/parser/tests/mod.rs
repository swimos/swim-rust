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

use super::*;
use num_bigint::RandBigInt;
use num_traits::Signed;
use std::ops::Neg;

mod documents;

fn read_single_token(repr: &str) -> Result<ReconToken<&str>, Option<BadToken>> {
    match tokenize_str(repr, false).next() {
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
    match tokenize_iteratee(false)
        .transduce_into(repr.char_indices())
        .next()
    {
        Some(Ok(LocatedReconToken(token, _))) => Ok(token),
        Some(Err(failed)) => Err(Some(failed)),
        _ => Err(None),
    }
}

fn consume_to_value(repr: &str) -> Result<Value, ParseFailure> {
    match parse_iteratee(false)
        .transduce_into(repr.char_indices())
        .next()
    {
        Some(result) => result,
        _ => Err(ParseFailure::IncompleteRecord),
    }
}

#[test]
fn unescape_strings() {
    assert_eq!(unescape("no escapes").unwrap(), "no escapes");
    assert_eq!(unescape(r"\r\n\t").unwrap(), "\r\n\t");
    assert_eq!(
        unescape(r"A string with \n two lines.").unwrap(),
        "A string with \n two lines."
    );
    assert_eq!(
        unescape(r#"A string with a \"quoted\" section."#).unwrap(),
        "A string with a \"quoted\" section."
    );
    assert_eq!(
        unescape(r"Escaped escapes \\r\\t.").unwrap(),
        r"Escaped escapes \r\t."
    );
    assert_eq!(unescape(r"\u0015").unwrap(), "\u{15}");
}

#[test]
fn symbol_tokens() {
    assert_eq!(read_single_token("@").unwrap(), ReconToken::AttrMarker);
    assert_eq!(read_single_token(":").unwrap(), ReconToken::SlotDivider);
    assert_eq!(read_single_token(",").unwrap(), ReconToken::EntrySep);
    assert_eq!(read_single_token(";").unwrap(), ReconToken::EntrySep);
    assert_eq!(read_single_token("(").unwrap(), ReconToken::AttrBodyStart);
    assert_eq!(read_single_token(")").unwrap(), ReconToken::AttrBodyEnd);
    assert_eq!(read_single_token("{").unwrap(), ReconToken::RecordBodyStart);
    assert_eq!(read_single_token("}").unwrap(), ReconToken::RecordBodyEnd);
}

#[test]
fn identifiers() {
    assert!(is_identifier("name"));
    assert!(is_identifier("اسم"));
    assert!(is_identifier("name2"));
    assert!(is_identifier("first_second"));
    assert!(is_identifier("_name"));

    assert!(!is_identifier(""));
    assert!(!is_identifier("2name"));
    assert!(!is_identifier("true"));
    assert!(!is_identifier("false"));
    assert!(!is_identifier("two words"));
    assert!(!is_identifier("£%^$&*"));
    assert!(!is_identifier("\r\n\t"));
    assert!(!is_identifier("\"\\\""));
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
    assert_eq!(
        read_single("-2147483648").unwrap(),
        ReconToken::Int32Literal(-2147483648)
    );
    assert_eq!(
        read_single("-2147483649").unwrap(),
        ReconToken::Int64Literal(-2147483649)
    );
    assert_eq!(
        read_single("-9223372036854775808").unwrap(),
        ReconToken::Int64Literal(-9223372036854775808)
    );
    assert_eq!(
        read_single("4294967295").unwrap(),
        ReconToken::UInt32Literal(4294967295)
    );
    assert_eq!(
        read_single("4294967296").unwrap(),
        ReconToken::UInt64Literal(4294967296)
    );
    assert_eq!(
        read_single("18446744073709551615").unwrap(),
        ReconToken::UInt64Literal(18446744073709551615)
    );

    assert_eq!(read_single("0").unwrap(), ReconToken::UInt32Literal(0));
    assert_eq!(read_single("1").unwrap(), ReconToken::UInt32Literal(1));
    assert_eq!(read_single("42").unwrap(), ReconToken::UInt32Literal(42));
    assert_eq!(
        read_single("1076").unwrap(),
        ReconToken::UInt32Literal(1076)
    );
    assert_eq!(read_single("-1").unwrap(), ReconToken::Int32Literal(-1));
    assert_eq!(read_single("-04").unwrap(), ReconToken::Int32Literal(-4));

    let big_n = i64::from(std::i32::MAX) * 2i64;
    let big = big_n.to_string();

    assert_eq!(
        read_single(big.borrow()).unwrap(),
        ReconToken::UInt32Literal(big_n as u32)
    );

    let big_n_neg = -big_n;
    let big_neg = big_n_neg.to_string();

    assert_eq!(
        read_single(big_neg.borrow()).unwrap(),
        ReconToken::Int64Literal(big_n_neg)
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
    assert_eq!(read_single("true").unwrap(), ReconToken::BoolLiteral(true));
    assert_eq!(
        read_single("false").unwrap(),
        ReconToken::BoolLiteral(false)
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
    assert_eq!(
        read_single("name").unwrap(),
        ReconToken::Identifier("name").owned_token()
    );
    assert_eq!(
        read_single("اسم").unwrap(),
        ReconToken::Identifier("اسم").owned_token()
    );
    assert_eq!(
        read_single("name2").unwrap(),
        ReconToken::Identifier("name2").owned_token()
    );
    assert_eq!(
        read_single("_name").unwrap(),
        ReconToken::Identifier("_name").owned_token()
    );
    assert_eq!(
        read_single("first_second").unwrap(),
        ReconToken::Identifier("first_second").owned_token()
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
    assert_eq!(
        read_single(r#""name""#).unwrap(),
        ReconToken::StringLiteral("name").owned_token()
    );
    assert_eq!(
        read_single(r#""اسم""#).unwrap(),
        ReconToken::StringLiteral("اسم").owned_token()
    );
    assert_eq!(
        read_single(r#""two words""#).unwrap(),
        ReconToken::StringLiteral("two words").owned_token()
    );
    assert_eq!(
        read_single(r#""2name""#).unwrap(),
        ReconToken::StringLiteral("2name").owned_token()
    );
    assert_eq!(
        read_single(r#""true""#).unwrap(),
        ReconToken::StringLiteral("true").owned_token()
    );
    assert_eq!(
        read_single(r#""false""#).unwrap(),
        ReconToken::StringLiteral("false").owned_token()
    );
    assert_eq!(
        read_single(r#""£%^$&*""#).unwrap(),
        ReconToken::StringLiteral("£%^$&*").owned_token()
    );
    assert_eq!(
        read_single("\"\r\n\t\"").unwrap(),
        ReconToken::StringLiteral("\r\n\t").owned_token()
    );
    assert_eq!(
        read_single(r#""\r\n\t""#).unwrap(),
        ReconToken::StringLiteral(r"\r\n\t").owned_token()
    );
    assert_eq!(
        read_single(r#""a \"quote\" z""#).unwrap(),
        ReconToken::StringLiteral(r#"a \"quote\" z"#).owned_token()
    );
    assert_eq!(
        read_single(r#""a \\ z""#).unwrap(),
        ReconToken::StringLiteral(r#"a \\ z"#).owned_token()
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
    assert_eq!(read_single("0.0").unwrap(), ReconToken::Float64Literal(0.0));
    assert_eq!(read_single(".0").unwrap(), ReconToken::Float64Literal(0.0));
    assert_eq!(read_single("3.5").unwrap(), ReconToken::Float64Literal(3.5));
    assert_eq!(
        read_single("-1.0").unwrap(),
        ReconToken::Float64Literal(-1.0)
    );
    assert_eq!(read_single("3e2").unwrap(), ReconToken::Float64Literal(3e2));
    assert_eq!(
        read_single("50.06e8").unwrap(),
        ReconToken::Float64Literal(50.06e8)
    );
    assert_eq!(
        read_single(".2e0").unwrap(),
        ReconToken::Float64Literal(0.2e0)
    );
    assert_eq!(read_single("3E2").unwrap(), ReconToken::Float64Literal(3e2));
    assert_eq!(
        read_single("50.06E8").unwrap(),
        ReconToken::Float64Literal(50.06e8)
    );
    assert_eq!(
        read_single(".2E0").unwrap(),
        ReconToken::Float64Literal(0.2e0)
    );
    assert_eq!(
        read_single("3e-9").unwrap(),
        ReconToken::Float64Literal(3e-9)
    );
    assert_eq!(
        read_single("3E-9").unwrap(),
        ReconToken::Float64Literal(3e-9)
    );
    assert_eq!(
        read_single("-.76e-12").unwrap(),
        ReconToken::Float64Literal(-0.76e-12)
    );
}

#[test]
fn token_sequence() {
    let source = "@name(2){ x : -3, \"y\": true \n 7 } ";
    let tokens = tokenize_str(source, false)
        .map(|r| r.unwrap().0)
        .collect::<Vec<_>>();

    assert_eq!(
        tokens,
        vec![
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
        ]
    );
}

#[test]
fn iteratee_token_sequence() {
    let source = "@name(2){ x : -3, \"y\": true \n 7 } ";
    let tokens = tokenize_iteratee(false)
        .fuse_on_error()
        .transduce_into(source.char_indices())
        .map(|r| r.unwrap().0)
        .collect::<Vec<_>>();

    assert_eq!(
        tokens,
        vec![
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
        ]
    );
}

#[test]
fn parse_simple_values() {
    simple_values(|repr| parse_single(repr, false));
}

#[test]
fn iteratee_simple_values() {
    simple_values(consume_to_value);
}

fn simple_values(read_single: ReadSingleValue) {
    assert_eq!(read_single("1").unwrap(), Value::UInt32Value(1));
    assert_eq!(read_single("123").unwrap(), Value::UInt32Value(123));
    assert_eq!(read_single("-77").unwrap(), Value::Int32Value(-77));

    assert_eq!(read_single("name").unwrap(), Value::text("name"));
    assert_eq!(read_single("اسم").unwrap(), Value::text("اسم"));

    assert_eq!(read_single(r#""name""#).unwrap(), Value::text("name"));
    assert_eq!(
        read_single(r#""two words""#).unwrap(),
        Value::text("two words")
    );
    assert_eq!(
        read_single(r#""two \n lines""#).unwrap(),
        Value::text("two \n lines")
    );
    assert_eq!(
        read_single(r#""\"quoted\"""#).unwrap(),
        Value::text(r#""quoted""#)
    );

    assert_eq!(read_single("true").unwrap(), Value::BooleanValue(true));
    assert_eq!(read_single("false").unwrap(), Value::BooleanValue(false));

    assert_eq!(read_single("1.25").unwrap(), Value::Float64Value(1.25));
    assert_eq!(
        read_single("-1.25e-7").unwrap(),
        Value::Float64Value(-1.25e-7)
    );
}

#[test]
fn parse_simple_attributes() {
    simple_attributes(|repr| parse_single(repr, false));
}

#[test]
fn iteratee_simple_attributes() {
    simple_attributes(consume_to_value);
}

fn simple_attributes(read_single: ReadSingleValue) {
    assert_eq!(
        read_single("@name").unwrap(),
        Value::of_attr(Attr::of("name"))
    );
    assert_eq!(
        read_single("@name1@name2").unwrap(),
        Value::of_attrs(vec![Attr::of("name1"), Attr::of("name2")])
    );
    assert_eq!(
        read_single("@name(1)").unwrap(),
        Value::of_attr(Attr::of(("name", 1u32)))
    );
    assert_eq!(
        read_single(r#"@"two words""#).unwrap(),
        Value::of_attr(Attr::of("two words"))
    );
    assert_eq!(
        read_single(r#"@"@name""#).unwrap(),
        Value::of_attr(Attr::of("@name"))
    );
}

#[test]
fn parse_simple_records() {
    simple_records(|repr| parse_single(repr, false));
}

#[test]
fn iteratee_simple_records() {
    simple_records(consume_to_value);
}

fn simple_records(read_single: ReadSingleValue) {
    assert_eq!(read_single("{}").unwrap(), Value::empty_record());
    assert_eq!(read_single("{1}").unwrap(), Value::singleton(1u32));
    assert_eq!(read_single("{a:1}").unwrap(), Value::singleton(("a", 1u32)));
    assert_eq!(
        read_single("{1,2,3}").unwrap(),
        Value::from_vec(vec![1u32, 2u32, 3u32])
    );
    assert_eq!(
        read_single("{1;2;3}").unwrap(),
        Value::from_vec(vec![1u32, 2u32, 3u32])
    );
    assert_eq!(
        read_single("{1\n2\n3}").unwrap(),
        Value::from_vec(vec![1u32, 2u32, 3u32])
    );
    assert_eq!(
        read_single("{a: 1, b: 2, c: 3}").unwrap(),
        Value::from_vec(vec![("a", 1u32), ("b", 2u32), ("c", 3u32)])
    );
    assert_eq!(
        read_single("{a: 1; b: 2; c: 3}").unwrap(),
        Value::from_vec(vec![("a", 1u32), ("b", 2u32), ("c", 3u32)])
    );
    assert_eq!(
        read_single("{a: 1\n\n b: 2\r\n c: 3}").unwrap(),
        Value::from_vec(vec![("a", 1u32), ("b", 2u32), ("c", 3u32)])
    );
    assert_eq!(
        read_single(r#"{first: 1, 2: second, "3": 3}"#).unwrap(),
        Value::Record(
            vec![],
            vec![
                Item::slot("first", 1u32),
                Item::slot(2u32, "second"),
                Item::slot("3", 3u32)
            ]
        )
    );
    assert_eq!(
        read_single("{a:}").unwrap(),
        Value::singleton(("a", Value::Extant))
    );
    assert_eq!(
        read_single("{:1}").unwrap(),
        Value::singleton((Value::Extant, 1u32))
    );
    assert_eq!(
        read_single("{:}").unwrap(),
        Value::singleton((Value::Extant, Value::Extant))
    );
    assert_eq!(
        read_single("{a:1,:2,3:,:,}").unwrap(),
        Value::Record(
            vec![],
            vec![
                Item::slot("a", 1u32),
                Item::slot(Value::Extant, 2u32),
                Item::slot(3u32, Value::Extant),
                Item::slot(Value::Extant, Value::Extant),
                Item::of(Value::Extant),
            ]
        )
    );
    assert_eq!(
        read_single("{,}").unwrap(),
        Value::from_vec(vec![Value::Extant, Value::Extant])
    );
    assert_eq!(
        read_single("{1,,,2}").unwrap(),
        Value::from_vec(vec![
            Item::of(1u32),
            Item::of(Value::Extant),
            Item::of(Value::Extant),
            Item::of(2u32)
        ])
    );
}

#[test]
fn parse_complex_attributes() {
    complex_attributes(|repr| parse_single(repr, false));
}

#[test]
fn iteratee_complex_attributes() {
    complex_attributes(consume_to_value);
}

fn complex_attributes(read_single: ReadSingleValue) {
    assert_eq!(
        read_single("@name()").unwrap(),
        Value::of_attr(Attr::of("name"))
    );
    assert_eq!(
        read_single("@name({})").unwrap(),
        Value::of_attr(Attr::of(("name", Value::empty_record())))
    );
    assert_eq!(
        read_single("@name(single: -2)").unwrap(),
        Value::of_attr(Attr::of(("name", Value::singleton(("single", -2)))))
    );
    assert_eq!(
        read_single("@name(first: 1, second: 2, third : 3)").unwrap(),
        Value::of_attr(Attr::of((
            "name",
            Value::from_vec(vec![("first", 1u32), ("second", 2u32), ("third", 3u32)])
        )))
    );
    assert_eq!(
        read_single("@name(first: 1; second: 2; third : 3)").unwrap(),
        Value::of_attr(Attr::of((
            "name",
            Value::from_vec(vec![("first", 1u32), ("second", 2u32), ("third", 3u32)])
        )))
    );
    assert_eq!(
        read_single("@name(first: 1\n second: 2\n third : 3)").unwrap(),
        Value::of_attr(Attr::of((
            "name",
            Value::from_vec(vec![("first", 1u32), ("second", 2u32), ("third", 3u32)])
        )))
    );
    assert_eq!(
        read_single("@name(,)").unwrap(),
        Value::of_attr(Attr::of((
            "name",
            Value::from_vec(vec![Value::Extant, Value::Extant])
        )))
    );

    assert_eq!(
        read_single(
            "@first @second {
                b: 3
            }"
        )
        .unwrap(),
        Value::Record(
            vec![Attr::of("first"), Attr::of("second")],
            vec![Item::slot("b", 3u32)]
        )
    );

    assert_eq!(
        read_single(
            "{
                @first
                @second {
                    b: 3
                }
            }"
        )
        .unwrap(),
        Value::Record(
            vec![],
            vec![
                Item::ValueItem(Value::Record(vec![Attr::of("first")], vec![])),
                Item::ValueItem(Value::Record(
                    vec![Attr::of("second")],
                    vec![Item::slot("b", 3u32)]
                ))
            ]
        )
    );

    assert_eq!(
        read_single(
            "{
                @first {
                    @second {
                        b: 3
                    }
                }
            }"
        )
        .unwrap(),
        Value::Record(
            vec![],
            vec![Item::ValueItem(Value::Record(
                vec![Attr::of("first")],
                vec![Item::ValueItem(Value::Record(
                    vec![Attr::of("second")],
                    vec![Item::slot("b", 3u32)]
                ))]
            ))]
        )
    );

    assert_eq!(
        read_single(
            "{
                a: @first
                @second {
                    b: 3
                }
            }"
        )
        .unwrap(),
        Value::Record(
            vec![],
            vec![
                Item::slot("a", Attr::of("first")),
                Item::ValueItem(Value::Record(
                    vec![Attr::of("second")],
                    vec![Item::slot("b", 3u32)]
                ))
            ]
        )
    );

    assert_eq!(
        read_single(
            "{
                @first
                5
            }"
        )
        .unwrap(),
        Value::Record(
            vec![],
            vec![
                Item::ValueItem(Value::Record(vec![Attr::of("first")], vec![])),
                Item::ValueItem(Value::from(5u32))
            ]
        )
    );
}

#[test]
fn parse_nested_records() {
    nested_records(|repr| parse_single(repr, false));
}

#[test]
fn iteratee_nested_records() {
    nested_records(consume_to_value);
}

fn nested_records(read_single: ReadSingleValue) {
    assert_eq!(
        read_single("{{}}").unwrap(),
        Value::singleton(Value::empty_record())
    );
    assert_eq!(
        read_single("{{{}}}").unwrap(),
        Value::singleton(Value::singleton(Value::empty_record()))
    );
    assert_eq!(
        read_single("{@name}").unwrap(),
        Value::singleton(Value::of_attr("name"))
    );
    assert_eq!(
        read_single("{@name(1)}").unwrap(),
        Value::singleton(Value::of_attr(("name", 1u32)))
    );

    assert_eq!(
        read_single("{0, {}}").unwrap(),
        Value::from_vec(vec![Item::of(0u32), Item::of(Value::empty_record())])
    );
    assert_eq!(
        read_single("{0, @name}").unwrap(),
        Value::from_vec(vec![Item::of(0u32), Item::of(Value::of_attr("name"))])
    );
    assert_eq!(
        read_single("{0, @name(1)}").unwrap(),
        Value::from_vec(vec![
            Item::of(0u32),
            Item::of(Value::of_attr(("name", 1u32)))
        ])
    );

    assert_eq!(
        read_single("{a: {b:1}}").unwrap(),
        Value::from_vec(vec![Item::slot("a", Value::singleton(("b", 1u32)))])
    );
    assert_eq!(
        read_single("{0, a: {b:1}}").unwrap(),
        Value::from_vec(vec![
            Item::of(0u32),
            Item::slot("a", Value::singleton(("b", 1u32)))
        ])
    );

    assert_eq!(
        read_single("{{a:1}: b}").unwrap(),
        Value::from_vec(vec![Item::slot(Value::singleton(("a", 1u32)), "b")])
    );
    assert_eq!(
        read_single("{0, {a:1}: b}").unwrap(),
        Value::from_vec(vec![
            Item::of(0u32),
            Item::slot(Value::singleton(("a", 1u32)), "b")
        ])
    );

    assert_eq!(
        read_single("{a: {b:1,c:2}}").unwrap(),
        Value::from_vec(vec![Item::slot(
            "a",
            Value::from_vec(vec![("b", 1u32), ("c", 2u32)])
        )])
    );
    assert_eq!(
        read_single("{0, a: {b:1,c:2}}").unwrap(),
        Value::from_vec(vec![
            Item::of(0u32),
            Item::slot("a", Value::from_vec(vec![("b", 1u32), ("c", 2u32)]))
        ])
    );

    assert_eq!(
        read_single("@name({{}})").unwrap(),
        Value::of_attr(("name", Value::singleton(Value::empty_record())))
    );
    assert_eq!(
        read_single("@name(@inner)").unwrap(),
        Value::of_attr(("name", Value::of_attr("inner")))
    );
    assert_eq!(
        read_single("@name(@inner(1))").unwrap(),
        Value::of_attr(("name", Value::of_attr(("inner", 1u32))))
    );
    assert_eq!(
        read_single("@name(0, {})").unwrap(),
        Value::of_attr((
            "name",
            Value::from_vec(vec![Item::of(0u32), Item::of(Value::empty_record())])
        ))
    );
    assert_eq!(
        read_single("@name(0, @inner)").unwrap(),
        Value::of_attr((
            "name",
            Value::from_vec(vec![Item::of(0u32), Item::of(Value::of_attr("inner"))])
        ))
    );
    assert_eq!(
        read_single("@name(0, @inner(1))").unwrap(),
        Value::of_attr((
            "name",
            Value::from_vec(vec![
                Item::of(0u32),
                Item::of(Value::of_attr(("inner", 1u32)))
            ])
        ))
    );
    assert_eq!(
        read_single("@name(a: {b:1})").unwrap(),
        Value::of_attr((
            "name",
            Value::singleton(("a", Value::singleton(("b", 1u32))))
        ))
    );
    assert_eq!(
        read_single("@name(0, a: {b:1})").unwrap(),
        Value::of_attr((
            "name",
            Value::from_vec(vec![
                Item::of(0u32),
                Item::of(("a", Value::singleton(("b", 1u32))))
            ])
        ))
    );
    assert_eq!(
        read_single("@name({a:1}: b)").unwrap(),
        Value::of_attr((
            "name",
            Value::singleton((Value::singleton(("a", 1u32)), "b"))
        ))
    );
    assert_eq!(
        read_single("@name(0, {a:1}: b)").unwrap(),
        Value::of_attr((
            "name",
            Value::from_vec(vec![
                Item::of(0u32),
                Item::of((Value::singleton(("a", 1u32)), "b"))
            ])
        ))
    );
    assert_eq!(
        read_single("@name(a: {b:1,c:2})").unwrap(),
        Value::of_attr((
            "name",
            Value::singleton(("a", Value::from_vec(vec![("b", 1u32), ("c", 2u32)])))
        ))
    );
    assert_eq!(
        read_single("@name(0, a: {b:1,c:2})").unwrap(),
        Value::of_attr((
            "name",
            Value::from_vec(vec![
                Item::from(0u32),
                Item::from(("a", Value::from_vec(vec![("b", 1u32), ("c", 2u32)])))
            ])
        ))
    );
}

#[test]
fn parse_attribute_only_records() {
    attribute_only_records(|repr| parse_single(repr, false));
}

#[test]
fn iteratee_attribute_only_records() {
    attribute_only_records(consume_to_value);
}

fn attribute_only_records(read_single: ReadSingleValue) {
    assert_eq!(
        read_single(
            "{
            first: @attr

        }"
        )
        .unwrap(),
        Value::from_vec(vec![("first", Value::of_attr("attr"))])
    );

    assert_eq!(
        read_single(
            "{
            first: @attr()

        }"
        )
        .unwrap(),
        Value::from_vec(vec![("first", Value::of_attr("attr"))])
    );

    assert_eq!(
        read_single(
            "{
            first: @attr,

        }"
        )
        .unwrap(),
        Value::from_vec(vec![
            Item::of(("first", Value::of_attr("attr"))),
            Item::ValueItem(Value::Extant)
        ])
    );

    assert_eq!(
        read_single(
            "{
            first: @attr(),

        }"
        )
        .unwrap(),
        Value::record(vec![
            Item::of(("first", Value::of_attr("attr"))),
            Item::ValueItem(Value::Extant)
        ])
    );

    assert_eq!(
        read_single(
            "{
            first: @attr, second

        }"
        )
        .unwrap(),
        Value::from_vec(vec![
            Item::of(("first", Value::of_attr("attr"))),
            Item::of("second")
        ])
    );

    assert_eq!(
        read_single(
            "{
            first: @attr,
            second

        }"
        )
        .unwrap(),
        Value::from_vec(vec![
            Item::of(("first", Value::of_attr("attr"))),
            Item::of("second")
        ])
    );

    assert_eq!(
        read_single(
            "{
            first: @attr
            second

        }"
        )
        .unwrap(),
        Value::from_vec(vec![
            Item::of(("first", Value::of_attr("attr"))),
            Item::of("second")
        ])
    );

    assert_eq!(
        read_single(
            "{
            first: @attr(), second

        }"
        )
        .unwrap(),
        Value::from_vec(vec![
            Item::of(("first", Value::of_attr("attr"))),
            Item::of("second")
        ])
    );

    assert_eq!(
        read_single(
            "{
            first: @attr(),
            second

        }"
        )
        .unwrap(),
        Value::from_vec(vec![
            Item::of(("first", Value::of_attr("attr"))),
            Item::of("second")
        ])
    );

    assert_eq!(
        read_single(
            "{
            first: @attr()
            second

        }"
        )
        .unwrap(),
        Value::from_vec(vec![
            Item::of(("first", Value::of_attr("attr"))),
            Item::of("second")
        ])
    );
}

#[test]
fn parse_attributes_with_new_line() {
    attributes_with_new_line(|repr| parse_single(repr, false));
}

#[test]
fn iteratee_attributes_with_new_line() {
    attributes_with_new_line(consume_to_value);
}

fn attributes_with_new_line(read_single: ReadSingleValue) {
    assert_eq!(
        read_single(
            "{
                foo: @bar()
                baz
            }
            "
        )
        .unwrap(),
        read_single(
            "{
                foo: @bar(),
                baz
            }
            "
        )
        .unwrap()
    );

    assert_eq!(
        read_single(
            "{
                foo: @bar(), baz
            }
            "
        )
        .unwrap(),
        read_single(
            "{
                foo: @bar(),
                baz
            }
            "
        )
        .unwrap()
    );

    assert_eq!(
        read_single(
            "{
                foo: @bar()
                baz: qux
            }
            "
        )
        .unwrap(),
        read_single(
            "{
                foo: @bar(),
                baz: qux
            }
            "
        )
        .unwrap()
    );

    assert_eq!(
        read_single(
            "{
                one: two
                three: @four()
                five: six
                seven: @eight(nine:ten)
                eleven: twelve
            }
            "
        )
        .unwrap(),
        read_single(
            "{
                one: two,
                three: @four(),
                five: six,
                seven: @eight(nine:ten),
                eleven: twelve
            }
            "
        )
        .unwrap()
    );

    assert_eq!(
        read_single(
            "{
                one: @two()
                three: @four(five:six),
                seven: eight,
                nine: ten
                eleven: twelve
            }
            "
        )
        .unwrap(),
        read_single(
            "{
                one: @two(),
                three: @four(five:six),
                seven: eight,
                nine: ten,
                eleven: twelve
            }
            "
        )
        .unwrap()
    );

    assert_eq!(
        read_single(
            "{
                foo: @bar()
                @baz
            }
            "
        )
        .unwrap(),
        read_single(
            "{
                foo: @bar
                @baz
            }
            "
        )
        .unwrap()
    );
}

#[test]
fn parse_tagged_records() {
    tagged_records(|repr| parse_single(repr, false));
}

#[test]
fn iteratee_tagged_records() {
    tagged_records(consume_to_value);
}

fn tagged_records(read_single: ReadSingleValue) {
    assert_eq!(read_single("@name {}").unwrap(), Value::of_attr("name"));
    assert_eq!(
        read_single("@first@second {}").unwrap(),
        Value::of_attrs(vec![Attr::of("first"), Attr::of("second")])
    );
    assert_eq!(
        read_single("@name id").unwrap(),
        Value::Record(vec![Attr::of("name")], vec![Item::of("id")])
    );
    assert_eq!(
        read_single(r#"@name"@value""#).unwrap(),
        Value::Record(vec![Attr::of("name")], vec![Item::of("@value")])
    );
    assert_eq!(
        read_single("@name -4").unwrap(),
        Value::Record(vec![Attr::of("name")], vec![Item::of(-4)])
    );

    assert_eq!(
        read_single("@name{id}").unwrap(),
        Value::Record(vec![Attr::of("name")], vec![Item::of("id")])
    );
    assert_eq!(
        read_single(r#"@name { "@value" }"#).unwrap(),
        Value::Record(vec![Attr::of("name")], vec![Item::of("@value")])
    );
    assert_eq!(
        read_single("@name{ -4}").unwrap(),
        Value::Record(vec![Attr::of("name")], vec![Item::of(-4)])
    );
    assert_eq!(
        read_single("@name {1, 2, 3}").unwrap(),
        Value::Record(
            vec![Attr::of("name")],
            vec![Item::of(1u32), Item::of(2u32), Item::of(3u32)]
        )
    );
    assert_eq!(
        read_single("@name {a:1,b:2}").unwrap(),
        Value::Record(
            vec![Attr::of("name")],
            vec![Item::of(("a", 1u32)), Item::of(("b", 2u32))]
        )
    );
    assert_eq!(
        read_single("@name {,}").unwrap(),
        Value::Record(
            vec![Attr::of("name")],
            vec![Item::of(Value::Extant), Item::of(Value::Extant)]
        )
    );
}

#[test]
fn parse_nested_tagged_records() {
    nested_tagged_records(|repr| parse_single(repr, false));
}

#[test]
fn iteratee_nested_tagged_records() {
    nested_tagged_records(consume_to_value);
}

fn nested_tagged_records(read_single: ReadSingleValue) {
    assert_eq!(
        read_single("@name(@inner1 {}, @inner2 {})").unwrap(),
        Value::of_attr((
            "name",
            Value::from_vec(vec![Value::of_attr("inner1"), Value::of_attr("inner2")])
        ))
    );
    assert_eq!(
        read_single("@name(@inner {1, 2})").unwrap(),
        Value::of_attr((
            "name",
            Value::Record(
                vec![Attr::of("inner")],
                vec![Item::of(1u32), Item::of(2u32)]
            )
        ))
    );
    assert_eq!(
        read_single("@name {@inner1 {}, @inner2 {} }").unwrap(),
        Value::Record(
            vec![Attr::of("name")],
            vec![
                Item::of(Value::of_attr("inner1")),
                Item::of(Value::of_attr("inner2"))
            ]
        )
    );
    assert_eq!(
        read_single("@name {@inner {1, 2}}").unwrap(),
        Value::Record(
            vec![Attr::of("name")],
            vec![Item::of(Value::Record(
                vec![Attr::of("inner")],
                vec![Item::of(1u32), Item::of(2u32)]
            ))]
        )
    );
    assert_eq!(
        read_single("@name {0, @inner {1, 2}}").unwrap(),
        Value::Record(
            vec![Attr::of("name")],
            vec![
                Item::of(0u32),
                Item::of(Value::Record(
                    vec![Attr::of("inner")],
                    vec![Item::of(1u32), Item::of(2u32)]
                ))
            ]
        )
    );
    assert_eq!(
        read_single("@name {first: {1, 2}, {second}: 3}").unwrap(),
        Value::Record(
            vec![Attr::of("name")],
            vec![
                Item::slot("first", Value::from_vec(vec![1u32, 2u32])),
                Item::slot(Value::singleton("second"), 3u32)
            ]
        )
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

    assert_eq!(
        parse_single(&format!("@name({})", big_str), false).unwrap(),
        Value::of_attr(Attr::of(("name", BigInt::from_str(&big_str).unwrap())))
    );

    let u64_max = u64::max_value();
    let u64_max_str = u64_max.to_string();

    assert_eq!(
        parse_single(&format!("@name({})", u64_max_str), false).unwrap(),
        Value::of_attr(Attr::of(("name", Value::UInt64Value(u64_max))))
    );
}

#[test]
fn biguint_tests() {
    let mut rng = rand::thread_rng();
    let big_str = rng.gen_biguint(1000).to_string();

    assert_eq!(
        parse_single(&format!("@name({})", big_str), false).unwrap(),
        Value::of_attr(Attr::of(("name", BigUint::from_str(&big_str).unwrap())))
    );

    let u64_oob_str = (u64::max_value() as i128 + 1).to_string();

    assert_eq!(
        parse_single(&format!("@name({})", u64_oob_str), false).unwrap(),
        Value::of_attr(Attr::of((
            "name",
            Value::BigUint(BigUint::from_str(&u64_oob_str).unwrap())
        )))
    );
}

#[test]
fn error_tests() {
    if let Err(e) = parse_single(&format!("@name(`)"), false) {
        assert_eq!(e.to_string(), "Bad token at: 1:7");
    } else {
        panic!("Error expected!");
    }

    if let Err(e) = parse_single(&format!("@name(\n12: *foo)"), false) {
        assert_eq!(e.to_string(), "Bad token at: 2:5");
    } else {
        panic!("Error expected!");
    }

    if let Err(e) = parse_single(&format!("@name(\r12: *foo)"), false) {
        assert_eq!(e.to_string(), "Bad token at: 2:5");
    } else {
        panic!("Error expected!");
    }

    if let Err(e) = parse_single(&format!("@name(\r\n12: *foo)"), false) {
        assert_eq!(e.to_string(), "Bad token at: 2:5");
    } else {
        panic!("Error expected!");
    }

    if let Err(e) = parse_single(
        &format!("@name(\n112: foo\n113: bar\n114: *baz\n115: qux)"),
        false,
    ) {
        assert_eq!(e.to_string(), "Bad token at: 4:6");
    } else {
        panic!("Error expected!");
    }

    if let Err(e) = parse_single(
        &format!("@name(\r112: foo\r113: bar\r114: *baz\r115: qux)"),
        false,
    ) {
        assert_eq!(e.to_string(), "Bad token at: 4:6");
    } else {
        panic!("Error expected!");
    }

    if let Err(e) = parse_single(
        &format!("@name(\r\n112: foo\r\n113: bar\r\n114: *baz\r\n115: qux)"),
        false,
    ) {
        assert_eq!(e.to_string(), "Bad token at: 4:6");
    } else {
        panic!("Error expected!");
    }

    if let Err(e) = parse_single(
        &format!(
        "@config {{\n    @client {{\n        buffer_size: 2\n        router: **invalid\n    }}\n}}"
    ),
        false,
    ) {
        assert_eq!(e.to_string(), "Bad token at: 4:17");
    } else {
        panic!("Error expected!");
    }

    if let Err(e) = parse_single(
        &format!(
        "@config {{\r    @client {{\r        buffer_size: 2\r        router: **invalid\r    }}\r}}"
    ),
        false,
    ) {
        assert_eq!(e.to_string(), "Bad token at: 4:17");
    } else {
        panic!("Error expected!");
    }

    if let Err(e) = parse_single(&format!(
        "@config {{\r\n    @client {{\r\n        buffer_size: 2\r\n        router: **invalid\r\n    }}\r\n}}"
    ), false) {
        assert_eq!(e.to_string(), "Bad token at: 4:17");
    } else {
        panic!("Error expected!");
    }
}
