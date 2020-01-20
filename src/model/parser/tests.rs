use hamcrest2::assert_that;
use hamcrest2::prelude::*;

use super::*;

fn read_single_token(repr: &str) -> Result<ReconToken<&str>, Option<BadToken>> {
    match tokenize_str(repr).next() {
        Some(Ok(LocatedReconToken(token, _))) => Ok(token),
        Some(Err(failed)) => Err(Some(failed)),
        _ => Err(None),
    }
}

#[test]
fn unescape_strings() {
    assert_that!(unescape("no escapes").unwrap(), eq("no escapes"));
    assert_that!(unescape(r"\r\n\t").unwrap(), eq("\r\n\t"));
    assert_that!(unescape(r"A string with \n two lines.").unwrap(), eq("A string with \n two lines."));
    assert_that!(unescape(r#"A string with a \"quoted\" section."#).unwrap(), eq("A string with a \"quoted\" section."));
    assert_that!(unescape(r"Escaped escapes \\r\\t.").unwrap(), eq(r"Escaped escapes \r\t."));
    assert_that!(unescape(r"\u0015").unwrap(), eq("\u{15}"));
}

#[test]
fn symbol_tokens() {
    assert_that!(read_single_token("@").unwrap(), eq(ReconToken::AttrMarker));
    assert_that!(read_single_token(":").unwrap(), eq(ReconToken::SlotDivider));
    assert_that!(read_single_token(",").unwrap(), eq(ReconToken::EntrySep));
    assert_that!(read_single_token(";").unwrap(), eq(ReconToken::EntrySep));
    assert_that!(read_single_token("(").unwrap(), eq(ReconToken::AttrBodyStart));
    assert_that!(read_single_token(")").unwrap(), eq(ReconToken::AttrBodyEnd));
    assert_that!(read_single_token("{").unwrap(), eq(ReconToken::RecordBodyStart));
    assert_that!(read_single_token("}").unwrap(), eq(ReconToken::RecordBodyEnd));
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
fn integer_tokens() {
    assert_that!(read_single_token("0").unwrap(), eq(ReconToken::Int32Literal(0)));
    assert_that!(read_single_token("1").unwrap(), eq(ReconToken::Int32Literal(1)));
    assert_that!(read_single_token("42").unwrap(), eq(ReconToken::Int32Literal(42)));
    assert_that!(read_single_token("1076").unwrap(), eq(ReconToken::Int32Literal(1076)));
    assert_that!(read_single_token("-1").unwrap(), eq(ReconToken::Int32Literal(-1)));
    assert_that!(read_single_token("-04").unwrap(), eq(ReconToken::Int32Literal(-4)));

    let big_n = i64::from(std::i32::MAX) * 2i64;
    let big = big_n.to_string();

    assert_that!(read_single_token(big.borrow()).unwrap(), eq(ReconToken::Int64Literal(big_n)));

    let big_n_neg = -big_n;
    let big_neg = big_n_neg.to_string();

    assert_that!(read_single_token(big_neg.borrow()).unwrap(), eq(ReconToken::Int64Literal(big_n_neg)));
}

#[test]
fn bool_tokens() {
    assert_that!(read_single_token("true").unwrap(), eq(ReconToken::BoolLiteral(true)));
    assert_that!(read_single_token("false").unwrap(), eq(ReconToken::BoolLiteral(false)));
}

#[test]
fn identifier_tokens() {
    assert_that!(read_single_token("name").unwrap(), eq(ReconToken::Identifier("name")));
    assert_that!(read_single_token("اسم").unwrap(), eq(ReconToken::Identifier("اسم")));
    assert_that!(read_single_token("name2").unwrap(), eq(ReconToken::Identifier("name2")));
    assert_that!(read_single_token("_name").unwrap(), eq(ReconToken::Identifier("_name")));
    assert_that!(read_single_token("first_second").unwrap(), eq(ReconToken::Identifier("first_second")));
}

#[test]
fn string_literal_tokens() {
    assert_that!(read_single_token(r#""name""#).unwrap(), eq(ReconToken::StringLiteral("name")));
    assert_that!(read_single_token(r#""اسم""#).unwrap(), eq(ReconToken::StringLiteral("اسم")));
    assert_that!(read_single_token(r#""two words""#).unwrap(), eq(ReconToken::StringLiteral("two words")));
    assert_that!(read_single_token(r#""2name""#).unwrap(), eq(ReconToken::StringLiteral("2name")));
    assert_that!(read_single_token(r#""true""#).unwrap(), eq(ReconToken::StringLiteral("true")));
    assert_that!(read_single_token(r#""false""#).unwrap(), eq(ReconToken::StringLiteral("false")));
    assert_that!(read_single_token(r#""£%^$&*""#).unwrap(), eq(ReconToken::StringLiteral("£%^$&*")));
    assert_that!(read_single_token("\"\r\n\t\"").unwrap(), eq(ReconToken::StringLiteral("\r\n\t")));
    assert_that!(read_single_token(r#""\r\n\t""#).unwrap(), eq(ReconToken::StringLiteral(r"\r\n\t")));
    assert_that!(read_single_token(r#""a \"quote\" z""#).unwrap(), eq(ReconToken::StringLiteral(r#"a \"quote\" z"#)));
    assert_that!(read_single_token(r#""a \\ z""#).unwrap(), eq(ReconToken::StringLiteral(r#"a \\ z"#)));
}

#[test]
fn floating_point_tokens() {
    assert_that!(read_single_token("0.0").unwrap(), eq(ReconToken::Float64Literal(0.0)));
    assert_that!(read_single_token(".0").unwrap(), eq(ReconToken::Float64Literal(0.0)));
    assert_that!(read_single_token("3.5").unwrap(), eq(ReconToken::Float64Literal(3.5)));
    assert_that!(read_single_token("-1.0").unwrap(), eq(ReconToken::Float64Literal(-1.0)));
    assert_that!(read_single_token("3e2").unwrap(), eq(ReconToken::Float64Literal(3e2)));
    assert_that!(read_single_token("50.06e8").unwrap(), eq(ReconToken::Float64Literal(50.06e8)));
    assert_that!(read_single_token(".2e0").unwrap(), eq(ReconToken::Float64Literal(0.2e0)));
    assert_that!(read_single_token("3E2").unwrap(), eq(ReconToken::Float64Literal(3e2)));
    assert_that!(read_single_token("50.06E8").unwrap(), eq(ReconToken::Float64Literal(50.06e8)));
    assert_that!(read_single_token(".2E0").unwrap(), eq(ReconToken::Float64Literal(0.2e0)));
    assert_that!(read_single_token("3e-9").unwrap(), eq(ReconToken::Float64Literal(3e-9)));
    assert_that!(read_single_token("3E-9").unwrap(), eq(ReconToken::Float64Literal(3e-9)));
    assert_that!(read_single_token("-.76e-12").unwrap(), eq(ReconToken::Float64Literal(-0.76e-12)));
}

#[test]
fn token_sequence() {
    let source = "@name(2){ x : -3, \"y\": true \n 7 } ";
    let tokens = tokenize_str(source)
        .map(|r| { r.unwrap().0 })
        .collect::<Vec<_>>();

    assert_that!(tokens, eq(vec![
        ReconToken::AttrMarker,
        ReconToken::Identifier("name"),
        ReconToken::AttrBodyStart,
        ReconToken::Int32Literal(2),
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
        ReconToken::Int32Literal(7),
        ReconToken::RecordBodyEnd,
    ]));
}
