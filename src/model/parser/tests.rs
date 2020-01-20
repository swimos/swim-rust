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

    let big_n =  i64::from(i32::max_value()) * 2i64;
    let big = big_n.to_string();

    assert_that!(read_single_token(big.borrow()).unwrap(), eq(ReconToken::Int64Literal(big_n)));

    let big_n_neg =  -big_n;
    let big_neg = big_n_neg.to_string();

    assert_that!(read_single_token(big_neg.borrow()).unwrap(), eq(ReconToken::Int64Literal(big_n_neg)));
}