use hamcrest2::assert_that;
use hamcrest2::prelude::*;

use super::*;

fn read_single_token(repr: &str) -> Result<ReconToken, FailedAt> {
    match tokenize_str(repr).next() {
        Some(Ok(LocatedReconToken(token, _))) => Ok(token),
        Some(Err(failed)) => Err(failed),
        _ => Err(FailedAt(0)),
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