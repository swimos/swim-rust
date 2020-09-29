use trybuild::TestCases;

#[test]
fn test_derive() {
    let t = TestCases::new();

    t.pass("src/agent/tests/derive/examples/pass/*.rs");
    t.compile_fail("src/agent/tests/derive/examples/fail/*.rs");
}
