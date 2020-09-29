use trybuild::TestCases;

#[test]
fn test_derive_pass() {
    let t = TestCases::new();

    t.pass("src/agent/tests/derive/examples/pass/*.rs");
}
