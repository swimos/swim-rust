use trybuild::TestCases;

// mod pass;

#[test]
fn test_derive() {
    let t = TestCases::new();

    t.compile_fail("src/agent/tests/derive/fail/*.rs");
}
