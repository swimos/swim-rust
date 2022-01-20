use crate::hasher::calculate_hash;
use crate::parser::{ParseError, Span};
use swim_model::Value;

fn value_from_string(rep: &str) -> Result<Value, ParseError> {
    let span = Span::new(rep);
    crate::parser::parse_recognize(span, false)
}

#[test]
fn recon_hash_not_eq() {
    let first = "@foo({1})";
    let second = "@name({a: 1, b: 2})";

    assert_ne!(
        calculate_hash(first).unwrap(),
        calculate_hash(second).unwrap()
    );

    let first = "@foo({})";
    let second = "@foo()";

    assert_ne!(
        calculate_hash(first).unwrap(),
        calculate_hash(second).unwrap()
    );

    let first = "@attr({{}})";
    let second = "@attr({})";

    assert_ne!(
        calculate_hash(first).unwrap(),
        calculate_hash(second).unwrap()
    );

    let first = "@foo({1})";
    let second = "@foo(1)";

    assert_ne!(
        calculate_hash(first).unwrap(),
        calculate_hash(second).unwrap()
    );

    let first = "@name(@foo@bar)";
    let second = "@name({@foo@bar})";

    assert_ne!(
        calculate_hash(first).unwrap(),
        calculate_hash(second).unwrap()
    );

    let first = "@foo(@bar {})";
    let second = "@foo(@bar, {})";

    assert_ne!(
        calculate_hash(first).unwrap(),
        calculate_hash(second).unwrap()
    );

    //Todo dm

    let first = "@name(a: 1, b: 2, c: 3)";
    let second = "@name(a:1, b: 4, c: 3)";

    assert_ne!(
        calculate_hash(first).unwrap(),
        calculate_hash(second).unwrap()
    );

    let first = "@foo({{1,2}})";
    let second = "@foo({1, 2})";

    assert_ne!(
        calculate_hash(first).unwrap(),
        calculate_hash(second).unwrap()
    );

    let first = "@name(a: 1, b: 2)";
    let second = "@name(   {a: 3, b: 2}    )";

    assert_ne!(
        calculate_hash(first).unwrap(),
        calculate_hash(second).unwrap()
    );

    let first = "@foo(1)";
    let second = "@foo({1})";

    assert_ne!(
        calculate_hash(first).unwrap(),
        calculate_hash(second).unwrap()
    );

    let first = "@foo()";
    let second = "@foo({})";

    assert_ne!(
        calculate_hash(first).unwrap(),
        calculate_hash(second).unwrap()
    );

    let first = "@foo(@bar@baz) ";
    let second = "@foo({@bar@baz})";

    assert_ne!(
        calculate_hash(first).unwrap(),
        calculate_hash(second).unwrap()
    );

    let first = "@name({1,2},{3,4})";
    let second = "@name({1,2,3,4})";

    assert_ne!(
        calculate_hash(first).unwrap(),
        calculate_hash(second).unwrap()
    );

    let first = "@foo(1, @bar(2), 3, 4)";
    let second = "@foo(1, @bar({2}), 3, 4)";

    assert_ne!(
        calculate_hash(first).unwrap(),
        calculate_hash(second).unwrap()
    );
}

#[test]
fn recon_hash_eq() {
    let first = "@name(a: 1, b: 2)";
    let second = "@name({a: 1, b: 2})";

    assert_eq!(
        calculate_hash(first).unwrap(),
        calculate_hash(second).unwrap()
    );

    let first = "@foo({a: 1})";
    let second = "@foo(a: 1)";

    assert_eq!(
        calculate_hash(first).unwrap(),
        calculate_hash(second).unwrap()
    );

    let first = "@foo(a:)";
    let second = "@foo({a: })";

    assert_eq!(
        calculate_hash(first).unwrap(),
        calculate_hash(second).unwrap()
    );

    let first = "@name({a: 1, b: 2}, 3)";
    let second = "@name({{a: 1, b: 2}, 3})";

    assert_eq!(
        calculate_hash(first).unwrap(),
        calculate_hash(second).unwrap()
    );

    let first = "@name(3, {a: 1, b: 2})";
    let second = "@name({3, {a: 1, b: 2}})";

    assert_eq!(
        calculate_hash(first).unwrap(),
        calculate_hash(second).unwrap()
    );

    let first = "@attr({{1,2},{3,4}})";
    let second = "@attr({1,2},{3,4})";

    assert_eq!(
        calculate_hash(first).unwrap(),
        calculate_hash(second).unwrap()
    );

    let first = "@attr({1,2,3,4})";
    let second = "@attr(1,2,3,4)";

    assert_eq!(
        calculate_hash(first).unwrap(),
        calculate_hash(second).unwrap()
    );

    let first = "@foo({one: 1, two: @bar(1,2,3), three: 3, four: {@baz({1,2})}})";
    let second = "@foo(one: 1, two: @bar({1,2,3}), three: 3, four: {@baz(1,2)})";

    assert_eq!(
        calculate_hash(first).unwrap(),
        calculate_hash(second).unwrap()
    );

    //Todo dm
    let first = "@name(a: 1, b: 2)";
    let second = "@name(a: 1, b: 2)";

    assert_eq!(
        calculate_hash(first).unwrap(),
        calculate_hash(second).unwrap()
    );

    let first = "\"test\"";
    let second = "test";

    assert_eq!(
        calculate_hash(first).unwrap(),
        calculate_hash(second).unwrap()
    );

    let first = "{a:2}";
    let second = "{ a: 2 }";

    assert_eq!(
        calculate_hash(first).unwrap(),
        calculate_hash(second).unwrap()
    );

    let first = "@tag(){}:1";
    let second = "@tag{}:1";

    assert_eq!(
        calculate_hash(first).unwrap(),
        calculate_hash(second).unwrap()
    );

    let first = "@name(a: 1, b: 2)";
    let second = "@name({a: 1, b: 2})";

    assert_eq!(
        calculate_hash(first).unwrap(),
        calculate_hash(second).unwrap()
    );

    let first = "@first(1)@second(2)";
    let second = "@first(1)@second(2) {}";

    assert_eq!(
        calculate_hash(first).unwrap(),
        calculate_hash(second).unwrap()
    );

    let first = "{ @inner(0), after }";
    let second = "{ @inner(0) {}, after }";

    assert_eq!(
        calculate_hash(first).unwrap(),
        calculate_hash(second).unwrap()
    );

    let first = "@outer(@inner)";
    let second = "@outer(@inner {})";

    assert_eq!(
        calculate_hash(first).unwrap(),
        calculate_hash(second).unwrap()
    );

    let first = "@foo({one: 1, two: @bar(1,2,3), three: 3, four: {@baz({1,2})}})";
    let second = "@foo(one: 1, two: @bar({1,2,3}), three: 3, four: {@baz(1,2)})";

    assert_eq!(
        calculate_hash(first).unwrap(),
        calculate_hash(second).unwrap()
    );

    let first = "@foo(1,2)";
    let second = "@foo({1,2})";

    assert_eq!(
        calculate_hash(first).unwrap(),
        calculate_hash(second).unwrap()
    );

    let first = "@foo({one: 1, two: @bar(1,2,3)})";
    let second = "@foo(one: 1, two: @bar({1,2,3}))";

    assert_eq!(
        calculate_hash(first).unwrap(),
        calculate_hash(second).unwrap()
    );

    let first = "@name(@foo,@bar)";
    let second = "@name({@foo, @bar})";

    assert_eq!(
        calculate_hash(first).unwrap(),
        calculate_hash(second).unwrap()
    );

    let first = "@name(1, @foo@bar)";
    let second = "@name({1, @foo@bar})";

    assert_eq!(
        calculate_hash(first).unwrap(),
        calculate_hash(second).unwrap()
    );

    let first = "@name(@foo@bar({@bar, 1, @baz()}), @bar@foo()@baz)";
    let second = "@name({@foo@bar(@bar, 1, @baz()), @bar@foo()@baz})";

    assert_eq!(
        calculate_hash(first).unwrap(),
        calculate_hash(second).unwrap()
    );

    let first = "@foo(1, @bar(2,3), 4, 5)";
    let second = "@foo(1, @bar({2,3}), 4, 5)";

    assert_eq!(
        calculate_hash(first).unwrap(),
        calculate_hash(second).unwrap()
    );

    let first = "@name(a: @foo)";
    let second = "@name({a: @foo})";

    assert_eq!(
        calculate_hash(first).unwrap(),
        calculate_hash(second).unwrap()
    );

    let first = "@name(b: @foo@bar@baz, @foo(1,2), @bar@baz))";
    let second = "@name({b: @foo@bar@baz, @foo({1,2}), @bar@baz})";

    assert_eq!(
        calculate_hash(first).unwrap(),
        calculate_hash(second).unwrap()
    );

    let first = "@name(b: )";
    let second = "@name({b: })";

    assert_eq!(
        calculate_hash(first).unwrap(),
        calculate_hash(second).unwrap()
    );
}
