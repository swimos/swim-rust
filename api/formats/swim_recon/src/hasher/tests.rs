use crate::hasher::normalise;
use crate::parser::{ParseError, ParseIterator, Span};
use swim_model::Value;

fn value_from_string(rep: &str) -> Result<Value, ParseError> {
    let span = Span::new(rep);
    crate::parser::parse_recognize(span, false)
}

#[test]
fn recon_hash_not_eq() {
    let first = "@foo({1})";
    let second = "@name({a: 1, b: 2})";

    let first_iter = &mut ParseIterator::new(Span::new(first), false).peekable();
    let second_iter = &mut ParseIterator::new(Span::new(second), false).peekable();

    assert_ne!(normalise(first_iter), normalise(second_iter));

    let first = "@foo({})";
    let second = "@foo()";

    let first_iter = &mut ParseIterator::new(Span::new(first), false).peekable();
    let second_iter = &mut ParseIterator::new(Span::new(second), false).peekable();

    assert_ne!(normalise(first_iter), normalise(second_iter));

    let first = "@attr({{}})";
    let second = "@attr({})";

    let first_iter = &mut ParseIterator::new(Span::new(first), false).peekable();
    let second_iter = &mut ParseIterator::new(Span::new(second), false).peekable();

    assert_ne!(normalise(first_iter), normalise(second_iter));

    let first = "@foo({1})";
    let second = "@foo(1)";

    let first_iter = &mut ParseIterator::new(Span::new(first), false).peekable();
    let second_iter = &mut ParseIterator::new(Span::new(second), false).peekable();

    assert_ne!(normalise(first_iter), normalise(second_iter));

    let first = "@name(@foo@bar)";
    let second = "@name({@foo@bar})";

    let first_iter = &mut ParseIterator::new(Span::new(first.clone()), false).peekable();
    let second_iter = &mut ParseIterator::new(Span::new(second.clone()), false).peekable();

    assert_ne!(normalise(first_iter), normalise(second_iter));

    let first = "@foo(@bar {})";
    let second = "@foo(@bar, {})";

    let first_iter = &mut ParseIterator::new(Span::new(first.clone()), false).peekable();
    let second_iter = &mut ParseIterator::new(Span::new(second.clone()), false).peekable();

    assert_ne!(normalise(first_iter), normalise(second_iter));

    //Todo dm

    let first = "@name(a: 1, b: 2, c: 3)";
    let second = "@name(a:1, b: 4, c: 3)";

    let first_iter = &mut ParseIterator::new(Span::new(first), false).peekable();
    let second_iter = &mut ParseIterator::new(Span::new(second), false).peekable();

    assert_ne!(normalise(first_iter), normalise(second_iter));

    let first = "@foo({{1,2}})";
    let second = "@foo({1, 2})";

    let first_iter = &mut ParseIterator::new(Span::new(first), false).peekable();
    let second_iter = &mut ParseIterator::new(Span::new(second), false).peekable();

    assert_ne!(normalise(first_iter), normalise(second_iter));

    let first = "@name(a: 1, b: 2)";
    let second = "@name(   {a: 3, b: 2}    )";

    let first_iter = &mut ParseIterator::new(Span::new(first), false).peekable();
    let second_iter = &mut ParseIterator::new(Span::new(second), false).peekable();

    assert_ne!(normalise(first_iter), normalise(second_iter));

    let first = "{{test}:3}";
    let second = "{{test}3}";

    let first_iter = &mut ParseIterator::new(Span::new(first), false).peekable();
    let second_iter = &mut ParseIterator::new(Span::new(second), false).peekable();

    assert_ne!(normalise(first_iter), normalise(second_iter));

    let first = "@foo(1)";
    let second = "@foo({1})";

    let first_iter = &mut ParseIterator::new(Span::new(first), false).peekable();
    let second_iter = &mut ParseIterator::new(Span::new(second), false).peekable();

    assert_ne!(normalise(first_iter), normalise(second_iter));

    let first = "@foo()";
    let second = "@foo({})";

    let first_iter = &mut ParseIterator::new(Span::new(first), false).peekable();
    let second_iter = &mut ParseIterator::new(Span::new(second), false).peekable();

    assert_ne!(normalise(first_iter), normalise(second_iter));

    let first = "@foo(@bar@baz) ";
    let second = "@foo({@bar@baz})";

    let first_iter = &mut ParseIterator::new(Span::new(first), false).peekable();
    let second_iter = &mut ParseIterator::new(Span::new(second), false).peekable();

    assert_ne!(normalise(first_iter), normalise(second_iter));

    let first = "@name({1,2},{3,4})";
    let second = "@name({1,2,3,4})";

    let first_iter = &mut ParseIterator::new(Span::new(first), false).peekable();
    let second_iter = &mut ParseIterator::new(Span::new(second), false).peekable();

    assert_ne!(normalise(first_iter), normalise(second_iter));

    let first = "@foo(1, @bar(2), 3, 4)";
    let second = "@foo(1, @bar({2}), 3, 4)";

    let first_iter = &mut ParseIterator::new(Span::new(first), false).peekable();
    let second_iter = &mut ParseIterator::new(Span::new(second), false).peekable();

    assert_ne!(normalise(first_iter), normalise(second_iter));
}

#[test]
fn recon_hash_eq() {
    let first = "@name(a: 1, b: 2)";
    let second = "@name({a: 1, b: 2})";

    let first_iter = &mut ParseIterator::new(Span::new(first), false).peekable();
    let second_iter = &mut ParseIterator::new(Span::new(second), false).peekable();

    assert_eq!(normalise(first_iter), normalise(second_iter));

    let first = "@foo({a: 1})";
    let second = "@foo(a: 1)";

    let first_iter = &mut ParseIterator::new(Span::new(first), false).peekable();
    let second_iter = &mut ParseIterator::new(Span::new(second), false).peekable();

    assert_eq!(normalise(first_iter), normalise(second_iter));

    let first = "@foo(a:)";
    let second = "@foo({a: })";

    let first_iter = &mut ParseIterator::new(Span::new(first), false).peekable();
    let second_iter = &mut ParseIterator::new(Span::new(second), false).peekable();

    assert_eq!(normalise(first_iter), normalise(second_iter));

    let first = "@name({a: 1, b: 2}, 3)";
    let second = "@name({{a: 1, b: 2}, 3})";

    let first_iter = &mut ParseIterator::new(Span::new(first), false).peekable();
    let second_iter = &mut ParseIterator::new(Span::new(second), false).peekable();

    assert_eq!(normalise(first_iter), normalise(second_iter));

    let first = "@name(3, {a: 1, b: 2})";
    let second = "@name({3, {a: 1, b: 2}})";

    let first_iter = &mut ParseIterator::new(Span::new(first), false).peekable();
    let second_iter = &mut ParseIterator::new(Span::new(second), false).peekable();

    assert_eq!(normalise(first_iter), normalise(second_iter));

    let first = "@attr({{1,2},{3,4}})";
    let second = "@attr({1,2},{3,4})";

    let first_iter = &mut ParseIterator::new(Span::new(first), false).peekable();
    let second_iter = &mut ParseIterator::new(Span::new(second), false).peekable();

    assert_eq!(normalise(first_iter), normalise(second_iter));

    let first = "@attr({1,2,3,4})";
    let second = "@attr(1,2,3,4)";

    let first_iter = &mut ParseIterator::new(Span::new(first), false).peekable();
    let second_iter = &mut ParseIterator::new(Span::new(second), false).peekable();

    assert_eq!(normalise(first_iter), normalise(second_iter));

    let first = "@foo({one: 1, two: @bar(1,2,3), three: 3, four: {@baz({1,2})}})";
    let second = "@foo(one: 1, two: @bar({1,2,3}), three: 3, four: {@baz(1,2)})";

    let first_iter = &mut ParseIterator::new(Span::new(first), false).peekable();
    let second_iter = &mut ParseIterator::new(Span::new(second), false).peekable();

    assert_eq!(normalise(first_iter), normalise(second_iter));

    //Todo dm
    let first = "@name(a: 1, b: 2)";
    let second = "@name(a: 1, b: 2)";

    let first_iter = &mut ParseIterator::new(Span::new(first), false).peekable();
    let second_iter = &mut ParseIterator::new(Span::new(second), false).peekable();

    assert_eq!(normalise(first_iter), normalise(second_iter));

    let first = "\"test\"";
    let second = "test";

    let first_iter = &mut ParseIterator::new(Span::new(first), false).peekable();
    let second_iter = &mut ParseIterator::new(Span::new(second), false).peekable();

    assert_eq!(normalise(first_iter), normalise(second_iter));

    let first = "{a:2}";
    let second = "{ a: 2 }";

    let first_iter = &mut ParseIterator::new(Span::new(first), false).peekable();
    let second_iter = &mut ParseIterator::new(Span::new(second), false).peekable();

    assert_eq!(normalise(first_iter), normalise(second_iter));

    let first = "@tag(){}:1";
    let second = "@tag{}:1";

    let first_iter = &mut ParseIterator::new(Span::new(first), false).peekable();
    let second_iter = &mut ParseIterator::new(Span::new(second), false).peekable();

    assert_eq!(normalise(first_iter), normalise(second_iter));

    let first = "@name(a: 1, b: 2)";
    let second = "@name({a: 1, b: 2})";

    let first_iter = &mut ParseIterator::new(Span::new(first), false).peekable();
    let second_iter = &mut ParseIterator::new(Span::new(second), false).peekable();

    assert_eq!(normalise(first_iter), normalise(second_iter));

    let first = "@first(1)@second(2)";
    let second = "@first(1)@second(2) {}";

    let first_iter = &mut ParseIterator::new(Span::new(first), false).peekable();
    let second_iter = &mut ParseIterator::new(Span::new(second), false).peekable();

    assert_eq!(normalise(first_iter), normalise(second_iter));

    let first = "{ @inner(0), after }";
    let second = "{ @inner(0) {}, after }";

    let first_iter = &mut ParseIterator::new(Span::new(first), false).peekable();
    let second_iter = &mut ParseIterator::new(Span::new(second), false).peekable();

    assert_eq!(normalise(first_iter), normalise(second_iter));

    let first = "@outer(@inner)";
    let second = "@outer(@inner {})";

    let first_iter = &mut ParseIterator::new(Span::new(first), false).peekable();
    let second_iter = &mut ParseIterator::new(Span::new(second), false).peekable();

    assert_eq!(normalise(first_iter), normalise(second_iter));

    let first = "@foo({one: 1, two: @bar(1,2,3), three: 3, four: {@baz({1,2})}})";
    let second = "@foo(one: 1, two: @bar({1,2,3}), three: 3, four: {@baz(1,2)})";

    let first_iter = &mut ParseIterator::new(Span::new(first), false).peekable();
    let second_iter = &mut ParseIterator::new(Span::new(second), false).peekable();

    assert_eq!(normalise(first_iter), normalise(second_iter));

    let first = "@foo(1,2)";
    let second = "@foo({1,2})";

    let first_iter = &mut ParseIterator::new(Span::new(first), false).peekable();
    let second_iter = &mut ParseIterator::new(Span::new(second), false).peekable();

    assert_eq!(normalise(first_iter), normalise(second_iter));

    let first = "@foo({one: 1, two: @bar(1,2,3)})";
    let second = "@foo(one: 1, two: @bar({1,2,3}))";

    let first_iter = &mut ParseIterator::new(Span::new(first), false).peekable();
    let second_iter = &mut ParseIterator::new(Span::new(second), false).peekable();

    assert_eq!(normalise(first_iter), normalise(second_iter));

    let first = "@name(@foo,@bar)";
    let second = "@name({@foo, @bar})";

    let first_iter = &mut ParseIterator::new(Span::new(first), false).peekable();
    let second_iter = &mut ParseIterator::new(Span::new(second), false).peekable();

    assert_eq!(normalise(first_iter), normalise(second_iter));

    let first = "@name(1, @foo@bar)";
    let second = "@name({1, @foo@bar})";

    let first_iter = &mut ParseIterator::new(Span::new(first), false).peekable();
    let second_iter = &mut ParseIterator::new(Span::new(second), false).peekable();

    assert_eq!(normalise(first_iter), normalise(second_iter));

    let first = "@name(@foo@bar({@bar, 1, @baz()}), @bar@foo()@baz)";
    let second = "@name({@foo@bar(@bar, 1, @baz()), @bar@foo()@baz})";

    let first_iter = &mut ParseIterator::new(Span::new(first), false).peekable();
    let second_iter = &mut ParseIterator::new(Span::new(second), false).peekable();

    assert_eq!(normalise(first_iter), normalise(second_iter));

    let first = "@foo(1, @bar(2,3), 4, 5)";
    let second = "@foo(1, @bar({2,3}), 4, 5)";

    let first_iter = &mut ParseIterator::new(Span::new(first), false).peekable();
    let second_iter = &mut ParseIterator::new(Span::new(second), false).peekable();

    assert_eq!(normalise(first_iter), normalise(second_iter));

    let first = "@name(a: @foo)";
    let second = "@name({a: @foo})";

    let first_iter = &mut ParseIterator::new(Span::new(first), false).peekable();
    let second_iter = &mut ParseIterator::new(Span::new(second), false).peekable();

    assert_eq!(normalise(first_iter), normalise(second_iter));

    let first = "@name(b: @foo@bar@baz, @foo(1,2), @bar@baz))";
    let second = "@name({b: @foo@bar@baz, @foo({1,2}), @bar@baz})";

    let first_iter = &mut ParseIterator::new(Span::new(first), false).peekable();
    let second_iter = &mut ParseIterator::new(Span::new(second), false).peekable();

    assert_eq!(normalise(first_iter), normalise(second_iter));

    let first = "@name(b: )";
    let second = "@name({b: })";

    let first_iter = &mut ParseIterator::new(Span::new(first), false).peekable();
    let second_iter = &mut ParseIterator::new(Span::new(second), false).peekable();

    assert_eq!(normalise(first_iter), normalise(second_iter));
}
