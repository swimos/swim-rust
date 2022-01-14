use crate::hasher::normalise;
use crate::parser::{ParseIterator, Span};

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
}
