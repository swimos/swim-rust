// Copyright 2015-2020 SWIM.AI inc.
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

use crate::model::parser::ParseFailure;
use crate::model::parser::{parse_document, parse_document_iteratee};
use crate::model::{Attr, Item, Value};
use hamcrest2::assert_that;
use hamcrest2::prelude::*;
use utilities::iteratee::Iteratee;

type ReadDocument = fn(&str) -> Result<Vec<Item>, ParseFailure>;

fn run_document_iteratee(repr: &str) -> Result<Vec<Item>, ParseFailure> {
    match parse_document_iteratee()
        .transduce_into(repr.char_indices())
        .next()
    {
        Some(result) => result,
        _ => Err(ParseFailure::IncompleteRecord),
    }
}

fn empty_document(read_doc: ReadDocument) {
    assert_that!(read_doc(""), eq(Ok(vec![])));
    assert_that!(read_doc("    "), eq(Ok(vec![])));
    assert_that!(read_doc("\n"), eq(Ok(vec![])));
    assert_that!(read_doc(" \r\n "), eq(Ok(vec![])));
}

#[test]
fn parse_empty_document() {
    empty_document(parse_document);
}

#[test]
fn iteratee_empty_document() {
    empty_document(run_document_iteratee);
}

fn single_value_document(read_doc: ReadDocument) {
    assert_that!(read_doc("3"), eq(Ok(vec![Item::of(3)])));
    assert_that!(read_doc("name"), eq(Ok(vec![Item::of("name")])));
    assert_that!(
        read_doc("@name"),
        eq(Ok(vec![Item::of(Value::of_attr("name"))]))
    );
    assert_that!(
        read_doc("{1, 2, 3}"),
        eq(Ok(vec![Item::of(Value::from_vec(vec![1, 2, 3]))]))
    );
    let complex = Value::Record(
        vec![Attr::of(("name", 0))],
        vec![Item::slot("a", 1), Item::slot("b", 2)],
    );
    assert_that!(
        read_doc("@name(0){a:1, b:2}"),
        eq(Ok(vec![Item::of(complex.clone())]))
    );

    assert_that!(read_doc("3  "), eq(Ok(vec![Item::of(3)])));
    assert_that!(read_doc("3\n "), eq(Ok(vec![Item::of(3)])));
    assert_that!(
        read_doc(" @name(0){a:1, b:2} "),
        eq(Ok(vec![Item::of(complex.clone())]))
    );
    assert_that!(
        read_doc("@name(0){a:1, b:2}  \n  "),
        eq(Ok(vec![Item::of(complex.clone())]))
    );
}

#[test]
fn parse_single_value_document() {
    single_value_document(parse_document);
}

#[test]
fn iteratee_single_value_document() {
    single_value_document(run_document_iteratee);
}

fn single_slot_document(read_doc: ReadDocument) {
    assert_that!(read_doc("a:3"), eq(Ok(vec![Item::slot("a", 3)])));
    assert_that!(
        read_doc("\"a\":"),
        eq(Ok(vec![Item::slot("a", Value::Extant)]))
    );
    assert_that!(
        read_doc("my_slot:@name(1)"),
        eq(Ok(vec![Item::slot("my_slot", Value::of_attr(("name", 1)))]))
    );
    assert_that!(read_doc("  a :3   "), eq(Ok(vec![Item::slot("a", 3)])));
    assert_that!(read_doc("  a :   3   \n"), eq(Ok(vec![Item::slot("a", 3)])));
}

#[test]
fn parse_single_slot_document() {
    single_slot_document(parse_document);
}

#[test]
fn iteratee_single_slot_document() {
    single_slot_document(run_document_iteratee);
}

fn multiple_value_document(read_doc: ReadDocument) {
    assert_that!(
        read_doc("1, 2, hello"),
        eq(Ok(vec![Item::of(1), Item::of(2), Item::of("hello")]))
    );
    assert_that!(
        read_doc("simple,\n @medium,\n @complex(3) { a, b, c }"),
        eq(Ok(vec![
            Item::of("simple"),
            Item::of(Value::of_attr("medium")),
            Item::of(Value::Record(
                vec![Attr::of(("complex", 3))],
                vec![Item::of("a"), Item::of("b"), Item::of("c")]
            ))
        ]))
    );
}

#[test]
fn parse_multiple_value_document() {
    multiple_value_document(parse_document);
}

#[test]
fn iteratee_multiple_value_document() {
    multiple_value_document(run_document_iteratee);
}

fn mixed_document(read_doc: ReadDocument) {
    assert_that!(
        read_doc("1, name: 2, hello"),
        eq(Ok(vec![
            Item::of(1),
            Item::slot("name", 2),
            Item::of("hello")
        ]))
    );
    assert_that!(
        read_doc("first: simple,\n @medium,\n last: @complex(3) { a, b, c }"),
        eq(Ok(vec![
            Item::slot("first", "simple"),
            Item::of(Value::of_attr("medium")),
            Item::slot(
                "last",
                Value::Record(
                    vec![Attr::of(("complex", 3))],
                    vec![Item::of("a"), Item::of("b"), Item::of("c")]
                )
            )
        ]))
    );
}

#[test]
fn parse_mixed_document() {
    mixed_document(parse_document);
}

#[test]
fn iteratee_mixed_document() {
    mixed_document(run_document_iteratee);
}

fn no_extant_after_trailing_sep(read_doc: ReadDocument) {
    assert_that!(read_doc("3,"), eq(Ok(vec![Item::of(3)])));

    assert_that!(
        read_doc("name: true,\n"),
        eq(Ok(vec![Item::slot("name", true)]))
    );
}

#[test]
fn parse_no_extant_after_trailing_sep() {
    no_extant_after_trailing_sep(parse_document);
}

#[test]
fn iteratee_no_extant_after_trailing_sep() {
    no_extant_after_trailing_sep(run_document_iteratee);
}

fn fails_on_top_level_close(read_doc: ReadDocument) {
    assert_that!(read_doc("}"), err());

    assert_that!(read_doc(")"), err());

    assert_that!(read_doc("0 \n }"), err());

    assert_that!(read_doc("0 \n )"), err());

    assert_that!(
        read_doc("first: simple,\n @medium,\n last: @complex(3) { a, b, c }}"),
        err()
    );
}

#[test]
fn parse_fails_on_top_level_close() {
    fails_on_top_level_close(parse_document);
}

#[test]
fn iteratee_fails_on_top_level_close() {
    fails_on_top_level_close(run_document_iteratee);
}
