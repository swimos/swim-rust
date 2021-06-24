// Copyright 2015-2021 SWIM.AI inc.
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

use crate::form::structural::read::recognizer::primitive::I32Recognizer;
use crate::form::structural::read::recognizer::{Recognizer, VecRecognizer};
use crate::form::structural::read::parser::ParseEvent;
use crate::form::structural::read::ReadError;

fn run_recognizer<R: Recognizer>(events: Vec<ParseEvent<'_>>, mut rec: R) -> R::Target {
    let mut result = None;
    for event in events.into_iter() {
        if result.is_none() {
            result = rec.feed_event(event);
        } else {
            panic!("Not all input was consumed.");
        }
    }
    if result.is_none() {
        result = rec.try_flush();
    }
    match result {
        Some(Err(e)) => panic!("{}", e),
        Some(Ok(t)) => t,
        _ => panic!("Record not complete."),
    }
}

#[test]
fn vec_recognizer() {
    let rec: VecRecognizer<i32, I32Recognizer> = VecRecognizer::new(false, I32Recognizer);

    let events = vec![
        ParseEvent::StartBody,
        ParseEvent::from(1),
        ParseEvent::from(2),
        ParseEvent::from(3),
        ParseEvent::EndRecord,
    ];

    let v = run_recognizer(events, rec);
    assert_eq!(v, vec![1, 2, 3]);
}

type OrdFields = (Option<i32>, Option<i32>);

fn select_ord(
    fields: &mut OrdFields,
    n: u32,
    event: ParseEvent<'_>,
) -> Option<Result<(), ReadError>> {
    match n {
        0 => {
            let mut rec = I32Recognizer;
            match rec.feed_event(event) {
                Some(Ok(m)) => {
                    fields.0 = Some(m);
                    Some(Ok(()))
                }
                Some(Err(e)) => Some(Err(e)),
                _ => None,
            }
        }
        1 => {
            let mut rec = I32Recognizer;
            match rec.feed_event(event) {
                Some(Ok(m)) => {
                    fields.1 = Some(m);
                    Some(Ok(()))
                }
                Some(Err(e)) => Some(Err(e)),
                _ => None,
            }
        }
        _ => Some(Err(ReadError::UnexpectedItem)),
    }
}

fn ord_done(fields: &mut OrdFields) -> Result<(i32, i32), ReadError> {
    match fields {
        (Some(a), Some(b)) => Ok((*a, *b)),
        _ => Err(ReadError::IncompleteRecord),
    }
}

fn ord_reset(fields: &mut OrdFields) {
    fields.0 = None;
    fields.1 = None;
}

#[test]
fn ordinal_fields_recognizer() {
    let rec: OrdinalFieldsRecognizer<(i32, i32), OrdFields> =
        OrdinalFieldsRecognizer::new((None, None), 2, select_ord, ord_done, ord_reset);

    let events = vec![
        ParseEvent::StartBody,
        ParseEvent::from(1),
        ParseEvent::from(2),
        ParseEvent::EndRecord,
    ];

    let (a, b) = run_recognizer(events, rec);
    assert_eq!(a, 1);
    assert_eq!(b, 2);
}

use super::*;

fn read<'a, It, T>(mut it: It) -> Result<T, ReadError>
where
    It: Iterator<Item = ParseEvent<'a>> + 'a,
    T: RecognizerReadable,
{
    let mut recognizer = T::make_recognizer();

    loop {
        if let Some(event) = it.next() {
            if let Some(r) = recognizer.feed_event(event) {
                break r;
            }
        } else {
            break Err(ReadError::IncompleteRecord);
        }
    }
}

#[test]
fn examples() {
    let events = vec![ParseEvent::Number(NumericLiteral::Int(1))];
    let r: Result<i32, ReadError> = read(events.into_iter());
    assert_eq!(r, Ok(1));
}

#[test]
fn optional_recognizer() {
    let rec = <Option<i32>>::make_recognizer();

    let events_some = vec![3.into()];

    assert_eq!(run_recognizer(events_some, rec), Some(3));

    let rec = <Option<i32>>::make_recognizer();

    let events_none = vec![ParseEvent::Extant];

    assert_eq!(run_recognizer(events_none, rec), None);
}

#[test]
fn hash_map_recognizer() {
    let rec = <HashMap<String, i32>>::make_recognizer();

    let events = vec![
        ParseEvent::StartBody,
        "first".into(),
        ParseEvent::Slot,
        4i32.into(),
        "second".into(),
        ParseEvent::Slot,
        8i32.into(),
        "third".into(),
        ParseEvent::Slot,
        (-12i32).into(),
        ParseEvent::EndRecord,
    ];

    let map = run_recognizer(events, rec);

    let mut expected = HashMap::new();
    expected.insert("first".to_string(), 4);
    expected.insert("second".to_string(), 8);
    expected.insert("third".to_string(), -12);

    assert_eq!(map, expected);
}
