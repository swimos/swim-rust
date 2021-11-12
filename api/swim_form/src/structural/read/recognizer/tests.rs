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

use crate::structural::read::event::{NumericValue, ReadEvent};
use crate::structural::read::recognizer::primitive::I32Recognizer;
use crate::structural::read::recognizer::{Recognizer, VecRecognizer};
use crate::structural::read::ReadError;

fn run_recognizer<R: Recognizer>(events: Vec<ReadEvent<'_>>, mut rec: R) -> R::Target {
    let mut result = None;
    for event in events.into_iter() {
        match result {
            None => {
                result = rec.feed_event(event);
            }
            Some(Err(e)) => {
                panic!("{:?}", e);
            }
            _ => {
                panic!("Not all input was consumed.");
            }
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
        ReadEvent::StartBody,
        ReadEvent::from(1),
        ReadEvent::from(2),
        ReadEvent::from(3),
        ReadEvent::EndRecord,
    ];

    let v = run_recognizer(events, rec);
    assert_eq!(v, vec![1, 2, 3]);
}

#[test]
fn vec_recognizer_flattened() {
    let rec: VecRecognizer<i32, I32Recognizer> = VecRecognizer::new(true, I32Recognizer);

    let events = vec![
        ReadEvent::from(1),
        ReadEvent::from(2),
        ReadEvent::from(3),
        ReadEvent::EndAttribute,
    ];

    let v = run_recognizer(events, rec);
    assert_eq!(v, vec![1, 2, 3]);
}

#[test]
fn vec_recognizer_attr_body() {
    let rec = <Vec<i32> as RecognizerReadable>::make_attr_recognizer();

    let events = vec![
        ReadEvent::from(1),
        ReadEvent::from(2),
        ReadEvent::from(3),
        ReadEvent::EndAttribute,
    ];

    let v = run_recognizer(events, rec);
    assert_eq!(v, vec![1, 2, 3]);

    let rec = <Vec<i32> as RecognizerReadable>::make_attr_recognizer();

    let events = vec![
        ReadEvent::StartBody,
        ReadEvent::from(1),
        ReadEvent::from(2),
        ReadEvent::from(3),
        ReadEvent::EndRecord,
        ReadEvent::EndAttribute,
    ];

    let v = run_recognizer(events, rec);
    assert_eq!(v, vec![1, 2, 3]);
}

use super::*;

fn read<'a, It, T>(mut it: It) -> Result<T, ReadError>
where
    It: Iterator<Item = ReadEvent<'a>> + 'a,
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
    let events = vec![ReadEvent::Number(NumericValue::Int(1))];
    let r: Result<i32, ReadError> = read(events.into_iter());
    assert_eq!(r, Ok(1));
}

#[test]
fn optional_recognizer() {
    let rec = <Option<i32>>::make_recognizer();

    let events_some = vec![3.into()];

    assert_eq!(run_recognizer(events_some, rec), Some(3));

    let rec = <Option<i32>>::make_recognizer();

    let events_none = vec![ReadEvent::Extant];

    assert_eq!(run_recognizer(events_none, rec), None);
}

#[test]
fn hash_map_recognizer() {
    let rec = <HashMap<String, i32>>::make_recognizer();

    let events = vec![
        ReadEvent::StartBody,
        "first".into(),
        ReadEvent::Slot,
        4i32.into(),
        "second".into(),
        ReadEvent::Slot,
        8i32.into(),
        "third".into(),
        ReadEvent::Slot,
        (-12i32).into(),
        ReadEvent::EndRecord,
    ];

    let map = run_recognizer(events, rec);

    let mut expected = HashMap::new();
    expected.insert("first".to_string(), 4);
    expected.insert("second".to_string(), 8);
    expected.insert("third".to_string(), -12);

    assert_eq!(map, expected);
}
