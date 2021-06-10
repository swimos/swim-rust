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

use crate::form::structural::read::improved::{VecRecognizer, Recognizer};
use crate::form::structural::read::improved::primitive::I32Recognizer;
use crate::form::structural::read::parser::ParseEvent;
use crate::form::structural::read::ReadError;

fn run_recognizer<T, R: Recognizer<T>>(events: Vec<ParseEvent<'_>>, mut rec: R) -> T {
    let mut result = None;
    for event in events.into_iter() {
        if result.is_none() {
            result = rec.feed(event);
        } else {
            panic!("Not all input was consumed.");
        }
    }
    if result.is_none() {
        result = rec.flush();
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
        ParseEvent::EndRecord
    ];

    let v = run_recognizer(events, rec);
    assert_eq!(v, vec![1, 2, 3]);
}

type OrdFields = (Option<i32>, Option<i32>);

fn select_ord(fields: &mut OrdFields, n: u32, event: ParseEvent<'_>) -> Option<Result<(), ReadError>> {
    match n {
        0 => {
            let mut rec = I32Recognizer;
            match rec.feed(event) {
                Some(Ok(m)) => {
                    fields.0 = Some(m);
                    Some(Ok(()))
                },
                Some(Err(e)) => Some(Err(e)),
                _ => None,
            }
        }
        1 => {
            let mut rec = I32Recognizer;
            match rec.feed(event) {
                Some(Ok(m)) => {
                    fields.1 = Some(m);
                    Some(Ok(()))
                },
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
    let rec: OrdinalFieldsRecognizer<(i32, i32), OrdFields> = OrdinalFieldsRecognizer::new(
        (None, None),
        2,
        select_ord,
        ord_done,
        ord_reset,
    );

    let events = vec![
        ParseEvent::StartBody,
        ParseEvent::from(1),
        ParseEvent::from(2),
        ParseEvent::EndRecord
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
            if let Some(r) = recognizer.feed(event) {
                break r;
            }
        } else {
            break Err(ReadError::IncompleteRecord)
        }
    }
}

#[test]
fn examples() {
    let events = vec![ParseEvent::Number(NumericLiteral::Int(1))];
    let r: Result<i32, ReadError> = read(events.into_iter());
    assert_eq!(r, Ok(1));
}

#[derive(Debug, PartialEq, Eq)]
pub struct Example<S, T> {
    pub first: S,
    pub second: T,
}

fn example_select_field(name: &str) -> Option<u32> {
    match name {
        "first" => Some(0),
        "second" => Some(1),
        _ => None,
    }
}

fn example_select<'a, S, RS: Recognizer<S>, T, RT: Recognizer<T>>(state: &mut (Option<S>, Option<T>, RS, RT), index: u32, input: ParseEvent<'a>) -> Option<Result<(), ReadError>> {
    let (first, second, first_rec, second_rec) = state;
    match index {
        0 => {
            if first.is_some() {
                Some(Err(ReadError::DuplicateField(Text::new("first"))))
            } else {
                let r = first_rec.feed(input)?;
                match r {
                    Ok(s) => {
                        *first = Some(s);
                        Some(Ok(()))
                    }
                    Err(e) => {
                        Some(Err(e))
                    }
                }
            }
        },
        1 => {
            if second.is_some() {
                Some(Err(ReadError::DuplicateField(Text::new("second"))))
            } else {
                let r = second_rec.feed(input)?;
                match r {
                    Ok(t) => {
                        *second = Some(t);
                        Some(Ok(()))
                    }
                    Err(e) => {
                        Some(Err(e))
                    }
                }
            }
        },
        _ => Some(Err(ReadError::InconsistentState)),
    }
}

fn example_construct<S: RecognizerReadable, RS: Recognizer<S>, T: RecognizerReadable, RT: Recognizer<T>>(state: &mut (Option<S>, Option<T>, RS, RT)) -> Result<Example<S, T>, ReadError> {
    let (first, second, first_rec, second_rec) = state;
    first_rec.reset();
    second_rec.reset();
    match (first.take().or_else(|| S::on_absent()), second.take().or_else(|| T::on_absent())) {
        (Some(first), Some(second)) => Ok(Example {first, second}),
        (Some(_), _) => Err(ReadError::MissingFields(vec![Text::new("second")])),
        (_, Some(_)) => Err(ReadError::MissingFields(vec![Text::new("first")])),
        _ => Err(ReadError::MissingFields(vec![Text::new("first"), Text::new("second")])),
    }
}

fn example_reset<S: RecognizerReadable, RS: Recognizer<S>, T: RecognizerReadable, RT: Recognizer<T>>(state: &mut (Option<S>, Option<T>, RS, RT)) {
    let (first, second, first_rec, second_rec) = state;
    *first = None;
    *second = None;
    first_rec.reset();
    second_rec.reset();
}

type ExampleFields<S, T> = (Option<S>, Option<T>, <S as RecognizerReadable>::Rec, <T as RecognizerReadable>::Rec);
type ExampleRec<S, T> = NamedFieldsRecognizer<Example<S, T>, ExampleFields<S, T>>;
type ExampleAttrRec<S, T> = FirstOf<Example<S, T>, ExampleRec<S, T>, SimpleAttrBody<Example<S, T>, ExampleRec<S, T>>>;

impl<S: RecognizerReadable, T: RecognizerReadable> RecognizerReadable for Example<S, T> {
    type Rec = ExampleRec<S, T>;
    type AttrRec = ExampleAttrRec<S, T>;

    fn make_recognizer() -> Self::Rec {
        NamedFieldsRecognizer::new((None, None, S::make_recognizer(), T::make_recognizer()),
                                   example_select_field, 2, example_select, example_construct, example_reset)
    }

    fn make_attr_recognizer() -> Self::AttrRec {
        let option1 = NamedFieldsRecognizer::new_attr((None, None, S::make_recognizer(), T::make_recognizer()),
                                                      example_select_field, 2, example_select, example_construct, example_reset);

        let option2 = SimpleAttrBody::new(Self::make_recognizer());
        FirstOf::new(option1, option2)
    }
}

#[test]
fn named_fields_recognizer() {

    let rec = <Example<i32, i32>>::make_recognizer();

    let events = vec![
        ParseEvent::StartBody,
        "first".into(),
        ParseEvent::Slot,
        4i32.into(),
        "second".into(),
        ParseEvent::Slot,
        8i32.into(),
        ParseEvent::EndRecord
    ];

    let rec = run_recognizer(events, rec);
    assert_eq!(rec, Example { first: 4, second: 8 });

}

#[test]
fn optional_recognizer() {
    let rec = <Option<i32>>::make_recognizer();

    let events_some = vec![
        3.into()
    ];

    assert_eq!(run_recognizer(events_some, rec), Some(3));

    let rec = <Option<i32>>::make_recognizer();

    let events_none = vec![
        ParseEvent::Extant
    ];

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
        ParseEvent::EndRecord
    ];

    let map = run_recognizer(events, rec);

    let mut expected = HashMap::new();
    expected.insert("first".to_string(), 4);
    expected.insert("second".to_string(), 8);
    expected.insert("third".to_string(), -12);

    assert_eq!(map, expected);
}