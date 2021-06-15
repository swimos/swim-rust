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

use crate::form::structural::read::improved::primitive::I32Recognizer;
use crate::form::structural::read::improved::{Recognizer, VecRecognizer};
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

fn example_select<'a, S, RS: Recognizer<Target = S>, T, RT: Recognizer<Target = T>>(
    state: &mut (Option<S>, Option<T>, RS, RT),
    index: u32,
    input: ParseEvent<'a>,
) -> Option<Result<(), ReadError>> {
    let (first, second, first_rec, second_rec) = state;
    match index {
        0 => {
            if first.is_some() {
                Some(Err(ReadError::DuplicateField(Text::new("first"))))
            } else {
                let r = first_rec.feed_event(input)?;
                match r {
                    Ok(s) => {
                        *first = Some(s);
                        Some(Ok(()))
                    }
                    Err(e) => Some(Err(e)),
                }
            }
        }
        1 => {
            if second.is_some() {
                Some(Err(ReadError::DuplicateField(Text::new("second"))))
            } else {
                let r = second_rec.feed_event(input)?;
                match r {
                    Ok(t) => {
                        *second = Some(t);
                        Some(Ok(()))
                    }
                    Err(e) => Some(Err(e)),
                }
            }
        }
        _ => Some(Err(ReadError::InconsistentState)),
    }
}

fn example_construct<
    S: RecognizerReadable,
    RS: Recognizer<Target = S>,
    T: RecognizerReadable,
    RT: Recognizer<Target = T>,
>(
    state: &mut (Option<S>, Option<T>, RS, RT),
) -> Result<Example<S, T>, ReadError> {
    let (first, second, first_rec, second_rec) = state;
    first_rec.reset();
    second_rec.reset();
    match (
        first.take().or_else(|| S::on_absent()),
        second.take().or_else(|| T::on_absent()),
    ) {
        (Some(first), Some(second)) => Ok(Example { first, second }),
        (Some(_), _) => Err(ReadError::MissingFields(vec![Text::new("second")])),
        (_, Some(_)) => Err(ReadError::MissingFields(vec![Text::new("first")])),
        _ => Err(ReadError::MissingFields(vec![
            Text::new("first"),
            Text::new("second"),
        ])),
    }
}

fn example_reset<
    S: RecognizerReadable,
    RS: Recognizer<Target = S>,
    T: RecognizerReadable,
    RT: Recognizer<Target = T>,
>(
    state: &mut (Option<S>, Option<T>, RS, RT),
) {
    let (first, second, first_rec, second_rec) = state;
    *first = None;
    *second = None;
    first_rec.reset();
    second_rec.reset();
}

type ExampleFields<S, T> = (
    Option<S>,
    Option<T>,
    <S as RecognizerReadable>::Rec,
    <T as RecognizerReadable>::Rec,
);
type ExampleRec<S, T> = NamedFieldsRecognizer<Example<S, T>, ExampleFields<S, T>>;
type ExampleAttrRec<S, T> = FirstOf<ExampleRec<S, T>, SimpleAttrBody<ExampleRec<S, T>>>;

impl<S: RecognizerReadable, T: RecognizerReadable> RecognizerReadable for Example<S, T> {
    type Rec = ExampleRec<S, T>;
    type AttrRec = ExampleAttrRec<S, T>;

    fn make_recognizer() -> Self::Rec {
        NamedFieldsRecognizer::new(
            (None, None, S::make_recognizer(), T::make_recognizer()),
            example_select_field,
            2,
            example_select,
            example_construct,
            example_reset,
        )
    }

    fn make_attr_recognizer() -> Self::AttrRec {
        let option1 = NamedFieldsRecognizer::new_attr(
            (None, None, S::make_recognizer(), T::make_recognizer()),
            example_select_field,
            2,
            example_select,
            example_construct,
            example_reset,
        );

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
        ParseEvent::EndRecord,
    ];

    let rec = run_recognizer(events, rec);
    assert_eq!(
        rec,
        Example {
            first: 4,
            second: 8
        }
    );
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

pub enum ExampleEnum<S, T> {
    First { a: S, b: S },
    Second(T, T),
}

type FirstFlds<S> = (Option<S>, Option<S>, <S as RecognizerReadable>::Rec, <S as RecognizerReadable>::Rec);
type SecondFlds<T> = (Option<T>, Option<T>, <T as RecognizerReadable>::Rec, <T as RecognizerReadable>::Rec);
type FirstRecog<S, T> = LabelledStructRecognizer<ExampleEnum<S, T>, FirstFlds<S>>;
type SecondRecog<S, T> = OrdinalStructRecognizer<ExampleEnum<S, T>, SecondFlds<T>>;
type Variants<S, T> = CCons<FirstRecog<S, T>, CCons<SecondRecog<S, T>, CNil>>;

fn enum_select_index_1(key: LabelledFieldKey<'_>) -> Option<u32> {
    match key {
        LabelledFieldKey::Item("a") => Some(0),
        LabelledFieldKey::Item("b") => Some(1),
        _ => None
    }
}

fn enum_select_index_2(key: OrdinalFieldKey<'_>) -> Option<u32> {
    match key {
        OrdinalFieldKey::FirstItem => Some(0),
        _ => None
    }
}

fn enum_select_feed_1<'a, S, RS: Recognizer<Target = S>>(
    state: &mut (Option<S>, Option<S>, RS, RS),
    index: u32,
    input: ParseEvent<'a>) -> Option<Result<(), ReadError>> {
    let (a, b, a_rec, b_rec) = state;
    match index {
        0 => {
            match a_rec.feed_event(input) {
                Some(Ok(s)) => {
                    *a = Some(s);
                    Some(Ok(()))
                }
                Some(Err(e)) => Some(Err(e)),
                _ => None,
            }
        }
        1 => {
            match b_rec.feed_event(input) {
                Some(Ok(s)) => {
                    *b = Some(s);
                    Some(Ok(()))
                }
                Some(Err(e)) => Some(Err(e)),
                _ => None,
            }
        }
        _ => Some(Err(ReadError::InconsistentState)),
    }
}


fn enum_select_feed_2<'a, T, RT: Recognizer<Target = T>>(
    state: &mut (Option<T>, Option<T>, RT, RT),
    index: u32,
    input: ParseEvent<'a>) -> Option<Result<(), ReadError>> {
    let (a, b, a_rec, b_rec) = state;
    match index {
        0 => {
            match a_rec.feed_event(input) {
                Some(Ok(t)) => {
                    *a = Some(t);
                    Some(Ok(()))
                }
                Some(Err(e)) => Some(Err(e)),
                _ => None,
            }
        }
        1 => {
            match b_rec.feed_event(input) {
                Some(Ok(t)) => {
                    *b = Some(t);
                    Some(Ok(()))
                }
                Some(Err(e)) => Some(Err(e)),
                _ => None,
            }
        }
        _ => Some(Err(ReadError::InconsistentState)),
    }
}

fn enum_on_done_1<S, T, RS: Recognizer<Target = S>>(state: &mut (Option<S>, Option<S>, RS, RS)) -> Result<ExampleEnum<S, T>, ReadError> {
    match (state.0.take(), state.1.take()) {
        (Some(a), Some(b)) => Ok(ExampleEnum::First { a, b }),
        (Some(_), _) => Err(ReadError::MissingFields(vec![Text::new("b")])),
        (_, Some(_)) => Err(ReadError::MissingFields(vec![Text::new("a")])),
        _ => Err(ReadError::MissingFields(vec![Text::new("a"), Text::new("b")])),
    }
}

fn enum_on_done_2<S, T, RT: Recognizer<Target = T>>(state: &mut (Option<T>, Option<T>, RT, RT)) -> Result<ExampleEnum<S, T>, ReadError> {
    match (state.0.take(), state.1.take()) {
        (Some(a), Some(b)) => Ok(ExampleEnum::Second(a, b)),
        (Some(_), _) => Err(ReadError::MissingFields(vec![Text::new("value_0")])),
        _ => Err(ReadError::MissingFields(vec![Text::new("value_0"), Text::new("value_1")])),
    }
}

fn enum_on_reset_1<S, RS: Recognizer<Target = S>>(state: &mut (Option<S>, Option<S>, RS, RS)) {
    state.0 = None;
    state.1 = None;
    state.2.reset();
    state.3.reset();
}

fn enum_on_reset_2<T, RT: Recognizer<Target = T>>(state: &mut (Option<T>, Option<T>, RT, RT)) {
    state.0 = None;
    state.1 = None;
    state.2.reset();
    state.3.reset();
}

fn enum_select_var<S: RecognizerReadable, T: RecognizerReadable>(name: &str) -> Option<Variants<S, T>> {
    match name {
        "First" => {
            let flds = (None, None, S::make_recognizer(), S::make_recognizer());
            let recog = LabelledStructRecognizer::variant(false, flds, 2, enum_select_index_1, enum_select_feed_1, enum_on_done_1, enum_on_reset_1);
            Some(CCons::Head(recog))
        }
        "Second" => {
            let flds = (None, None, T::make_recognizer(), T::make_recognizer());
            let recog = OrdinalStructRecognizer::variant(false, flds, 2, enum_select_index_2, enum_select_feed_2, enum_on_done_2, enum_on_reset_2);
            Some(CCons::Tail(CCons::Head(recog)))
        },
        _ => None
    }
}

impl<S: RecognizerReadable, T: RecognizerReadable> RecognizerReadable for ExampleEnum<S, T> {
    type Rec = TaggedEnumRecognizer<Variants<S, T>>;
    type AttrRec = SimpleAttrBody<TaggedEnumRecognizer<Variants<S, T>>>;

    fn make_recognizer() -> Self::Rec {
        TaggedEnumRecognizer::new(enum_select_var)
    }

    fn make_attr_recognizer() -> Self::AttrRec {
        SimpleAttrBody::new(Self::make_recognizer())
    }
}

