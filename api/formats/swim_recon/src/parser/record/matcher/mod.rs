// Copyright 2015-2021 Swim Inc.
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

use std::borrow::Cow;

use super::super::tokens::complete::{blob, identifier, numeric_literal};
use super::super::tokens::string_literal;
use super::attr_name;
use nom::branch::alt;
use nom::character::complete::{char, multispace0, one_of, space0};
use nom::combinator::{flat_map, map, map_res, opt, recognize, rest};
use nom::multi::{fold_many0, many0_count, many1_count};
use nom::sequence::{delimited, pair, preceded, terminated, tuple};
use nom::IResult;

use crate::parser::Span;

pub trait HeaderPeeler<'a>: Clone {
    type Output: 'a;
    type Error: Clone + 'a;

    fn tag(self, name: &str) -> Result<Self, Self::Error>;

    fn feed_header_slot(self, name: &str, value: Span<'a>) -> Result<Self, Self::Error>;

    fn feed_header_value(self, value: Span<'a>) -> Result<Self, Self::Error>;

    fn feed_header_extant(self) -> Result<Self, Self::Error>;

    fn done(self, body: Span<'a>) -> Result<Self::Output, Self::Error>;
}

pub fn peel_message<P>(
    peeler: P,
) -> impl for<'a> FnMut(Span<'a>) -> IResult<Span<'a>, <P as HeaderPeeler<'a>>::Output>
where
    P: for<'a> HeaderPeeler<'a>,
{
    move |input| {
        map_res(pair(peel_tag_attr(peeler.clone()), rest), |(p, rem)| {
            p.done(rem)
        })(input)
    }
}

fn value<'a>(input: Span<'a>) -> IResult<Span<'a>, Span<'a>> {
    alt((
        recognize(string_literal),
        recognize(identifier),
        recognize(numeric_literal),
        recognize(blob),
        record,
    ))(input)
}

fn record<'a>(input: Span<'a>) -> IResult<Span<'a>, Span<'a>> {
    alt((recognize(pair(attrs, body_after_attrs)), standalone_body))(input)
}

fn attrs<'a>(input: Span<'a>) -> IResult<Span<'a>, Span<'a>> {
    recognize(many1_count(pair(attr, space0)))(input)
}

fn attr<'a>(input: Span<'a>) -> IResult<Span<'a>, Span<'a>> {
    recognize(tuple((char('@'), attr_name, opt(attr_body))))(input)
}

fn attr_body<'a>(input: Span<'a>) -> IResult<Span<'a>, Span<'a>> {
    delimited(
        pair(char('('), multispace0),
        items,
        pair(multispace0, char(')')),
    )(input)
}

fn items<'a>(input: Span<'a>) -> IResult<Span<'a>, Span<'a>> {
    recognize(pair(opt(item), many0_count(preceded(separator, opt(item)))))(input)
}

fn separator<'a>(input: Span<'a>) -> IResult<Span<'a>, Span<'a>> {
    recognize(delimited(space0, one_of(",;\n\r"), multispace0))(input)
}

fn slot_div<'a>(input: Span<'a>) -> IResult<Span<'a>, Span<'a>> {
    recognize(delimited(multispace0, char(':'), multispace0))(input)
}

fn item<'a>(input: Span<'a>) -> IResult<Span<'a>, Span<'a>> {
    recognize(pair(value, opt(preceded(slot_div, opt(value)))))(input)
}

fn body_after_attrs<'a>(input: Span<'a>) -> IResult<Span<'a>, Span<'a>> {
    recognize(opt(alt((
        recognize(string_literal),
        recognize(identifier),
        recognize(numeric_literal),
        recognize(blob),
        standalone_body,
    ))))(input)
}

fn standalone_body<'a>(input: Span<'a>) -> IResult<Span<'a>, Span<'a>> {
    delimited(char('{'), items, char('}'))(input)
}

fn name<'a>(input: Span<'a>) -> IResult<Span<'a>, Cow<'a, str>> {
    alt((map(identifier, Cow::Borrowed), string_literal))(input)
}

fn match_tag<P>(peeler: P) -> impl for<'a> FnMut(Span<'a>) -> IResult<Span<'a>, P>
where
    P: for<'a> HeaderPeeler<'a>,
{
    move |input| {
        map_res(preceded(char('@'), name), |tag| {
            let p = peeler.clone();
            p.tag(&tag)
        })(input)
    }
}

enum PeelItem<'a> {
    ValueItem(Span<'a>),
    SlotItem(Cow<'a, str>, Span<'a>),
}

fn peel_value_item<'a>(input: Span<'a>) -> IResult<Span<'a>, PeelItem<'a>> {
    map(recognize(value), PeelItem::ValueItem)(input)
}

fn peel_slot_item<'a>(input: Span<'a>) -> IResult<Span<'a>, PeelItem<'a>> {
    map(
        pair(name, preceded(slot_div, recognize(opt(value)))),
        |(name, v)| PeelItem::SlotItem(name, v),
    )(input)
}

fn peel_item<'a>(input: Span<'a>) -> IResult<Span<'a>, PeelItem<'a>> {
    alt((peel_slot_item, peel_value_item))(input)
}

struct SepPeelItem<'a> {
    item: Option<PeelItem<'a>>,
    after_sep: bool,
}

impl<'a> SepPeelItem<'a> {
    fn new(item: Option<PeelItem<'a>>, after_sep: bool) -> Self {
        SepPeelItem { item, after_sep }
    }
}

fn peel_item_with_sep<'a>(input: Span<'a>) -> IResult<Span<'a>, SepPeelItem<'a>> {
    alt((
        map(
            terminated(opt(peel_item), pair(space0, one_of(",;"))),
            |item| SepPeelItem::new(item, true),
        ),
        map(
            terminated(peel_item, pair(space0, one_of("\r\n"))),
            |item| SepPeelItem::new(Some(item), false),
        ),
    ))(input)
}

fn peel_final_item<P>(
    peeler: P,
    terminated: bool,
) -> impl for<'a> FnMut(Span<'a>) -> IResult<Span<'a>, P>
where
    P: for<'a> HeaderPeeler<'a>,
{
    move |input| {
        let peeler_ref = &peeler;
        map_res(opt(peel_item), move |maybe| {
            let p = (*peeler_ref).clone();
            if let Some(item) = maybe {
                apply_item(p, item)
            } else if terminated {
                p.feed_header_extant()
            } else {
                Ok(p)
            }
        })(input)
    }
}

pub fn peel_items<P>(peeler: P) -> impl for<'a> FnMut(Span<'a>) -> IResult<Span<'a>, P>
where
    P: for<'a> HeaderPeeler<'a>,
{
    move |input| {
        let peeler_ref = &peeler;
        let sequence_with_res = fold_many0(
            peel_item_with_sep,
            move || (Ok((*peeler_ref).clone()), true),
            |(p, _), item| {
                let SepPeelItem { item, after_sep } = item;
                let updated = p.and_then(move |peeler| {
                    if let Some(item) = item {
                        apply_item(peeler, item)
                    } else {
                        peeler.feed_header_extant()
                    }
                });
                (updated, after_sep)
            },
        );

        let sequence = map_res(sequence_with_res, |(r, after_sep)| {
            r.map(|p| (p, after_sep))
        });

        delimited(
            multispace0,
            flat_map(sequence, |(p, after_sep)| peel_final_item(p, after_sep)),
            multispace0,
        )(input)
    }
}

fn apply_item<'a, P>(peeler: P, item: PeelItem<'a>) -> Result<P, P::Error>
where
    P: HeaderPeeler<'a>,
{
    match item {
        PeelItem::ValueItem(v) => peeler.feed_header_value(v),
        PeelItem::SlotItem(name, v) => peeler.feed_header_slot(&name, v),
    }
}

fn peel_tag_attr<P>(peeler: P) -> impl for<'a> FnMut(Span<'a>) -> IResult<Span<'a>, P>
where
    P: for<'a> HeaderPeeler<'a>,
{
    move |input| {
        flat_map(match_tag(peeler.clone()), |p| {
            delimited(char('('), peel_items(p), char(')'))
        })(input)
    }
}
