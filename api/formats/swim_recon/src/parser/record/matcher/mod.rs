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
use std::str::Utf8Error;

use super::super::tokens::complete::{blob, identifier, numeric_literal};
use super::super::tokens::string_literal;
use nom::branch::alt;
use nom::character::complete::{char, multispace0, one_of, space0};
use nom::combinator::{complete, flat_map, map, map_res, opt, recognize, rest, success};
use nom::multi::{fold_many0, many0_count, many1_count};
use nom::sequence::{delimited, pair, preceded, terminated, tuple};
use nom::{Finish, IResult};
use thiserror::Error;

use crate::parser::Span;

#[cfg(test)]
mod tests;

/// Implementers of this trait attempt to recognize a particular pattern in the first attribute of
/// a Recon value. It is necessary for such types to be clonable (for use in different brancehs)
/// of the parser and cloned instances should behave identically to the original. As clone could
/// be called several times, this operation should be cheap (generally implementations should, in
/// fact be [`Copy`]).
pub trait HeaderPeeler<'a>: Clone {
    /// The type of results produced after recognizing an attribute.
    type Output: 'a;
    /// Possible failure cases from attempting to interpret the attribute.
    type Error: Clone + 'a;

    /// Provide the name of the tag attribute.
    ///
    /// #Arguments
    /// * `name` - The name of the tag. Note that this has a more restrictive lifetime as may
    /// have been unescaped by the parser.
    fn tag(self, name: &str) -> Result<Self, Self::Error>;

    /// Feed a slot from the body of the attribute.
    ///
    /// #Arguments
    /// * `name` - The name of the slot. Note that this has a more restrictive lifetime as may
    /// have been unescaped by the parser.
    /// * `value` - Span of the input containing the Recon of the value of the slot.
    fn feed_header_slot(self, name: &str, value: Span<'a>) -> Result<Self, Self::Error>;

    /// Feed a value item from the body of the attribute.
    ///
    /// #Arguments
    /// * `value` - Span of the input containing the Recon of the item
    fn feed_header_value(self, value: Span<'a>) -> Result<Self, Self::Error>;

    /// Feed an empty item from the body of the attribute. (Note that the result of recognizing
    /// "@attr" will be different from "@attr()").
    fn feed_header_extant(self) -> Result<Self, Self::Error>;

    /// Attempt to interpret the attribute.
    ///
    /// #Arguments
    /// * `body` - The remainder of the input span. Note that this is entirely uninterpreted and
    /// so may not contain valid Recon.
    fn done(self, body: Span<'a>) -> Result<Self::Output, Self::Error>;
}

/// Run an implementation of [`HeaderPeeler`] against an array of bytes.
pub fn try_extract_header<'a, P>(
    bytes: &'a [u8],
    peeler: P,
) -> Result<P::Output, MessageExtractError>
where
    P: HeaderPeeler<'a>,
{
    let text = std::str::from_utf8(bytes)?;
    let span = Span::new(text);
    match peel_message(peeler)(span).finish() {
        Ok((_, output)) => Ok(output),
        Err(e) => Err(MessageExtractError::ParseError(e.to_string())),
    }
}

fn peel_message<'a, P>(
    peeler: P,
) -> impl FnMut(Span<'a>) -> IResult<Span<'a>, <P as HeaderPeeler<'a>>::Output>
where
    P: HeaderPeeler<'a>,
{
    move |input| {
        map_res(
            pair(peel_tag_attr(peeler.clone()), preceded(space0, rest)),
            |(p, rem)| p.done(rem),
        )(input)
    }
}

fn value(input: Span<'_>) -> IResult<Span<'_>, Span<'_>> {
    alt((
        recognize(string_literal),
        recognize(identifier),
        recognize(numeric_literal),
        recognize(blob),
        record,
    ))(input)
}

fn record(input: Span<'_>) -> IResult<Span<'_>, Span<'_>> {
    alt((
        recognize(pair(attrs, body_after_attrs)),
        recognize(body(('{', '}'))),
    ))(input)
}

fn attrs(input: Span<'_>) -> IResult<Span<'_>, Span<'_>> {
    recognize(many1_count(pair(attr, space0)))(input)
}

fn attr_name(input: Span<'_>) -> IResult<Span<'_>, Cow<'_, str>> {
    alt((string_literal, map(identifier, Cow::Borrowed)))(input)
}

fn attr(input: Span<'_>) -> IResult<Span<'_>, Span<'_>> {
    recognize(tuple((char('@'), attr_name, opt(body(('(', ')'))))))(input)
}

fn body(delim: (char, char)) -> impl for<'a> FnMut(Span<'a>) -> IResult<Span<'a>, Span<'a>> {
    let (l, r) = delim;
    move |input| {
        delimited(
            pair(char(l), multispace0),
            items,
            pair(multispace0, char(r)),
        )(input)
    }
}

fn items(input: Span<'_>) -> IResult<Span<'_>, Span<'_>> {
    recognize(pair(opt(item), many0_count(preceded(separator, opt(item)))))(input)
}

fn separator(input: Span<'_>) -> IResult<Span<'_>, Span<'_>> {
    recognize(delimited(space0, one_of(",;\n\r"), multispace0))(input)
}

fn slot_div(input: Span<'_>) -> IResult<Span<'_>, Span<'_>> {
    recognize(delimited(multispace0, char(':'), multispace0))(input)
}

fn item(input: Span<'_>) -> IResult<Span<'_>, Span<'_>> {
    recognize(pair(value, opt(preceded(slot_div, opt(value)))))(input)
}

fn body_after_attrs(input: Span<'_>) -> IResult<Span<'_>, Span<'_>> {
    recognize(opt(alt((
        recognize(complete(string_literal)),
        recognize(identifier),
        recognize(numeric_literal),
        recognize(blob),
        body(('{', '}')),
    ))))(input)
}

fn name(input: Span<'_>) -> IResult<Span<'_>, Cow<'_, str>> {
    alt((map(identifier, Cow::Borrowed), string_literal))(input)
}

fn match_tag<'a, P>(peeler: P) -> impl FnMut(Span<'a>) -> IResult<Span<'a>, P>
where
    P: HeaderPeeler<'a>,
{
    move |input| {
        map_res(preceded(char('@'), name), |tag| {
            let p = peeler.clone();
            p.tag(&tag)
        })(input)
    }
}

#[derive(Debug)]
enum PeelItem<'a> {
    ValueItem(Span<'a>),
    SlotItem(Cow<'a, str>, Span<'a>),
}

fn peel_value_item(input: Span<'_>) -> IResult<Span<'_>, PeelItem<'_>> {
    map(recognize(value), PeelItem::ValueItem)(input)
}

fn peel_slot_item(input: Span<'_>) -> IResult<Span<'_>, PeelItem<'_>> {
    map(
        pair(name, preceded(slot_div, recognize(opt(value)))),
        |(name, v)| PeelItem::SlotItem(name, v),
    )(input)
}

fn peel_item(input: Span<'_>) -> IResult<Span<'_>, PeelItem<'_>> {
    alt((peel_slot_item, peel_value_item))(input)
}

#[derive(Debug)]
struct SepPeelItem<'a> {
    item: Option<PeelItem<'a>>,
    after_sep: bool,
}

impl<'a> SepPeelItem<'a> {
    fn new(item: Option<PeelItem<'a>>, after_sep: bool) -> Self {
        SepPeelItem { item, after_sep }
    }
}

fn peel_item_with_sep(input: Span<'_>) -> IResult<Span<'_>, SepPeelItem<'_>> {
    preceded(
        multispace0,
        alt((
            map(
                terminated(opt(peel_item), pair(space0, one_of(",;"))),
                |item| SepPeelItem::new(item, true),
            ),
            map(
                terminated(peel_item, pair(space0, one_of("\r\n"))),
                |item| SepPeelItem::new(Some(item), false),
            ),
        )),
    )(input)
}

fn peel_final_item<'a, P>(
    peeler: P,
    terminated: bool,
) -> impl FnMut(Span<'a>) -> IResult<Span<'a>, P>
where
    P: HeaderPeeler<'a>,
{
    move |input| {
        let peeler_ref = &peeler;
        map_res(preceded(multispace0, opt(peel_item)), move |maybe| {
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

fn peel_items<'a, P>(peeler: P) -> impl FnMut(Span<'a>) -> IResult<Span<'a>, P>
where
    P: HeaderPeeler<'a>,
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

        terminated(
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

fn peel_tag_attr<'a, P>(peeler: P) -> impl FnMut(Span<'a>) -> IResult<Span<'a>, P>
where
    P: HeaderPeeler<'a>,
{
    move |input| {
        flat_map(match_tag(peeler.clone()), |p| {
            alt((
                delimited(char('('), peel_items(p.clone()), char(')')),
                success(p),
            ))
        })(input)
    }
}

/// Errors that can occur attempting to parse the header of a Recon value.
#[derive(Error, Debug)]
pub enum MessageExtractError {
    #[error("Data contains invalid UTF8: {0}")]
    BadUtf8(#[from] Utf8Error),
    #[error("Parse error in header: {0}")]
    ParseError(String),
}
