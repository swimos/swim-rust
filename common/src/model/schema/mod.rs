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

use crate::model::{Attr, Item, Value, ValueKind};
use regex::{Error as RegexError, Regex};
use std::borrow::Borrow;
use std::collections::HashSet;

#[cfg(test)]
mod tests;

pub trait Schema<T> {
    fn matches(&self, value: &T) -> bool;
}

#[derive(Clone, Debug)]
pub enum TextSchema {
    NonEmpty,
    Exact(String),
    Matches(Regex),
}

impl TextSchema {
    pub fn exact(string: &str) -> TextSchema {
        TextSchema::Exact(string.to_string())
    }

    pub fn regex(string: &str) -> Result<TextSchema, RegexError> {
        Regex::new(string).map(TextSchema::Matches)
    }
}

impl PartialEq for TextSchema {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (TextSchema::NonEmpty, TextSchema::NonEmpty) => true,
            (TextSchema::Exact(left), TextSchema::Exact(right)) => left == right,
            (TextSchema::Matches(left), TextSchema::Matches(right)) => {
                left.as_str() == right.as_str()
            }
            _ => false,
        }
    }
}

impl Eq for TextSchema {}

impl TextSchema {
    pub fn matches_str(&self, text: &str) -> bool {
        match self {
            TextSchema::NonEmpty => !text.is_empty(),
            TextSchema::Exact(s) => text == s,
            TextSchema::Matches(r) => r.is_match(text),
        }
    }

    pub fn matches_value(&self, value: &Value) -> bool {
        match value {
            Value::Text(text) => self.matches_str(text.borrow()),
            _ => false,
        }
    }
}

impl Schema<String> for TextSchema {
    fn matches(&self, value: &String) -> bool {
        self.matches_str(value.borrow())
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct AttrSchema {
    name_schema: TextSchema,
    value_schema: StandardSchema,
}

impl Schema<Attr> for AttrSchema {
    fn matches(&self, attr: &Attr) -> bool {
        self.name_schema.matches_str(attr.name.borrow()) && self.value_schema.matches(&attr.value)
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct SlotSchema {
    key_schema: StandardSchema,
    value_schema: StandardSchema,
}

impl Schema<Item> for SlotSchema {
    fn matches(&self, item: &Item) -> bool {
        match item {
            Item::ValueItem(_) => false,
            Item::Slot(key, value) => {
                self.key_schema.matches(key) && self.value_schema.matches(value)
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum ItemSchema {
    Field(SlotSchema),
    ValueItem(StandardSchema),
}

impl Schema<Item> for ItemSchema {
    fn matches(&self, item: &Item) -> bool {
        match self {
            ItemSchema::Field(slot_schema) => slot_schema.matches(item),
            ItemSchema::ValueItem(schema) => match item {
                Item::ValueItem(value) => schema.matches(value),
                Item::Slot(_, _) => false,
            },
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum Attributes {
    HasAttrs(Vec<AttrSchema>),
    AttrsInOrder(Vec<(AttrSchema, bool)>),
}

impl Attributes {
    pub fn matches(&self, attrs: &[Attr], exhaustive: bool) -> bool {
        match self {
            Attributes::HasAttrs(schemas) => check_contained(schemas, attrs, exhaustive),
            Attributes::AttrsInOrder(schemas) => check_in_order(schemas, attrs, exhaustive),
        }
    }
}

fn check_contained<T, S: Schema<T>>(schemas: &[S], items: &[T], exhaustive: bool) -> bool {
    let matched = schemas
        .iter()
        .try_fold(HashSet::new(), |mut matched, schema| {
            let matched_items = items
                .iter()
                .enumerate()
                .filter_map(
                    |(i, item)| {
                        if schema.matches(item) {
                            Some(i)
                        } else {
                            None
                        }
                    },
                )
                .collect::<Vec<_>>();
            if matched_items.is_empty() {
                None
            } else {
                matched_items.iter().for_each(|i| {
                    matched.insert(*i);
                });
                Some(matched)
            }
        });
    matched
        .map(|set| set.len() == items.len() || !exhaustive)
        .unwrap_or(false)
}

fn check_in_order<T, S: Schema<T>>(schemas: &[(S, bool)], items: &[T], exhaustive: bool) -> bool {
    let mut schema_it = schemas.iter();
    let mut item_it = items.iter();
    let mut pending = item_it.next();
    let matched = loop {
        if let Some((schema, mandatory)) = schema_it.next() {
            if *mandatory {
                match pending {
                    Some(item) => {
                        if !schema.matches(item) {
                            break false;
                        }
                    }
                    _ => {
                        break false;
                    }
                }
                pending = item_it.next();
            } else if let Some(item) = pending {
                if schema.matches(item) {
                    pending = item_it.next();
                }
            }
        } else {
            break match item_it.next() {
                Some(_) if !exhaustive => true,
                None => true,
                _ => false,
            };
        }
    };
    matched && (pending.is_none() || !exhaustive)
}

#[derive(Clone, Debug, PartialEq)]
pub enum Items {
    HasSlots(Vec<SlotSchema>),
    ItemsInOrder(Vec<(ItemSchema, bool)>),
}

impl Items {
    pub fn matches(&self, items: &[Item], exhaustive: bool) -> bool {
        match self {
            Items::HasSlots(schemas) => check_contained(schemas, items, exhaustive),
            Items::ItemsInOrder(schemas) => check_in_order(schemas, items, exhaustive),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct RecordLayout {
    attrs: Option<Attributes>,
    items: Option<Items>,
    exhaustive: bool,
}

impl RecordLayout {
    pub fn matches(&self, value: &Value) -> bool {
        match as_record(value) {
            Some((attrs, items)) => {
                let attrs_match = self
                    .attrs
                    .as_ref()
                    .map(|attr_schema| attr_schema.matches(attrs, self.exhaustive))
                    .unwrap_or(!self.exhaustive);
                let items_match = self
                    .items
                    .as_ref()
                    .map(|item_schema| item_schema.matches(items, self.exhaustive))
                    .unwrap_or(!self.exhaustive);
                attrs_match && items_match
            }
            _ => false,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum StandardSchema {
    OfKind(ValueKind),
    Equal(Value),
    InRangeInt {
        min: Option<(i64, bool)>,
        max: Option<(i64, bool)>,
    },
    InRangeFloat {
        min: Option<(f64, bool)>,
        max: Option<(f64, bool)>,
    },
    NonNan,
    Finite,
    Text(TextSchema),
    Not(Box<StandardSchema>),
    And(Vec<StandardSchema>),
    Or(Vec<StandardSchema>),
    Layout(RecordLayout),
    AllItems(Box<ItemSchema>),
    NumAttrs(usize),
    NumItems(usize),
    Anything,
    Nothing,
}

impl Schema<Value> for StandardSchema {
    fn matches(&self, value: &Value) -> bool {
        match self {
            StandardSchema::OfKind(kind) => &value.kind() == kind,
            StandardSchema::InRangeInt { min, max } => in_int_range(value, min, max),
            StandardSchema::InRangeFloat { min, max } => in_float_range(value, min, max),
            StandardSchema::NonNan => as_f64(value).map(|x| !f64::is_nan(x)).unwrap_or(false),
            StandardSchema::Finite => as_f64(value).map(f64::is_finite).unwrap_or(false),
            StandardSchema::Not(p) => !p.matches(value),
            StandardSchema::And(ps) => ps.iter().all(|schema| schema.matches(value)),
            StandardSchema::Or(ps) => ps.iter().any(|schema| schema.matches(value)),
            StandardSchema::AllItems(schema) => as_record(value)
                .map(|(_, items)| items.iter().all(|item| schema.matches(item)))
                .unwrap_or(false),
            StandardSchema::NumAttrs(num_attrs) => as_record(value)
                .map(|(attrs, _)| attrs.len() == *num_attrs)
                .unwrap_or(false),
            StandardSchema::NumItems(num_items) => as_record(value)
                .map(|(_, items)| items.len() == *num_items)
                .unwrap_or(false),
            StandardSchema::Text(text_schema) => text_schema.matches_value(value),
            StandardSchema::Layout(layout) => layout.matches(value),
            StandardSchema::Anything => true,
            StandardSchema::Nothing => false,
            StandardSchema::Equal(v) => value == v,
        }
    }
}

impl StandardSchema {

    pub fn eq<T: Into<Value>>(value: T) -> Self {
        StandardSchema::Equal(value.into())
    }

    pub fn inclusive_int_range(min: i64, max: i64) -> Self {
        StandardSchema::InRangeInt { min: Some((min, true)), max: Some((max, true)) }
    }

    pub fn exclusive_int_range(min: i64, max: i64) -> Self {
        StandardSchema::InRangeInt { min: Some((min, false)), max: Some((max, false)) }
    }

    pub fn int_range(min: i64, max: i64) -> Self {
        StandardSchema::InRangeInt { min: Some((min, true)), max: Some((max, false)) }
    }

    pub fn until_int(n: i64, inclusive: bool) -> Self {
        StandardSchema::InRangeInt { min: None, max: Some((n, inclusive)) }
    }

    pub fn after_int(n: i64, inclusive: bool) -> Self {
        StandardSchema::InRangeInt { min: Some((n, inclusive)), max: None }
    }

    pub fn inclusive_float_range(min: f64, max: f64) -> Self {
        StandardSchema::InRangeFloat { min: Some((min, true)), max: Some((max, true)) }
    }

    pub fn exclusive_float_range(min: f64, max: f64) -> Self {
        StandardSchema::InRangeFloat { min: Some((min, false)), max: Some((max, false)) }
    }

    pub fn float_range(min: f64, max: f64) -> Self {
        StandardSchema::InRangeFloat { min: Some((min, true)), max: Some((max, false)) }
    }

    pub fn until_float(x: f64, inclusive: bool) -> Self {
        StandardSchema::InRangeFloat { min: None, max: Some((x, inclusive)) }
    }

    pub fn after_float(x: f64, inclusive: bool) -> Self {
        StandardSchema::InRangeFloat { min: Some((x, inclusive)), max: None }
    }

    pub fn negate(self) -> Self {
        StandardSchema::Not(Box::new(self))
    }

    pub fn and(self, other: Self) -> Self {
        StandardSchema::And(vec![self, other])
    }

    pub fn or(self, other: Self) -> Self {
        StandardSchema::Or(vec![self, other])
    }

}

fn as_i64(value: &Value) -> Option<i64> {
    match value {
        Value::Int32Value(n) => Some((*n).into()),
        Value::Int64Value(n) => Some(*n),
        _ => None,
    }
}

fn as_f64(value: &Value) -> Option<f64> {
    match value {
        Value::Float64Value(x) => Some(*x),
        _ => None,
    }
}

fn as_record(value: &Value) -> Option<(&[Attr], &[Item])> {
    match value {
        Value::Record(attrs, items) => Some((attrs, items)),
        _ => None,
    }
}

fn in_int_range(value: &Value, min: &Option<(i64, bool)>, max: &Option<(i64, bool)>) -> bool {
    match as_i64(&value) {
        Some(n) => in_range(n, min, max),
        _ => false,
    }
}

fn in_float_range(value: &Value, min: &Option<(f64, bool)>, max: &Option<(f64, bool)>) -> bool {
    match as_f64(&value) {
        Some(x) => in_range(x, min, max),
        _ => false,
    }
}

fn in_range<T: Copy + PartialOrd>(
    value: T,
    min: &Option<(T, bool)>,
    max: &Option<(T, bool)>,
) -> bool {
    let lower = min
        .map(|(lb, incl)| if incl { lb <= value } else { lb < value })
        .unwrap_or(true);
    let upper = max
        .map(|(ub, incl)| if incl { ub >= value } else { ub > value })
        .unwrap_or(true);
    lower && upper
}
