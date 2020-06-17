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

use crate::model::{Attr, Item, ToValue, Value, ValueKind};
use regex::{Error as RegexError, Regex};
use std::borrow::Borrow;
use std::cmp::Ordering;
use std::collections::HashSet;
use std::convert::TryFrom;
use std::fmt::{Display, Formatter};

#[cfg(test)]
mod tests;

/// A pattern against which values of a type can be tested.
pub trait Schema<T> {
    /// Determine if a value matches the schema.
    fn matches(&self, value: &T) -> bool;
}

/// The result of matching a field against a pair of schemas.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FieldMatchResult {
    /// The key schema matched but the value schema did not.
    KeyOnly,
    /// Both schemas matched.
    Both,
    /// The key schema matched and teh value schema was not tested.
    KeyFailed,
}

/// A schema for a field (either an attribute or a slot).
pub trait FieldSchema<T> {
    /// Determine whether the field matches.
    fn matches_field(&self, field: &T) -> FieldMatchResult;
}

impl<T, S: FieldSchema<T>> Schema<T> for S {
    fn matches(&self, value: &T) -> bool {
        self.matches_field(value) == FieldMatchResult::Both
    }
}

/// Schema for UTF8 strings.
#[derive(Clone, Debug)]
pub enum TextSchema {
    /// Matches if an only if the string is non-empty.
    NonEmpty,
    /// Matches only a specific string.
    Exact(String),
    /// Matches a string against a regular expression.
    Matches(Regex),
}

impl TextSchema {
    /// A schema that matches a single string.
    pub fn exact(string: &str) -> TextSchema {
        TextSchema::Exact(string.to_string())
    }

    /// A schema that accepts strings matching a regular expression.
    pub fn regex(string: &str) -> Result<TextSchema, RegexError> {
        Regex::new(string).map(TextSchema::Matches)
    }
}

impl ToValue for TextSchema {
    fn to_value(&self) -> Value {
        match self {
            TextSchema::NonEmpty => Attr::of("non_empty"),
            TextSchema::Exact(v) => Attr::of(("equal", v.clone())),
            TextSchema::Matches(r) => Attr::of(("matches", r.to_string())),
        }
        .into()
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

/// Schema for Recon [`Attr`]s.
#[derive(Clone, Debug, PartialEq)]
pub struct AttrSchema {
    name_schema: TextSchema,
    value_schema: StandardSchema,
}

impl AttrSchema {
    /// Create an attribute schema.
    /// # Arguments
    ///
    /// * `name` - Schema for the name of the attribute.
    /// * `value` - Schema for the value of the attribute.
    ///
    pub fn new(name: TextSchema, value: StandardSchema) -> Self {
        AttrSchema {
            name_schema: name,
            value_schema: value,
        }
    }

    /// Create an attribute schema with a fixed name.
    /// # Arguments
    ///
    /// * `name` - The name of the attribute.
    /// * `value` - Schema for the value of the attribute.
    ///
    pub fn named(name: &str, value: StandardSchema) -> Self {
        AttrSchema {
            name_schema: TextSchema::exact(name),
            value_schema: value,
        }
    }

    /// Create a schema that matches attributes without bodies.
    /// # Arguments
    ///
    /// * `name` - The name of the attribute.
    ///
    pub fn tag(name: &str) -> Self {
        AttrSchema {
            name_schema: TextSchema::exact(name),
            value_schema: StandardSchema::OfKind(ValueKind::Extant),
        }
    }

    /// Creates a schema that checks the first attribute of a record against this schema
    /// the the remainder of the record against another.
    pub fn and_then(self, schema: StandardSchema) -> StandardSchema {
        StandardSchema::HeadAttribute {
            schema: Box::new(self),
            required: true,
            remainder: Box::new(schema),
        }
    }

    /// Creates a schema that checks that the value is a record containing only an attribute
    /// matching this schema.
    pub fn only(self) -> StandardSchema {
        self.and_then(StandardSchema::is_empty_record())
    }

    /// Creates a schema that optionally checks the first attribute of a record against this schema
    /// the the remainder of the record against another.
    pub fn optionally_and_then(self, schema: StandardSchema) -> StandardSchema {
        StandardSchema::HeadAttribute {
            schema: Box::new(self),
            required: false,
            remainder: Box::new(schema),
        }
    }

    fn to_attr(&self) -> Attr {
        Attr::of((
            "attr",
            Value::from_vec(vec![
                Item::slot("name", self.name_schema.to_value()),
                Item::slot("value", self.value_schema.to_value()),
            ]),
        ))
    }
}

impl ToValue for AttrSchema {
    fn to_value(&self) -> Value {
        Value::of_attr(self.to_attr())
    }
}

impl FieldSchema<Attr> for AttrSchema {
    fn matches_field(&self, field: &Attr) -> FieldMatchResult {
        if self.name_schema.matches_str(&field.name.borrow()) {
            if self.value_schema.matches(&field.value) {
                FieldMatchResult::Both
            } else {
                FieldMatchResult::KeyOnly
            }
        } else {
            FieldMatchResult::KeyFailed
        }
    }
}

/// Specification for a field of a record.
#[derive(Clone, Debug, PartialEq)]
pub struct FieldSpec<S> {
    /// The schema to apply to the field.
    schema: S,
    /// Whether the filed is mandatory.
    required: bool,
    /// Whether the field must be unique.
    unique: bool,
}

impl<S> FieldSpec<S> {
    pub fn new(schema: S, required: bool, unique: bool) -> Self {
        FieldSpec {
            schema,
            required,
            unique,
        }
    }

    pub fn default(schema: S) -> Self {
        FieldSpec::new(schema, true, true)
    }
}

impl<S: ToValue> ToValue for FieldSpec<S> {
    fn to_value(&self) -> Value {
        let head_attr = Attr::with_items(
            "field",
            vec![("required", self.required), ("unique", self.unique)],
        );
        let inner = self.schema.to_value();
        match inner {
            Value::Record(mut attrs, items) => {
                attrs.insert(0, head_attr);
                Value::Record(attrs, items)
            }
            ow => Value::Record(vec![head_attr], vec![Item::ValueItem(ow)]),
        }
    }
}

/// Schema for Recon slots.
#[derive(Clone, Debug, PartialEq)]
pub struct SlotSchema {
    key_schema: StandardSchema,
    value_schema: StandardSchema,
}

impl SlotSchema {
    /// Create an slot schema.
    /// # Arguments
    ///
    /// * `key` - Schema for the key of the attribute.
    /// * `value` - Schema for the value of the attribute.
    ///
    pub fn new(key: StandardSchema, value: StandardSchema) -> Self {
        SlotSchema {
            key_schema: key,
            value_schema: value,
        }
    }
}

impl ToValue for SlotSchema {
    fn to_value(&self) -> Value {
        Value::of_attr((
            "slot",
            Value::from_vec(vec![
                Item::slot("key", self.key_schema.to_value()),
                Item::slot("value", self.value_schema.to_value()),
            ]),
        ))
    }
}

impl FieldSchema<Item> for SlotSchema {
    fn matches_field(&self, field: &Item) -> FieldMatchResult {
        match field {
            Item::ValueItem(_) => FieldMatchResult::KeyFailed,
            Item::Slot(key, value) => {
                if self.key_schema.matches(key) {
                    if self.value_schema.matches(value) {
                        FieldMatchResult::Both
                    } else {
                        FieldMatchResult::KeyOnly
                    }
                } else {
                    FieldMatchResult::KeyFailed
                }
            }
        }
    }
}

/// Schema for Recon [`Item`]s.
#[derive(Clone, Debug, PartialEq)]
pub enum ItemSchema {
    Field(SlotSchema),
    ValueItem(StandardSchema),
}

impl ToValue for ItemSchema {
    fn to_value(&self) -> Value {
        match self {
            ItemSchema::Field(slot) => slot.to_value(),
            ItemSchema::ValueItem(s) => s.to_value(),
        }
    }
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

fn check_contained<T, S: FieldSchema<T>>(
    schemas: &[FieldSpec<S>],
    items: &[T],
    exhaustive: bool,
) -> bool {
    let mut matched = HashSet::new();
    for spec in schemas.iter() {
        let FieldSpec {
            schema,
            required,
            unique,
        } = spec;
        let mut count: usize = 0;
        for (i, item) in items.iter().enumerate() {
            match schema.matches_field(item) {
                FieldMatchResult::KeyOnly => {
                    return false;
                }
                FieldMatchResult::Both => {
                    matched.insert(i);
                    count += 1;
                }
                FieldMatchResult::KeyFailed => {}
            }
        }
        if (*required && count == 0) || (*unique && count > 1) {
            return false;
        }
    }
    !exhaustive || matched.len() == items.len()
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
pub enum StandardSchema {
    /// Asserts that a [`Value`] is of a particular kind.
    OfKind(ValueKind),
    /// Asserts that a [`Value`] takes a specific value.
    Equal(Value),
    /// Asserts that a [`Value`] is an integer and within a specified range.
    InRangeInt {
        min: Option<(i64, bool)>,
        max: Option<(i64, bool)>,
    },
    /// Asserts that a [`Value`] is a floating point number and in a specified range.
    InRangeFloat {
        min: Option<(f64, bool)>,
        max: Option<(f64, bool)>,
    },
    /// Asserts that a [`Value`] is a non-NaN floating point number.
    NonNan,
    /// Asserts that a [`Value`] is a finite floating point number.
    Finite,
    /// Asserts that a [`Value`] is text and matches a specified [`TextSchema`].
    Text(TextSchema),
    /// Inversion of another schema.
    Not(Box<StandardSchema>),
    /// Conjunction of a number of other schemas.
    And(Vec<StandardSchema>),
    /// Disjunction of a number of other schemas.
    Or(Vec<StandardSchema>),
    /// Asserts that a [`Value`] is a record and all of its items match another schema.
    AllItems(Box<ItemSchema>),
    /// Asserts that a [`Value`] has a specific number of attributes.
    NumAttrs(usize),
    /// Asserts that a [`Value`] has a specific number of items.
    NumItems(usize),
    /// Optionally tests the first attributes of a record against a schema and then tests
    /// the remainder of the record against another schema.
    HeadAttribute {
        schema: Box<AttrSchema>,
        required: bool,
        remainder: Box<StandardSchema>,
    },
    /// Asserts than a [`Value`] is a record with the specified attributes.
    HasAttributes {
        attributes: Vec<FieldSpec<AttrSchema>>,
        exhaustive: bool,
    },
    /// Asserts than a [`Value`] is a record with the specified slots.
    HasSlots {
        slots: Vec<FieldSpec<SlotSchema>>,
        exhaustive: bool,
    },
    /// Asserts than a [`Value`] is a record with a precise layout of items in its body.
    Layout {
        items: Vec<(ItemSchema, bool)>,
        exhaustive: bool,
    },
    /// Matches anything.
    Anything,
    /// Matches nothing.
    Nothing,
}

// Very basic partial ordering for schemas. This could be significantly extended if desirable.
impl PartialOrd for StandardSchema {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        if self.eq(other) {
            Some(Ordering::Equal)
        } else {
            match (self, other) {
                (StandardSchema::Anything, _) => Some(Ordering::Greater),
                (_, StandardSchema::Anything) => Some(Ordering::Less),
                (_, StandardSchema::Nothing) => Some(Ordering::Greater),
                (StandardSchema::Nothing, _) => Some(Ordering::Less),
                (StandardSchema::OfKind(kind), _) => Some(of_kind_cmp(kind, other)?),
                (_, StandardSchema::OfKind(kind)) => Some(of_kind_cmp(kind, self)?.reverse()),

                _ => None,
            }
        }
    }
}

fn of_kind_cmp(this_kind: &ValueKind, other: &StandardSchema) -> Option<Ordering> {
    match (this_kind, other) {
        (ValueKind::Text, StandardSchema::Text(_)) => Some(Ordering::Greater),
        (ValueKind::Float64, StandardSchema::InRangeFloat { .. }) => Some(Ordering::Greater),
        (ValueKind::Float64, StandardSchema::NonNan) => Some(Ordering::Greater),
        (ValueKind::Float64, StandardSchema::Finite) => Some(Ordering::Greater),
        (ValueKind::Int64, StandardSchema::InRangeInt { .. }) => Some(Ordering::Greater),
        (_, StandardSchema::OfKind(other_kind)) => this_kind.partial_cmp(other_kind),
        (_, StandardSchema::Equal(value)) => {
            let other_kind = value.kind();
            if this_kind.eq(&other_kind) {
                if this_kind.eq(&ValueKind::Extant) {
                    Some(Ordering::Equal)
                } else {
                    Some(Ordering::Greater)
                }
            } else if this_kind > &other_kind {
                Some(Ordering::Greater)
            } else {
                None
            }
        }
        _ => None,
    }
}

impl Display for StandardSchema {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.to_value().fmt(f)
    }
}

struct RefRecord<'a> {
    attrs: &'a [Attr],
    items: &'a [Item],
}

impl<'a> RefRecord<'a> {
    fn new(attrs: &'a [Attr], items: &'a [Item]) -> Self {
        RefRecord { attrs, items }
    }
}

impl StandardSchema {
    fn matches_ref(&self, record: &RefRecord) -> bool {
        match self {
            StandardSchema::OfKind(ValueKind::Record) => true,
            StandardSchema::Equal(Value::Record(other_attrs, other_items)) => {
                record.attrs == other_attrs.as_slice() && record.items == other_items.as_slice()
            }
            StandardSchema::Not(p) => !p.matches_ref(record),
            StandardSchema::And(ps) => ps.iter().all(|schema| schema.matches_ref(record)),
            StandardSchema::Or(ps) => ps.iter().any(|schema| schema.matches_ref(record)),
            StandardSchema::AllItems(schema) => {
                record.items.iter().all(|item| schema.matches(item))
            }
            StandardSchema::NumAttrs(n) => record.attrs.len() == *n,
            StandardSchema::NumItems(n) => record.items.len() == *n,
            StandardSchema::HeadAttribute {
                schema,
                required,
                remainder,
            } => matches_head_attr(schema, *required, remainder, record),
            StandardSchema::HasAttributes {
                attributes,
                exhaustive,
            } => check_contained(attributes.as_slice(), record.attrs, *exhaustive),
            StandardSchema::HasSlots { slots, exhaustive } => {
                check_contained(slots.as_slice(), record.items, *exhaustive)
            }
            StandardSchema::Layout { items, exhaustive } => {
                check_in_order(items, record.items, *exhaustive)
            }
            StandardSchema::Anything => true,
            _ => false,
        }
    }

    fn matches_rem(&self, record: &RefRecord) -> bool {
        if record.attrs.is_empty() {
            match record.items {
                [Item::ValueItem(single)] => self.matches(single) || self.matches_ref(record),
                _ => self.matches_ref(record),
            }
        } else {
            self.matches_ref(record)
        }
    }
}

fn matches_head_attr<'a>(
    schema: &AttrSchema,
    required: bool,
    remainder: &StandardSchema,
    record: &RefRecord<'a>,
) -> bool {
    match record.attrs.split_first() {
        Some((head, tail)) => match schema.matches_field(head) {
            FieldMatchResult::KeyOnly => false,
            FieldMatchResult::Both => remainder.matches_rem(&RefRecord::new(tail, record.items)),
            FieldMatchResult::KeyFailed => {
                !required && remainder.matches_rem(&RefRecord::new(tail, record.items))
            }
        },
        _ => !required && remainder.matches_ref(record),
    }
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
            StandardSchema::Anything => true,
            StandardSchema::Nothing => false,
            StandardSchema::Equal(v) => value == v,
            StandardSchema::HeadAttribute {
                schema,
                required,
                remainder,
            } => as_record(value)
                .map(|(attrs, items)| {
                    matches_head_attr(schema, *required, remainder, &RefRecord::new(attrs, items))
                })
                .unwrap_or(false),
            StandardSchema::HasAttributes {
                attributes,
                exhaustive,
            } => as_record(value)
                .map(|(attrs, _)| check_contained(attributes.as_slice(), attrs, *exhaustive))
                .unwrap_or(false),
            StandardSchema::HasSlots { slots, exhaustive } => as_record(value)
                .map(|(_, items)| check_contained(slots.as_slice(), items, *exhaustive))
                .unwrap_or(false),
            StandardSchema::Layout {
                items: item_schemas,
                exhaustive,
            } => as_record(value)
                .map(|(_, items)| check_in_order(item_schemas.as_slice(), items, *exhaustive))
                .unwrap_or(false),
        }
    }
}

impl StandardSchema {
    /// A schema that matches a specific [`Value`].
    pub fn eq<T: Into<Value>>(value: T) -> Self {
        StandardSchema::Equal(value.into())
    }

    /// Matches integer values in an inclusive range.
    pub fn inclusive_int_range(min: i64, max: i64) -> Self {
        StandardSchema::InRangeInt {
            min: Some((min, true)),
            max: Some((max, true)),
        }
    }

    /// Matches integer values in an exclusive range.
    pub fn exclusive_int_range(min: i64, max: i64) -> Self {
        StandardSchema::InRangeInt {
            min: Some((min, false)),
            max: Some((max, false)),
        }
    }

    /// Matches integer values, inclusive below and exclusive above.
    pub fn int_range(min: i64, max: i64) -> Self {
        StandardSchema::InRangeInt {
            min: Some((min, true)),
            max: Some((max, false)),
        }
    }

    /// Matches integer values less than (or less than or equal to) a value.
    pub fn until_int(n: i64, inclusive: bool) -> Self {
        StandardSchema::InRangeInt {
            min: None,
            max: Some((n, inclusive)),
        }
    }

    /// Matches integer values greater than (or greater than or equal to) a value.
    pub fn after_int(n: i64, inclusive: bool) -> Self {
        StandardSchema::InRangeInt {
            min: Some((n, inclusive)),
            max: None,
        }
    }

    /// Matches floating point values in an inclusive range.
    pub fn inclusive_float_range(min: f64, max: f64) -> Self {
        StandardSchema::InRangeFloat {
            min: Some((min, true)),
            max: Some((max, true)),
        }
    }

    /// Matches floating point values in an exclusive range.
    pub fn exclusive_float_range(min: f64, max: f64) -> Self {
        StandardSchema::InRangeFloat {
            min: Some((min, false)),
            max: Some((max, false)),
        }
    }

    /// Matches floating point values, inclusive below and exclusive above.
    pub fn float_range(min: f64, max: f64) -> Self {
        StandardSchema::InRangeFloat {
            min: Some((min, true)),
            max: Some((max, false)),
        }
    }

    /// Matches floating point values less than (or less than or equal to) a value.
    pub fn until_float(x: f64, inclusive: bool) -> Self {
        StandardSchema::InRangeFloat {
            min: None,
            max: Some((x, inclusive)),
        }
    }

    /// Matches floating point values greater than (or greater than or equal to) a value.
    pub fn after_float(x: f64, inclusive: bool) -> Self {
        StandardSchema::InRangeFloat {
            min: Some((x, inclusive)),
            max: None,
        }
    }

    /// Negate this schema.
    pub fn negate(self) -> Self {
        StandardSchema::Not(Box::new(self))
    }

    /// Form the conjunction of this schema with another.
    pub fn and(self, other: Self) -> Self {
        StandardSchema::And(vec![self, other])
    }

    /// Form the disjunction of this schema with another.
    pub fn or(self, other: Self) -> Self {
        StandardSchema::Or(vec![self, other])
    }

    /// A schema that matches a specific string.
    pub fn text(string: &str) -> Self {
        StandardSchema::Text(TextSchema::exact(string))
    }

    /// A schema for records with items that all match a schema.
    pub fn array(elements: StandardSchema) -> Self {
        StandardSchema::AllItems(Box::new(ItemSchema::ValueItem(elements)))
    }

    /// A schema for records of slots with keys and values matching specific schemas.
    pub fn map(keys: StandardSchema, values: StandardSchema) -> Self {
        StandardSchema::AllItems(Box::new(ItemSchema::Field(SlotSchema::new(keys, values))))
    }

    pub fn is_empty_record() -> Self {
        StandardSchema::eq(Value::empty_record())
    }
}

impl ToValue for StandardSchema {
    fn to_value(&self) -> Value {
        match self {
            StandardSchema::OfKind(kind) => Value::of_attr(("kind", kind_to_str(*kind))),
            StandardSchema::Equal(v) => Value::of_attr(("equal", v.clone())),
            StandardSchema::InRangeInt { min, max } => range_to_value("in_range_int", *min, *max),
            StandardSchema::InRangeFloat { min, max } => {
                range_to_value("in_range_float", *min, *max)
            }
            StandardSchema::NonNan => Value::of_attr("non_nan"),
            StandardSchema::Finite => Value::of_attr("finite"),
            StandardSchema::Text(text_schema) => text_schema.to_value().prepend(Attr::of("text")),
            StandardSchema::Not(s) => Attr::with_value("not", s.to_value()).into(),
            StandardSchema::And(terms) => operator_sequence_to_value("and", terms.as_slice()),
            StandardSchema::Or(terms) => operator_sequence_to_value("or", terms.as_slice()),
            StandardSchema::AllItems(s) => Value::of_attr(("all_items", s.to_value())),
            StandardSchema::NumAttrs(n) => {
                Value::of_attr(("num_attrs", i64::try_from(*n).unwrap_or(i64::max_value())))
            }
            StandardSchema::NumItems(n) => {
                Value::of_attr(("num_items", i64::try_from(*n).unwrap_or(i64::max_value())))
            }
            StandardSchema::HeadAttribute {
                schema,
                required,
                remainder,
            } => {
                let tag = Attr::with_field("head", "required", *required);
                Value::from_vec(vec![
                    ("schema", schema.to_value()),
                    ("remainder", remainder.to_value()),
                ])
                .prepend(tag)
            }
            StandardSchema::HasAttributes {
                attributes,
                exhaustive,
            } => has_fields_to_value("has_attributes", attributes.as_slice(), *exhaustive),
            StandardSchema::HasSlots { slots, exhaustive } => {
                has_fields_to_value("has_slots", slots.as_slice(), *exhaustive)
            }
            StandardSchema::Layout { items, exhaustive } => {
                let item_schemas = items
                    .iter()
                    .map(|(s, required)| layout_item(s, *required))
                    .collect();
                Value::Record(
                    vec![Attr::with_field("layout", "exhaustive", *exhaustive)],
                    item_schemas,
                )
            }
            StandardSchema::Anything => Value::of_attr("anything"),
            StandardSchema::Nothing => Value::of_attr("nothing"),
        }
    }
}

// Create a Value from a "has attributes" or "has slots" schema.
fn has_fields_to_value<F: ToValue>(tag: &str, fields: &[FieldSpec<F>], exhaustive: bool) -> Value {
    Value::Record(
        vec![Attr::with_field(tag, "exhaustive", exhaustive)],
        fields
            .iter()
            .map(|fld| Item::ValueItem(fld.to_value()))
            .collect(),
    )
}

// Create a Value from one end-point of a range of numbers.
fn endpoint_to_slot<N: Into<Value>>(tag: &str, value: N, inclusive: bool) -> Item {
    let end_point = Value::from_vec(vec![
        Item::slot("value", value),
        Item::slot("inclusive", inclusive),
    ]);
    Item::slot(tag, end_point)
}

// Create a Value from a numeric range schema.
fn range_to_value<N: Into<Value>>(
    tag: &str,
    min: Option<(N, bool)>,
    max: Option<(N, bool)>,
) -> Value {
    let mut slots = vec![];
    if let Some((value, inclusive)) = min {
        slots.push(endpoint_to_slot("min", value, inclusive))
    }
    if let Some((value, inclusive)) = max {
        slots.push(endpoint_to_slot("max", value, inclusive))
    }
    Attr::with_items(tag, slots).into()
}

// Create a Value from an "and" or "or" schema.
fn operator_sequence_to_value(tag: &str, terms: &[StandardSchema]) -> Value {
    Attr::with_items(tag, terms.iter().map(ToValue::to_value).collect()).into()
}

// Create a Value from a item specification from a layout schema.
fn layout_item(schema: &ItemSchema, required: bool) -> Item {
    schema
        .to_value()
        .prepend(Attr::with_field("item", "required", required))
        .into()
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

fn kind_to_str(kind: ValueKind) -> &'static str {
    match kind {
        ValueKind::Extant => "extant",
        ValueKind::Int32 => "int32",
        ValueKind::Int64 => "int64",
        ValueKind::Float64 => "float64",
        ValueKind::Boolean => "boolean",
        ValueKind::Text => "text",
        ValueKind::Record => "record",
    }
}
