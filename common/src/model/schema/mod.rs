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

use crate::model::schema::attr::AttrSchema;
use crate::model::schema::item::ItemSchema;
use crate::model::schema::range::{
    float_64_range, float_range_to_value, in_float_range, in_int_range, in_range, int_32_range,
    int_64_range, int_range_to_value, Bound, Range,
};
use crate::model::schema::slot::SlotSchema;
use crate::model::schema::text::TextSchema;
use crate::model::{Attr, Item, ToValue, Value, ValueKind};
use std::cmp::Ordering;
use std::collections::HashSet;
use std::convert::TryFrom;
use std::fmt::{Display, Formatter};

pub mod attr;
mod item;
mod range;
pub mod slot;
mod text;

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
    /// The key schema did not match and the value schema was not tested.
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

fn combine_orderings(this: Ordering, that: Ordering) -> Option<Ordering> {
    if this != Ordering::Equal && this == that.reverse() {
        None
    } else if this != Ordering::Equal {
        Some(this)
    } else {
        Some(that)
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

fn check_contained<T, S: FieldSchema<T>>(
    schemas: &[FieldSpec<S>],
    items: &[T],
    exhaustive: bool,
) -> bool {
    let mut matched = HashSet::new();
    let mut partial_matched = HashSet::new();
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
                    partial_matched.insert(i);
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

    (!exhaustive || matched.len() == items.len()) && matched.is_superset(&partial_matched)
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
    InRangeInt(Range<i64>),
    /// Asserts that a [`Value`] is a floating point number and in a specified range.
    InRangeFloat(Range<f64>),
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
    /// Asserts a BLOB's data length.
    DataLength(usize),
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
                (StandardSchema::OfKind(kind), _) => of_kind_cmp(kind, other),
                (_, StandardSchema::OfKind(kind)) => Some(of_kind_cmp(kind, self)?.reverse()),
                (StandardSchema::Equal(value), _) => equal_cmp(value, other),
                (_, StandardSchema::Equal(value)) => Some(equal_cmp(value, self)?.reverse()),
                (StandardSchema::InRangeInt(range), _) => in_range_int_cmp(range, other),
                (_, StandardSchema::InRangeInt(range)) => {
                    Some(in_range_int_cmp(range, self)?.reverse())
                }
                (StandardSchema::InRangeFloat(range), _) => in_range_float_cmp(range, other),
                (_, StandardSchema::InRangeFloat(range)) => {
                    Some(in_range_float_cmp(range, self)?.reverse())
                }
                (StandardSchema::NonNan, _) => non_nan_cmp(other),
                (_, StandardSchema::NonNan) => Some(non_nan_cmp(self)?.reverse()),
                (StandardSchema::Text(value), _) => text_cmp(value, other),
                (_, StandardSchema::Text(value)) => Some(text_cmp(value, self)?.reverse()),
                (StandardSchema::AllItems(value), _) => all_items_cmp(value, other),
                (_, StandardSchema::AllItems(value)) => Some(all_items_cmp(value, self)?.reverse()),
                (StandardSchema::NumAttrs(size), _) => num_attrs_cmp(size, other),
                (_, StandardSchema::NumAttrs(size)) => Some(num_attrs_cmp(size, self)?.reverse()),
                (StandardSchema::NumItems(size), _) => num_items_cmp(size, other),
                (_, StandardSchema::NumItems(size)) => Some(num_items_cmp(size, self)?.reverse()),
                (
                    StandardSchema::HeadAttribute {
                        schema,
                        required,
                        remainder,
                    },
                    _,
                ) => head_attribute_cmp(schema, *required, remainder, other),
                (
                    _,
                    StandardSchema::HeadAttribute {
                        schema,
                        required,
                        remainder,
                    },
                ) => Some(head_attribute_cmp(schema, *required, remainder, self)?.reverse()),
                (
                    StandardSchema::HasAttributes {
                        attributes,
                        exhaustive,
                    },
                    _,
                ) => has_attributes_cmp(attributes, *exhaustive, other),
                (
                    _,
                    StandardSchema::HasAttributes {
                        attributes,
                        exhaustive,
                    },
                ) => Some(has_attributes_cmp(attributes, *exhaustive, self)?.reverse()),
                (StandardSchema::HasSlots { slots, exhaustive }, _) => {
                    has_slots_cmp(slots, *exhaustive, other)
                }
                (_, StandardSchema::HasSlots { slots, exhaustive }) => {
                    Some(has_slots_cmp(slots, *exhaustive, self)?.reverse())
                }
                (StandardSchema::Layout { items, exhaustive }, _) => {
                    layout_cmp(items, *exhaustive, other)
                }
                (_, StandardSchema::Layout { items, exhaustive }) => {
                    Some(layout_cmp(items, *exhaustive, self)?.reverse())
                }

                _ => None,
            }
        }
    }
}

fn of_kind_cmp(this_kind: &ValueKind, other: &StandardSchema) -> Option<Ordering> {
    match (this_kind, other) {
        (ValueKind::Extant, StandardSchema::Equal(Value::Extant)) => Some(Ordering::Equal),
        (ValueKind::Int32, StandardSchema::OfKind(ValueKind::Int64)) => Some(Ordering::Less),
        (ValueKind::Int32, StandardSchema::Equal(Value::Int32Value(_))) => Some(Ordering::Greater),
        (ValueKind::Int32, StandardSchema::Equal(Value::Int64Value(value))) => {
            if in_range(*value, &int_32_range()) {
                Some(Ordering::Greater)
            } else {
                None
            }
        }
        (ValueKind::Int32, StandardSchema::InRangeInt(range)) => int_32_range().partial_cmp(range),
        (ValueKind::Int64, StandardSchema::OfKind(ValueKind::Int32)) => Some(Ordering::Greater),
        (ValueKind::Int64, StandardSchema::Equal(Value::Int32Value(_))) => Some(Ordering::Greater),
        (ValueKind::Int64, StandardSchema::Equal(Value::Int64Value(_))) => Some(Ordering::Greater),
        (ValueKind::Int64, StandardSchema::InRangeInt(range)) => int_64_range().partial_cmp(range),
        (ValueKind::Float64, StandardSchema::Equal(Value::Float64Value(_))) => {
            Some(Ordering::Greater)
        }
        (ValueKind::Float64, StandardSchema::InRangeFloat(range)) => {
            float_64_range().partial_cmp(range)
        }
        (ValueKind::Float64, StandardSchema::NonNan) => Some(Ordering::Greater),
        (ValueKind::Float64, StandardSchema::Finite) => Some(Ordering::Greater),
        (ValueKind::Boolean, StandardSchema::Equal(Value::BooleanValue(_))) => {
            Some(Ordering::Greater)
        }
        (ValueKind::Text, StandardSchema::Equal(Value::Text(_))) => Some(Ordering::Greater),
        (ValueKind::Text, StandardSchema::Text(_)) => Some(Ordering::Greater),
        (ValueKind::Record, StandardSchema::Equal(Value::Record(_, _))) => Some(Ordering::Greater),
        (ValueKind::Record, StandardSchema::AllItems(_)) => Some(Ordering::Greater),
        (ValueKind::Record, StandardSchema::NumItems(_)) => Some(Ordering::Greater),
        (ValueKind::Record, StandardSchema::NumAttrs(_)) => Some(Ordering::Greater),
        (ValueKind::Record, StandardSchema::HeadAttribute { .. }) => Some(Ordering::Greater),
        (ValueKind::Record, StandardSchema::HasAttributes { .. }) => Some(Ordering::Greater),
        (ValueKind::Record, StandardSchema::HasSlots { .. }) => Some(Ordering::Greater),
        (ValueKind::Record, StandardSchema::Layout { .. }) => Some(Ordering::Greater),

        _ => None,
    }
}

fn equal_cmp(this: &Value, other: &StandardSchema) -> Option<Ordering> {
    match (this, other) {
        (Value::Int32Value(this_val), StandardSchema::Equal(Value::Int64Value(other_val)))
            if *this_val as i64 == *other_val =>
        {
            Some(Ordering::Equal)
        }
        (Value::Int32Value(this_val), StandardSchema::InRangeInt(range))
            if in_range(*this_val as i64, range) =>
        {
            Some(Ordering::Less)
        }
        (Value::Int64Value(this_val), StandardSchema::Equal(Value::Int32Value(other_val)))
            if *this_val == *other_val as i64 =>
        {
            Some(Ordering::Equal)
        }
        (Value::Int64Value(this_val), StandardSchema::InRangeInt(range))
            if in_range(*this_val, range) =>
        {
            Some(Ordering::Less)
        }
        (Value::Float64Value(this_val), StandardSchema::InRangeFloat(range))
            if in_range(*this_val, range) =>
        {
            Some(Ordering::Less)
        }
        (Value::Float64Value(this_val), StandardSchema::NonNan) if !this_val.is_nan() => {
            Some(Ordering::Less)
        }
        (Value::Float64Value(this_val), StandardSchema::Finite) if this_val.is_finite() => {
            Some(Ordering::Less)
        }
        (Value::Text(this_val), StandardSchema::Text(TextSchema::NonEmpty))
            if !this_val.is_empty() =>
        {
            Some(Ordering::Less)
        }
        (Value::Text(this_val), StandardSchema::Text(TextSchema::Exact(other_val)))
            if this_val.eq(other_val) =>
        {
            Some(Ordering::Equal)
        }
        (Value::Text(this_val), StandardSchema::Text(TextSchema::Matches(regex)))
            if regex.is_match(this_val) =>
        {
            Some(Ordering::Less)
        }
        (Value::Record(attr, items), schema) => {
            if schema.matches_ref(&RefRecord::new(attr, items)) {
                Some(Ordering::Less)
            } else {
                None
            }
        }

        _ => None,
    }
}

fn in_range_int_cmp(this: &Range<i64>, other: &StandardSchema) -> Option<Ordering> {
    match other {
        StandardSchema::InRangeInt(other_range) => this.partial_cmp(&other_range),
        _ => None,
    }
}

fn in_range_float_cmp(this: &Range<f64>, other: &StandardSchema) -> Option<Ordering> {
    match other {
        StandardSchema::InRangeFloat(other_range) => this.partial_cmp(&other_range),
        StandardSchema::Finite if this.is_bounded() => Some(Ordering::Less),
        StandardSchema::NonNan if !this.is_doubly_unbounded() => Some(Ordering::Less),
        _ => None,
    }
}

fn non_nan_cmp(other: &StandardSchema) -> Option<Ordering> {
    match other {
        StandardSchema::Finite => Some(Ordering::Greater),
        _ => None,
    }
}

fn text_cmp(this: &TextSchema, other: &StandardSchema) -> Option<Ordering> {
    match other {
        StandardSchema::Text(other_schema) => this.partial_cmp(other_schema),
        _ => None,
    }
}

fn all_items_cmp(this: &ItemSchema, other: &StandardSchema) -> Option<Ordering> {
    match other {
        StandardSchema::AllItems(other_schema) => this.partial_cmp(&other_schema),

        StandardSchema::HeadAttribute {
            schema: _,
            required: true,
            remainder,
        } => {
            let cmp = StandardSchema::AllItems(Box::new(this.clone())).partial_cmp(remainder)?;

            if cmp != Ordering::Less {
                Some(Ordering::Greater)
            } else {
                None
            }
        }

        StandardSchema::HasAttributes {
            attributes,
            exhaustive,
        } if attributes.is_empty() && !*exhaustive => Some(Ordering::Less),

        StandardSchema::HasSlots { slots, exhaustive } if slots.is_empty() && !*exhaustive => {
            Some(Ordering::Less)
        }

        StandardSchema::HasSlots { slots, exhaustive } if *exhaustive => {
            if let ItemSchema::Field(this_schema) = this {
                for FieldSpec { schema: s, .. } in slots {
                    if this_schema.partial_cmp(s)? == Ordering::Less {
                        return None;
                    }
                }
                Some(Ordering::Greater)
            } else {
                None
            }
        }

        StandardSchema::Layout { items, exhaustive } if items.is_empty() && !*exhaustive => {
            Some(Ordering::Less)
        }

        StandardSchema::Layout { items, exhaustive } if *exhaustive => {
            for (schema, _) in items {
                if this.partial_cmp(schema)? == Ordering::Less {
                    return None;
                }
            }

            Some(Ordering::Greater)
        }

        _ => None,
    }
}

fn num_attrs_cmp(size: &usize, other: &StandardSchema) -> Option<Ordering> {
    match other {
        StandardSchema::HeadAttribute {
            schema: _,
            required,
            remainder,
        } if *required => {
            if StandardSchema::NumAttrs(size - 1).partial_cmp(remainder)? != Ordering::Less {
                Some(Ordering::Greater)
            } else {
                None
            }
        }

        StandardSchema::HasAttributes {
            attributes,
            exhaustive,
        } if !*exhaustive && attributes.is_empty() => Some(Ordering::Less),

        StandardSchema::HasAttributes {
            attributes,
            exhaustive,
        } if *exhaustive && attributes.len() == *size => {
            for attr in attributes {
                if !attr.required {
                    return None;
                }
            }

            Some(Ordering::Greater)
        }

        StandardSchema::HasSlots { slots, exhaustive } if !*exhaustive && slots.is_empty() => {
            Some(Ordering::Less)
        }

        StandardSchema::Layout { items, exhaustive } if !*exhaustive && items.is_empty() => {
            Some(Ordering::Less)
        }

        _ => None {},
    }
}

fn num_items_cmp(size: &usize, other: &StandardSchema) -> Option<Ordering> {
    match other {
        StandardSchema::HeadAttribute {
            schema: _,
            required: _,
            remainder,
        } => {
            if StandardSchema::NumItems(*size).partial_cmp(remainder)? != Ordering::Less {
                Some(Ordering::Greater)
            } else {
                None
            }
        }

        StandardSchema::HasAttributes {
            attributes,
            exhaustive,
        } if !*exhaustive && attributes.is_empty() => Some(Ordering::Less),

        StandardSchema::HasSlots { slots, exhaustive } if !*exhaustive && slots.is_empty() => {
            Some(Ordering::Less)
        }

        StandardSchema::HasSlots { slots, exhaustive } if *exhaustive && *size == slots.len() => {
            for s in slots {
                if !s.required {
                    return None;
                }
            }

            Some(Ordering::Greater)
        }

        StandardSchema::Layout { items, exhaustive } if !*exhaustive && items.is_empty() => {
            Some(Ordering::Less)
        }

        StandardSchema::Layout { items, exhaustive } if *exhaustive && *size == items.len() => {
            for i in items {
                if !i.1 {
                    return None;
                }
            }

            Some(Ordering::Greater)
        }

        _ => None,
    }
}

fn head_attribute_cmp(
    this_schema: &AttrSchema,
    this_required: bool,
    this_remainder: &StandardSchema,
    other: &StandardSchema,
) -> Option<Ordering> {
    match other {
        StandardSchema::HeadAttribute {
            schema: other_schema,
            required: other_required,
            remainder: other_remainder,
        } => {
            let schema = this_schema.partial_cmp(other_schema)?;
            let required = this_required.partial_cmp(other_required)?.reverse();

            let combined = combine_orderings(schema, required)?;
            let remainder = this_remainder.partial_cmp(other_remainder)?;

            combine_orderings(combined, remainder)
        }

        StandardSchema::HasAttributes {
            attributes,
            exhaustive,
        } if !*exhaustive && attributes.is_empty() => Some(Ordering::Less),

        StandardSchema::HasAttributes {
            attributes,
            exhaustive,
        } if *exhaustive => {
            let mut prev_comp = Ordering::Equal;

            for FieldSpec {
                schema: other_schema,
                ..
            } in attributes
            {
                let current_cmp = this_schema.partial_cmp(other_schema)?;
                prev_comp = combine_orderings(prev_comp, current_cmp)?;
            }

            let remainder = this_remainder.partial_cmp(&StandardSchema::HasAttributes {
                attributes: attributes.clone(),
                exhaustive: *exhaustive,
            })?;

            combine_orderings(prev_comp, remainder)
        }

        StandardSchema::HasSlots { slots, exhaustive } if !*exhaustive && slots.is_empty() => {
            Some(Ordering::Less)
        }

        StandardSchema::Layout { items, exhaustive } if !*exhaustive && items.is_empty() => {
            Some(Ordering::Less)
        }

        _ => None,
    }
}

fn has_attributes_cmp(
    this_attributes: &[FieldSpec<AttrSchema>],
    this_exhaustive: bool,
    other: &StandardSchema,
) -> Option<Ordering> {
    match other {
        StandardSchema::HasAttributes {
            attributes: other_attributes,
            exhaustive: other_exhaustive,
        } => {
            let cmp = vec_schemas_cmp(
                this_exhaustive,
                *other_exhaustive,
                this_attributes.is_empty(),
                other_attributes.is_empty(),
            );

            if cmp.is_none() && this_exhaustive == *other_exhaustive {
                if fields_are_related(this_attributes, other_attributes, this_exhaustive) {
                    cmp_related(
                        this_attributes.len(),
                        other_attributes.len(),
                        this_exhaustive,
                    )
                } else {
                    None
                }
            } else {
                cmp
            }
        }

        StandardSchema::HasSlots {
            slots,
            exhaustive: slots_exhaustive,
        } => vec_schemas_cmp(
            this_exhaustive,
            *slots_exhaustive,
            this_attributes.is_empty(),
            slots.is_empty(),
        ),

        StandardSchema::Layout {
            items,
            exhaustive: layout_exhaustive,
        } => vec_schemas_cmp(
            this_exhaustive,
            *layout_exhaustive,
            this_attributes.is_empty(),
            items.is_empty(),
        ),

        _ => None,
    }
}

fn has_slots_cmp(
    this_slots: &[FieldSpec<SlotSchema>],
    this_exhaustive: bool,
    other: &StandardSchema,
) -> Option<Ordering> {
    match other {
        StandardSchema::HasSlots {
            slots: other_slots,
            exhaustive: other_exhaustive,
        } => {
            let cmp = vec_schemas_cmp(
                this_exhaustive,
                *other_exhaustive,
                this_slots.is_empty(),
                other_slots.is_empty(),
            );

            if cmp.is_none()
                && this_exhaustive == *other_exhaustive
                && fields_are_related(this_slots, other_slots, this_exhaustive)
            {
                cmp_related(this_slots.len(), other_slots.len(), this_exhaustive)
            } else {
                cmp
            }
        }

        StandardSchema::Layout {
            items,
            exhaustive: other_exhaustive,
        } => {
            let cmp = vec_schemas_cmp(
                this_exhaustive,
                *other_exhaustive,
                this_slots.is_empty(),
                items.is_empty(),
            );

            if this_exhaustive == *other_exhaustive && this_exhaustive && cmp.is_none() {
                let mut other_slots = Vec::new();

                for (item, required) in items {
                    if let ItemSchema::Field(slot) = item {
                        other_slots.push(FieldSpec::new(slot.clone(), *required, false))
                    } else {
                        return None;
                    }
                }

                if this_slots.len() < other_slots.len() {
                    None
                } else {
                    let are_related = fields_are_related(this_slots, &other_slots, this_exhaustive);

                    if are_related {
                        Some(Ordering::Greater)
                    } else {
                        None
                    }
                }
            } else {
                cmp
            }
        }

        _ => None,
    }
}

fn layout_cmp(
    this_items: &[(ItemSchema, bool)],
    this_exhaustive: bool,
    other_schema: &StandardSchema,
) -> Option<Ordering> {
    match other_schema {
        StandardSchema::Layout {
            items: other_items,
            exhaustive: other_exhaustive,
        } => {
            let cmp = vec_schemas_cmp(
                this_exhaustive,
                *other_exhaustive,
                this_items.is_empty(),
                other_items.is_empty(),
            );

            if cmp.is_none() && this_exhaustive == *other_exhaustive {
                let are_related =
                    ordered_items_are_related(this_items, other_items, this_exhaustive);

                if are_related {
                    cmp_related(this_items.len(), other_items.len(), this_exhaustive)
                } else {
                    None
                }
            } else {
                cmp
            }
        }

        _ => None,
    }
}

fn cmp_related(first_len: usize, second_len: usize, exhaustive: bool) -> Option<Ordering> {
    if first_len == second_len {
        Some(Ordering::Equal)
    } else if first_len > second_len {
        if exhaustive {
            Some(Ordering::Greater)
        } else {
            Some(Ordering::Less)
        }
    } else if exhaustive {
        Some(Ordering::Less)
    } else {
        Some(Ordering::Greater)
    }
}

fn vec_schemas_cmp(
    this_exhaustive: bool,
    other_exhaustive: bool,
    this_is_empty: bool,
    other_is_empty: bool,
) -> Option<Ordering> {
    if !this_exhaustive && !other_exhaustive && this_is_empty && other_is_empty {
        Some(Ordering::Equal)
    } else if !this_exhaustive && this_is_empty {
        Some(Ordering::Greater)
    } else if !other_exhaustive && other_is_empty {
        Some(Ordering::Less)
    } else {
        None
    }
}

fn fields_are_related<T: PartialEq>(
    this_fields: &[FieldSpec<T>],
    other_fields: &[FieldSpec<T>],
    exhaustive: bool,
) -> bool {
    let (sup_fields, sub_fields) = if this_fields.len() >= other_fields.len() {
        (this_fields, other_fields)
    } else {
        (other_fields, this_fields)
    };

    let mut required_sub_count = 0;

    for sub_field in sub_fields {
        let mut has_equal = false;

        for sup_field in sup_fields {
            if sup_field == sub_field {
                has_equal = true;
                break;
            }
        }

        if !has_equal {
            return false;
        }

        if sub_field.required {
            required_sub_count += 1
        }
    }

    if exhaustive {
        let required_sup_count = sup_fields.iter().filter(|f| f.required).count();
        if required_sup_count > required_sub_count {
            return false;
        }
    }

    true
}

fn ordered_items_are_related(
    this_items: &[(ItemSchema, bool)],
    other_items: &[(ItemSchema, bool)],
    exhaustive: bool,
) -> bool {
    let (mut sup_items_iter, sub_items) = if this_items.len() >= other_items.len() {
        (this_items.iter(), other_items)
    } else {
        (other_items.iter(), this_items)
    };

    for sub_field in sub_items {
        if Some(sub_field) != sup_items_iter.next() {
            return false;
        }
    }

    if exhaustive {
        return sup_items_iter.all(|(_, required)| !*required);
    }

    true
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
            StandardSchema::InRangeInt(range) => in_int_range(value, range),
            StandardSchema::InRangeFloat(range) => in_float_range(value, range),
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
            StandardSchema::DataLength(len) => match value {
                Value::Data(b) => b.as_ref().len() == *len,
                _ => false,
            },
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
        StandardSchema::InRangeInt(Range::<i64>::bounded(
            Bound::inclusive(min),
            Bound::inclusive(max),
        ))
    }

    /// Matches integer values in an exclusive range.
    pub fn exclusive_int_range(min: i64, max: i64) -> Self {
        StandardSchema::InRangeInt(Range::<i64>::bounded(
            Bound::exclusive(min),
            Bound::exclusive(max),
        ))
    }

    /// Matches integer values, inclusive below and exclusive above.
    pub fn int_range(min: i64, max: i64) -> Self {
        StandardSchema::InRangeInt(Range::<i64>::bounded(
            Bound::inclusive(min),
            Bound::exclusive(max),
        ))
    }

    /// Matches integer values less than (or less than or equal to) a value.
    pub fn until_int(n: i64, inclusive: bool) -> Self {
        StandardSchema::InRangeInt(Range::<i64>::upper_bounded(Bound::new(n, inclusive)))
    }

    /// Matches integer values greater than (or greater than or equal to) a value.
    pub fn after_int(n: i64, inclusive: bool) -> Self {
        StandardSchema::InRangeInt(Range::<i64>::lower_bounded(Bound::new(n, inclusive)))
    }

    /// Matches floating point values in an inclusive range.
    pub fn inclusive_float_range(min: f64, max: f64) -> Self {
        StandardSchema::InRangeFloat(Range::<f64>::bounded(
            Bound::inclusive(min),
            Bound::inclusive(max),
        ))
    }

    /// Matches floating point values in an exclusive range.
    pub fn exclusive_float_range(min: f64, max: f64) -> Self {
        StandardSchema::InRangeFloat(Range::<f64>::bounded(
            Bound::exclusive(min),
            Bound::exclusive(max),
        ))
    }

    /// Matches floating point values, inclusive below and exclusive above.
    pub fn float_range(min: f64, max: f64) -> Self {
        StandardSchema::InRangeFloat(Range::<f64>::bounded(
            Bound::inclusive(min),
            Bound::exclusive(max),
        ))
    }

    /// Matches floating point values less than (or less than or equal to) a value.
    pub fn until_float(x: f64, inclusive: bool) -> Self {
        StandardSchema::InRangeFloat(Range::<f64>::upper_bounded(Bound::new(x, inclusive)))
    }

    /// Matches floating point values greater than (or greater than or equal to) a value.
    pub fn after_float(x: f64, inclusive: bool) -> Self {
        StandardSchema::InRangeFloat(Range::<f64>::lower_bounded(Bound::new(x, inclusive)))
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

    /// A schema that matches the length of a BLOB.
    pub fn binary_length(len: usize) -> Self {
        StandardSchema::DataLength(len)
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
            StandardSchema::InRangeInt(range) => int_range_to_value("in_range_int", *range),
            StandardSchema::InRangeFloat(range) => float_range_to_value("in_range_float", *range),
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
            StandardSchema::DataLength(len) => Value::of_attr(("binary_length", *len as i32)),
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

// Create a Value from one end-point of a range of ints.
fn int_endpoint_to_slot<N: Into<Value>>(tag: &str, value: N) -> Item {
    let end_point = Value::from_vec(vec![Item::slot("value", value)]);
    Item::slot(tag, end_point)
}

// Create a Value from one end-point of a range of floats.
fn float_endpoint_to_slot<N: Into<Value>>(tag: &str, value: N, inclusive: bool) -> Item {
    let end_point = Value::from_vec(vec![
        Item::slot("value", value),
        Item::slot("inclusive", inclusive),
    ]);
    Item::slot(tag, end_point)
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

fn kind_to_str(kind: ValueKind) -> &'static str {
    match kind {
        ValueKind::Extant => "extant",
        ValueKind::Int32 => "int32",
        ValueKind::Int64 => "int64",
        ValueKind::Float64 => "float64",
        ValueKind::Boolean => "boolean",
        ValueKind::Text => "text",
        ValueKind::Record => "record",
        ValueKind::Data => "data",
    }
}
