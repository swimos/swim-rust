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

use super::*;
use std::collections::HashMap;

#[test]
fn non_empty_string() {
    let schema = TextSchema::NonEmpty;
    assert!(!schema.matches_str(""));
    assert!(schema.matches_str("a"));
}

#[test]
fn exact_string() {
    let schema = TextSchema::exact("Hello");
    assert!(!schema.matches_str("hello"));
    assert!(schema.matches_str("Hello"));
}

#[test]
fn regex_match() {
    let schema = TextSchema::regex("^ab*a$").unwrap();
    assert!(schema.matches_str("aa"));
    assert!(schema.matches_str("abba"));
    assert!(!schema.matches_str("aaba"));
    assert!(!schema.matches_str("abca"));
}

const KINDS: [ValueKind; 7] = [
    ValueKind::Extant,
    ValueKind::Int32,
    ValueKind::Int64,
    ValueKind::Float64,
    ValueKind::Boolean,
    ValueKind::Text,
    ValueKind::Record,
];

fn arbitrary() -> HashMap<ValueKind, Value> {
    let mut map = HashMap::new();
    map.insert(ValueKind::Extant, Value::Extant);
    map.insert(ValueKind::Int32, Value::Int32Value(23));
    map.insert(ValueKind::Int64, Value::Int64Value(-4569847476726364i64));
    map.insert(ValueKind::Float64, Value::Float64Value(-0.5));
    map.insert(ValueKind::Boolean, Value::BooleanValue(true));
    map.insert(ValueKind::Text, Value::text("Hello"));
    map.insert(ValueKind::Record, Value::empty_record());
    map
}

fn arbitrary_without(kinds: Vec<ValueKind>) -> HashMap<ValueKind, Value> {
    let mut map = arbitrary();
    for kind in kinds.iter() {
        map.remove(&kind);
    }
    map
}

#[test]
fn kind_schema() {
    let examples = arbitrary();
    for schema_kind in KINDS.iter() {
        let schema = StandardSchema::OfKind(*schema_kind);
        for input_kind in KINDS.iter() {
            let input = examples.get(input_kind).unwrap();
            if input_kind == schema_kind {
                assert!(schema.matches(input));
            } else {
                assert!(!schema.matches(input));
            }
        }
    }
}

#[test]
fn int_range_schema() {
    let schema = StandardSchema::int_range(-2, 3);

    let bad_kinds = arbitrary_without(vec![ValueKind::Int32, ValueKind::Int64]);
    for value in bad_kinds.values() {
        assert!(!schema.matches(value));
    }

    assert!(!schema.matches(&Value::Int32Value(-3)));
    assert!(!schema.matches(&Value::Int64Value(-3)));

    assert!(schema.matches(&Value::Int32Value(-2)));
    assert!(schema.matches(&Value::Int64Value(-2)));

    assert!(schema.matches(&Value::Int32Value(0)));
    assert!(schema.matches(&Value::Int64Value(0)));

    assert!(!schema.matches(&Value::Int32Value(3)));
    assert!(!schema.matches(&Value::Int64Value(3)));

    assert!(!schema.matches(&Value::Int32Value(5)));
    assert!(!schema.matches(&Value::Int64Value(5)));
}

#[test]
fn bounded_float_range_schema() {
    use std::f64;

    let schema = StandardSchema::float_range(-2.0, 3.0);

    let bad_kinds = arbitrary_without(vec![ValueKind::Float64]);
    for value in bad_kinds.values() {
        assert!(!schema.matches(value));
    }

    assert!(!schema.matches(&Value::Float64Value(-3.0)));

    assert!(schema.matches(&Value::Float64Value(-2.0)));

    assert!(schema.matches(&Value::Float64Value(0.0)));

    assert!(!schema.matches(&Value::Float64Value(3.0)));

    assert!(!schema.matches(&Value::Float64Value(5.0)));

    assert!(!schema.matches(&Value::Float64Value(f64::INFINITY)));
    assert!(!schema.matches(&Value::Float64Value(f64::NEG_INFINITY)));
    assert!(!schema.matches(&Value::Float64Value(f64::NAN)));
}

#[test]
fn unbounded_float_range_schema() {
    use std::f64;

    let above = StandardSchema::after_float(1.5, true);
    let below = StandardSchema::until_float(1.5, true);

    assert!(above.matches(&Value::Float64Value(1.5)));
    assert!(above.matches(&Value::Float64Value(f64::INFINITY)));
    assert!(!above.matches(&Value::Float64Value(f64::NEG_INFINITY)));
    assert!(!above.matches(&Value::Float64Value(f64::NAN)));

    assert!(below.matches(&Value::Float64Value(1.5)));
    assert!(!below.matches(&Value::Float64Value(f64::INFINITY)));
    assert!(below.matches(&Value::Float64Value(f64::NEG_INFINITY)));
    assert!(!below.matches(&Value::Float64Value(f64::NAN)));
}

#[test]
fn non_nan_schema() {
    use std::f64;

    let schema = StandardSchema::NonNan;

    let bad_kinds = arbitrary_without(vec![ValueKind::Float64]);
    for value in bad_kinds.values() {
        assert!(!schema.matches(value));
    }

    assert!(schema.matches(&Value::Float64Value(0.0)));
    assert!(schema.matches(&Value::Float64Value(1.0)));
    assert!(schema.matches(&Value::Float64Value(-1.0)));
    assert!(schema.matches(&Value::Float64Value(f64::INFINITY)));
    assert!(schema.matches(&Value::Float64Value(f64::NEG_INFINITY)));
    assert!(!schema.matches(&Value::Float64Value(f64::NAN)));
}

#[test]
fn finite_schema() {
    use std::f64;

    let schema = StandardSchema::Finite;

    let bad_kinds = arbitrary_without(vec![ValueKind::Float64]);
    for value in bad_kinds.values() {
        assert!(!schema.matches(value));
    }

    assert!(schema.matches(&Value::Float64Value(0.0)));
    assert!(schema.matches(&Value::Float64Value(1.0)));
    assert!(schema.matches(&Value::Float64Value(-1.0)));
    assert!(!schema.matches(&Value::Float64Value(f64::INFINITY)));
    assert!(!schema.matches(&Value::Float64Value(f64::NEG_INFINITY)));
    assert!(!schema.matches(&Value::Float64Value(f64::NAN)));
}

#[test]
fn text_schema() {
    let schema = StandardSchema::Text(TextSchema::NonEmpty);
    let bad_kinds = arbitrary_without(vec![ValueKind::Text]);
    for value in bad_kinds.values() {
        assert!(!schema.matches(value));
    }
    assert!(!schema.matches(&Value::text("")));
    assert!(schema.matches(&Value::text("a")));
}

#[test]
fn negated_schema() {
    let schema = StandardSchema::Text(TextSchema::NonEmpty).negate();
    let bad_kinds = arbitrary_without(vec![ValueKind::Text]);
    for value in bad_kinds.values() {
        assert!(schema.matches(value));
    }
    assert!(schema.matches(&Value::text("")));
    assert!(!schema.matches(&Value::text("a")));
}

#[test]
fn equal_schema() {
    let schema = StandardSchema::Equal(Value::text("hello"));
    let bad_kinds = arbitrary_without(vec![ValueKind::Text]);
    for value in bad_kinds.values() {
        assert!(!schema.matches(value));
    }

    assert!(schema.matches(&Value::text("hello")));
    assert!(!schema.matches(&Value::text("world")));
}

#[test]
fn and_schema() {
    let schema = StandardSchema::int_range(0, 5).and(StandardSchema::eq(2).negate());

    assert!(schema.matches(&Value::Int32Value(0)));
    assert!(schema.matches(&Value::Int32Value(1)));
    assert!(!schema.matches(&Value::Int32Value(2)));
    assert!(schema.matches(&Value::Int32Value(3)));
    assert!(schema.matches(&Value::Int32Value(4)));
}

#[test]
fn or_schema() {
    let schema = StandardSchema::eq("hello").or(StandardSchema::eq(1));

    assert!(schema.matches(&Value::Int32Value(1)));
    assert!(schema.matches(&Value::text("hello")));
    assert!(!schema.matches(&Value::Int32Value(2)));
    assert!(!schema.matches(&Value::text("world")));
}

#[test]
fn anything_schema() {
    let schema = StandardSchema::Anything;
    for value in arbitrary().values() {
        assert!(schema.matches(&value));
    }
}

#[test]
fn nothing_schema() {
    let schema = StandardSchema::Nothing;
    for value in arbitrary().values() {
        assert!(!schema.matches(&value));
    }
}

#[test]
fn zero_attrs_schema() {
    let schema = StandardSchema::NumAttrs(0);
    let bad_kinds = arbitrary_without(vec![ValueKind::Record]);
    for value in bad_kinds.values() {
        assert!(!schema.matches(value));
    }

    assert!(schema.matches(&Value::empty_record()));
    assert!(schema.matches(&Value::from_vec(vec![1, 2, 3])));

    assert!(!schema.matches(&Value::of_attr("name")));
}

#[test]
fn one_attrs_schema() {
    let schema = StandardSchema::NumAttrs(1);
    let bad_kinds = arbitrary_without(vec![ValueKind::Record]);
    for value in bad_kinds.values() {
        assert!(!schema.matches(value));
    }

    assert!(!schema.matches(&Value::empty_record()));
    assert!(!schema.matches(&Value::from_vec(vec![1, 2, 3])));

    assert!(schema.matches(&Value::of_attr("name")));
}

#[test]
fn zero_items_schema() {
    let schema = StandardSchema::NumItems(0);
    let bad_kinds = arbitrary_without(vec![ValueKind::Record]);
    for value in bad_kinds.values() {
        assert!(!schema.matches(value));
    }

    assert!(schema.matches(&Value::empty_record()));
    assert!(!schema.matches(&Value::from_vec(vec![1, 2, 3])));

    assert!(schema.matches(&Value::of_attr("name")));
}

#[test]
fn multiple_items_schema() {
    let schema = StandardSchema::NumItems(3);
    let bad_kinds = arbitrary_without(vec![ValueKind::Record]);
    for value in bad_kinds.values() {
        assert!(!schema.matches(value));
    }

    assert!(!schema.matches(&Value::empty_record()));
    assert!(schema.matches(&Value::from_vec(vec![1, 2, 3])));
    assert!(!schema.matches(&Value::from_vec(vec![2, 3])));

    assert!(!schema.matches(&Value::of_attr("name")));
    assert!(schema.matches(&Value::Record(
        vec![Attr::of("name")],
        vec![Item::of(1), Item::of(2), Item::of(3)]
    )));
}

#[test]
fn attr_schema() {
    let attr_schema = AttrSchema::new(TextSchema::exact("name"), StandardSchema::eq(0));
    let bad_kinds = arbitrary_without(vec![ValueKind::Int32]);
    for value in bad_kinds.values() {
        let attr = Attr::of(("name", value.clone()));
        assert!(!attr_schema.matches(&attr));
    }

    let good = Attr::of(("name", 0));
    let bad = Attr::of(("other", 2));

    assert!(attr_schema.matches(&good));
    assert!(!attr_schema.matches(&bad));
}

#[test]
fn slot_schema() {
    let slot_schema = SlotSchema::new(StandardSchema::text("name"), StandardSchema::eq(1));
    let bad_kinds = arbitrary_without(vec![ValueKind::Int32]);
    for value in bad_kinds.values() {
        let slot = Item::slot("name", value.clone());
        let val_item = Item::ValueItem(value.clone());
        assert!(!slot_schema.matches(&val_item));
        assert!(!slot_schema.matches(&slot));
    }

    let good = Item::slot("name", 1);
    let bad1 = Item::of(1);
    let bad2 = Item::slot("other", 0);

    assert!(slot_schema.matches(&good));
    assert!(!slot_schema.matches(&bad1));
    assert!(!slot_schema.matches(&bad2));
}

#[test]
fn has_attributes_single_exhaustive() {
    let attrs = Attributes::HasAttrs(vec![AttrSchema::new(
        TextSchema::exact("name"),
        StandardSchema::OfKind(ValueKind::Int32),
    )]);
    let schema = StandardSchema::Layout(RecordLayout::new(Some(attrs), None, true));

    let bad_kinds = arbitrary_without(vec![ValueKind::Record]);
    for value in bad_kinds.values() {
        assert!(!schema.matches(value));
    }

    assert!(!schema.matches(&Value::empty_record()));

    let bad1 = Value::of_attr(Attr::of(("other", 3)));
    let bad2 = Value::of_attr(Attr::of("name"));
    let bad3 = Value::of_attrs(vec![Attr::of("other"), Attr::of(("name", 3))]);
    assert!(!schema.matches(&bad1));
    assert!(!schema.matches(&bad2));
    assert!(!schema.matches(&bad3));

    let good = Value::of_attr(Attr::of(("name", 3)));

    assert!(schema.matches(&good));
}

#[test]
fn has_attributes_single_non_exhaustive() {
    let attrs = Attributes::HasAttrs(vec![AttrSchema::new(
        TextSchema::exact("name"),
        StandardSchema::OfKind(ValueKind::Int32),
    )]);
    let schema = StandardSchema::Layout(RecordLayout::new(Some(attrs), None, false));

    let bad_kinds = arbitrary_without(vec![ValueKind::Record]);
    for value in bad_kinds.values() {
        assert!(!schema.matches(value));
    }

    assert!(!schema.matches(&Value::empty_record()));

    let bad1 = Value::of_attr(Attr::of(("other", 3)));
    let bad2 = Value::of_attr(Attr::of("name"));

    assert!(!schema.matches(&bad1));
    assert!(!schema.matches(&bad2));

    let good1 = Value::of_attr(Attr::of(("name", 3)));
    let good2 = Value::of_attrs(vec![Attr::of("other"), Attr::of(("name", 3))]);

    assert!(schema.matches(&good1));
    assert!(schema.matches(&good2));
}

#[test]
fn has_attributes_multiple_exhaustive() {
    let attrs = Attributes::HasAttrs(vec![
        AttrSchema::new(
            TextSchema::exact("name1"),
            StandardSchema::OfKind(ValueKind::Int32),
        ),
        AttrSchema::new(
            TextSchema::exact("name2"),
            StandardSchema::OfKind(ValueKind::Text),
        ),
    ]);
    let schema = StandardSchema::Layout(RecordLayout::new(Some(attrs), None, true));

    let bad_kinds = arbitrary_without(vec![ValueKind::Record]);
    for value in bad_kinds.values() {
        assert!(!schema.matches(value));
    }

    assert!(!schema.matches(&Value::empty_record()));

    let bad1 = Value::of_attr(Attr::of(("other", 3)));
    let bad2 = Value::of_attr(Attr::of(("name1", 2)));
    let bad3 = Value::of_attrs(vec![
        Attr::of("other"),
        Attr::of(("name1", 3)),
        Attr::of(("name2", "hello")),
    ]);
    assert!(!schema.matches(&bad1));
    assert!(!schema.matches(&bad2));
    assert!(!schema.matches(&bad3));

    let good1 = Value::of_attrs(vec![Attr::of(("name1", 3)), Attr::of(("name2", "hello"))]);
    let good2 = Value::of_attrs(vec![Attr::of(("name2", "hello")), Attr::of(("name1", 3))]);

    assert!(schema.matches(&good1));
    assert!(schema.matches(&good2));
}

#[test]
fn has_attributes_multiple_non_exhaustive() {
    let attrs = Attributes::HasAttrs(vec![
        AttrSchema::new(
            TextSchema::exact("name1"),
            StandardSchema::OfKind(ValueKind::Int32),
        ),
        AttrSchema::new(
            TextSchema::exact("name2"),
            StandardSchema::OfKind(ValueKind::Text),
        ),
    ]);
    let schema = StandardSchema::Layout(RecordLayout::new(Some(attrs), None, false));

    let bad_kinds = arbitrary_without(vec![ValueKind::Record]);
    for value in bad_kinds.values() {
        assert!(!schema.matches(value));
    }

    assert!(!schema.matches(&Value::empty_record()));

    let bad1 = Value::of_attr(Attr::of(("other", 3)));
    let bad2 = Value::of_attr(Attr::of(("name1", 2)));

    assert!(!schema.matches(&bad1));
    assert!(!schema.matches(&bad2));

    let good1 = Value::of_attrs(vec![Attr::of(("name1", 3)), Attr::of(("name2", "hello"))]);
    let good2 = Value::of_attrs(vec![Attr::of(("name2", "hello")), Attr::of(("name1", 3))]);
    let good3 = Value::of_attrs(vec![
        Attr::of("other"),
        Attr::of(("name1", 3)),
        Attr::of(("name2", "hello")),
    ]);

    assert!(schema.matches(&good1));
    assert!(schema.matches(&good2));
    assert!(schema.matches(&good3));
}

#[test]
fn has_slots_single_exhaustive() {
    let items = Items::HasSlots(vec![SlotSchema::new(
        StandardSchema::Text(TextSchema::exact("name")),
        StandardSchema::OfKind(ValueKind::Int32),
    )]);
    let schema = StandardSchema::Layout(RecordLayout::new(None, Some(items), true));

    let bad_kinds = arbitrary_without(vec![ValueKind::Record]);
    for value in bad_kinds.values() {
        assert!(!schema.matches(value));
    }

    assert!(!schema.matches(&Value::empty_record()));

    let bad1 = Value::singleton(Item::of(("other", 3)));
    let bad2 = Value::singleton(Item::of("name"));
    let bad3 = Value::from_vec(vec![("other", 12), ("name", 3)]);
    assert!(!schema.matches(&bad1));
    assert!(!schema.matches(&bad2));
    assert!(!schema.matches(&bad3));

    let good = Value::singleton(("name", 3));

    assert!(schema.matches(&good));
}

#[test]
fn has_slots_single_non_exhaustive() {
    let items = Items::HasSlots(vec![SlotSchema::new(
        StandardSchema::Text(TextSchema::exact("name")),
        StandardSchema::OfKind(ValueKind::Int32),
    )]);
    let schema = StandardSchema::Layout(RecordLayout::new(None, Some(items), false));

    let bad_kinds = arbitrary_without(vec![ValueKind::Record]);
    for value in bad_kinds.values() {
        assert!(!schema.matches(value));
    }

    assert!(!schema.matches(&Value::empty_record()));

    let bad1 = Value::singleton(Item::of(("other", 3)));
    let bad2 = Value::singleton(Item::of("name"));

    assert!(!schema.matches(&bad1));
    assert!(!schema.matches(&bad2));

    let good1 = Value::singleton(("name", 3));
    let good2 = Value::from_vec(vec![("other", 12), ("name", 3)]);

    assert!(schema.matches(&good1));
    assert!(schema.matches(&good2));
}

#[test]
fn has_slots_multiple_exhaustive() {
    let items = Items::HasSlots(vec![
        SlotSchema::new(
            StandardSchema::Text(TextSchema::exact("name1")),
            StandardSchema::OfKind(ValueKind::Int32),
        ),
        SlotSchema::new(
            StandardSchema::Text(TextSchema::exact("name2")),
            StandardSchema::OfKind(ValueKind::Text),
        ),
    ]);
    let schema = StandardSchema::Layout(RecordLayout::new(None, Some(items), true));

    let bad_kinds = arbitrary_without(vec![ValueKind::Record]);
    for value in bad_kinds.values() {
        assert!(!schema.matches(value));
    }

    assert!(!schema.matches(&Value::empty_record()));

    let bad1 = Value::singleton(("other", 3));
    let bad2 = Value::singleton(("name1", 2));
    let bad3 = Value::from_vec(vec![
        Item::of("other"),
        Item::slot("name1", 3),
        Item::slot("name2", "hello"),
    ]);
    assert!(!schema.matches(&bad1));
    assert!(!schema.matches(&bad2));
    assert!(!schema.matches(&bad3));

    let good1 = Value::from_vec(vec![Item::slot("name1", 3), Item::slot("name2", "hello")]);
    let good2 = Value::from_vec(vec![Item::slot("name2", "hello"), Item::slot("name1", 3)]);

    assert!(schema.matches(&good1));
    assert!(schema.matches(&good2));
}

#[test]
fn has_slots_multiple_non_exhaustive() {
    let items = Items::HasSlots(vec![
        SlotSchema::new(
            StandardSchema::Text(TextSchema::exact("name1")),
            StandardSchema::OfKind(ValueKind::Int32),
        ),
        SlotSchema::new(
            StandardSchema::Text(TextSchema::exact("name2")),
            StandardSchema::OfKind(ValueKind::Text),
        ),
    ]);
    let schema = StandardSchema::Layout(RecordLayout::new(None, Some(items), false));

    let bad_kinds = arbitrary_without(vec![ValueKind::Record]);
    for value in bad_kinds.values() {
        assert!(!schema.matches(value));
    }

    assert!(!schema.matches(&Value::empty_record()));

    let bad1 = Value::singleton(("other", 3));
    let bad2 = Value::singleton(("name1", 2));

    assert!(!schema.matches(&bad1));
    assert!(!schema.matches(&bad2));

    let good1 = Value::from_vec(vec![Item::slot("name1", 3), Item::slot("name2", "hello")]);
    let good2 = Value::from_vec(vec![Item::slot("name2", "hello"), Item::slot("name1", 3)]);
    let good3 = Value::from_vec(vec![
        Item::of("other"),
        Item::slot("name1", 3),
        Item::slot("name2", "hello"),
    ]);

    assert!(schema.matches(&good1));
    assert!(schema.matches(&good2));
    assert!(schema.matches(&good3));
}

#[test]
fn mandatory_attributes_in_order_exhaustive() {
    let attrs = Attributes::AttrsInOrder(vec![
        (
            AttrSchema::new(
                TextSchema::exact("name1"),
                StandardSchema::OfKind(ValueKind::Int32),
            ),
            true,
        ),
        (
            AttrSchema::new(
                TextSchema::exact("name2"),
                StandardSchema::OfKind(ValueKind::Text),
            ),
            true,
        ),
    ]);
    let schema = StandardSchema::Layout(RecordLayout::new(Some(attrs), None, true));

    let bad_kinds = arbitrary_without(vec![ValueKind::Record]);
    for value in bad_kinds.values() {
        assert!(!schema.matches(value));
    }

    assert!(!schema.matches(&Value::empty_record()));

    let bad1 = Value::of_attr(Attr::of(("other", 3)));
    let bad2 = Value::of_attr(Attr::of(("name1", 2)));
    let bad3 = Value::of_attrs(vec![
        Attr::of(("name1", 3)),
        Attr::of(("name2", "hello")),
        Attr::of("other"),
    ]);
    let bad4 = Value::of_attrs(vec![Attr::of(("name2", "hello")), Attr::of(("name1", 3))]);
    assert!(!schema.matches(&bad1));
    assert!(!schema.matches(&bad2));
    assert!(!schema.matches(&bad3));
    assert!(!schema.matches(&bad4));

    let good1 = Value::of_attrs(vec![Attr::of(("name1", 3)), Attr::of(("name2", "hello"))]);

    assert!(schema.matches(&good1));
}

#[test]
fn mandatory_attributes_in_order_non_exhaustive() {
    let attrs = Attributes::AttrsInOrder(vec![
        (
            AttrSchema::new(
                TextSchema::exact("name1"),
                StandardSchema::OfKind(ValueKind::Int32),
            ),
            true,
        ),
        (
            AttrSchema::new(
                TextSchema::exact("name2"),
                StandardSchema::OfKind(ValueKind::Text),
            ),
            true,
        ),
    ]);
    let schema = StandardSchema::Layout(RecordLayout::new(Some(attrs), None, false));

    let bad_kinds = arbitrary_without(vec![ValueKind::Record]);
    for value in bad_kinds.values() {
        assert!(!schema.matches(value));
    }

    assert!(!schema.matches(&Value::empty_record()));

    let bad1 = Value::of_attr(Attr::of(("other", 3)));
    let bad2 = Value::of_attr(Attr::of(("name1", 2)));
    let bad3 = Value::of_attrs(vec![
        Attr::of("other"),
        Attr::of(("name1", 3)),
        Attr::of(("name2", "hello")),
    ]);
    let bad4 = Value::of_attrs(vec![Attr::of(("name2", "hello")), Attr::of(("name1", 3))]);
    assert!(!schema.matches(&bad1));
    assert!(!schema.matches(&bad2));
    assert!(!schema.matches(&bad3));
    assert!(!schema.matches(&bad4));

    let good1 = Value::of_attrs(vec![Attr::of(("name1", 3)), Attr::of(("name2", "hello"))]);
    let good2 = Value::of_attrs(vec![
        Attr::of(("name1", 3)),
        Attr::of(("name2", "hello")),
        Attr::of("other"),
    ]);

    assert!(schema.matches(&good1));
    assert!(schema.matches(&good2));
}

#[test]
fn optional_attribute_in_order_exhaustive() {
    let attrs = Attributes::AttrsInOrder(vec![
        (
            AttrSchema::new(
                TextSchema::exact("name1"),
                StandardSchema::OfKind(ValueKind::Int32),
            ),
            true,
        ),
        (
            AttrSchema::new(
                TextSchema::exact("name2"),
                StandardSchema::OfKind(ValueKind::Text),
            ),
            false,
        ),
    ]);
    let schema = StandardSchema::Layout(RecordLayout::new(Some(attrs), None, true));

    let bad_kinds = arbitrary_without(vec![ValueKind::Record]);
    for value in bad_kinds.values() {
        assert!(!schema.matches(value));
    }

    assert!(!schema.matches(&Value::empty_record()));

    let bad1 = Value::of_attr(Attr::of(("other", 3)));
    let bad2 = Value::of_attrs(vec![
        Attr::of(("name1", 3)),
        Attr::of(("name2", "hello")),
        Attr::of("other"),
    ]);
    let bad3 = Value::of_attrs(vec![Attr::of(("name2", "hello")), Attr::of(("name1", 3))]);
    assert!(!schema.matches(&bad1));
    assert!(!schema.matches(&bad2));
    assert!(!schema.matches(&bad3));

    let good1 = Value::of_attrs(vec![Attr::of(("name1", 3)), Attr::of(("name2", "hello"))]);
    let good2 = Value::of_attr(Attr::of(("name1", 2)));
    assert!(schema.matches(&good1));
    assert!(schema.matches(&good2));
}

#[test]
fn optional_attribute_in_order_non_exhaustive() {
    let attrs = Attributes::AttrsInOrder(vec![
        (
            AttrSchema::new(
                TextSchema::exact("name1"),
                StandardSchema::OfKind(ValueKind::Int32),
            ),
            true,
        ),
        (
            AttrSchema::new(
                TextSchema::exact("name2"),
                StandardSchema::OfKind(ValueKind::Text),
            ),
            false,
        ),
    ]);
    let schema = StandardSchema::Layout(RecordLayout::new(Some(attrs), None, false));

    let bad_kinds = arbitrary_without(vec![ValueKind::Record]);
    for value in bad_kinds.values() {
        assert!(!schema.matches(value));
    }

    assert!(!schema.matches(&Value::empty_record()));

    let bad1 = Value::of_attr(Attr::of(("other", 3)));

    let bad2 = Value::of_attrs(vec![
        Attr::of("other"),
        Attr::of(("name1", 3)),
        Attr::of(("name2", "hello")),
    ]);
    let bad3 = Value::of_attrs(vec![Attr::of(("name2", "hello")), Attr::of(("name1", 3))]);
    assert!(!schema.matches(&bad1));
    assert!(!schema.matches(&bad2));
    assert!(!schema.matches(&bad3));

    let good1 = Value::of_attrs(vec![Attr::of(("name1", 3)), Attr::of(("name2", "hello"))]);
    let good2 = Value::of_attrs(vec![
        Attr::of(("name1", 3)),
        Attr::of(("name2", "hello")),
        Attr::of("other"),
    ]);
    let good3 = Value::of_attr(Attr::of(("name1", 2)));
    let good4 = Value::of_attrs(vec![Attr::of(("name1", 3)), Attr::of("other")]);

    assert!(schema.matches(&good1));
    assert!(schema.matches(&good2));
    assert!(schema.matches(&good3));
    assert!(schema.matches(&good4));
}
