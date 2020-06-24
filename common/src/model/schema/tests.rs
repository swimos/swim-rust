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
use hamcrest2::assert_that;
use hamcrest2::prelude::*;
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
    let spec = FieldSpec::default(AttrSchema::new(
        TextSchema::exact("name"),
        StandardSchema::OfKind(ValueKind::Int32),
    ));
    let schema = StandardSchema::HasAttributes {
        attributes: vec![spec],
        exhaustive: true,
    };

    let bad_kinds = arbitrary_without(vec![ValueKind::Record]);
    for value in bad_kinds.values() {
        assert!(!schema.matches(value));
    }

    assert!(!schema.matches(&Value::empty_record()));

    let bad1 = Value::of_attr(Attr::of(("other", 3)));
    let bad2 = Value::of_attr(Attr::of("name"));
    let bad3 = Value::of_attrs(vec![Attr::of("other"), Attr::of(("name", 3))]);
    let bad4 = Value::of_attrs(vec![Attr::of(("name", 3)), Attr::of(("name", 4))]);
    assert!(!schema.matches(&bad1));
    assert!(!schema.matches(&bad2));
    assert!(!schema.matches(&bad3));
    assert!(!schema.matches(&bad4));

    let good = Value::of_attr(Attr::of(("name", 3)));

    assert!(schema.matches(&good));
}

#[test]
fn has_attributes_single_non_exhaustive() {
    let spec = FieldSpec::default(AttrSchema::new(
        TextSchema::exact("name"),
        StandardSchema::OfKind(ValueKind::Int32),
    ));
    let schema = StandardSchema::HasAttributes {
        attributes: vec![spec],
        exhaustive: false,
    };

    let bad_kinds = arbitrary_without(vec![ValueKind::Record]);
    for value in bad_kinds.values() {
        assert!(!schema.matches(value));
    }

    assert!(!schema.matches(&Value::empty_record()));

    let bad1 = Value::of_attr(Attr::of(("other", 3)));
    let bad2 = Value::of_attr(Attr::of("name"));
    let bad3 = Value::of_attrs(vec![Attr::of(("name", 3)), Attr::of(("name", 4))]);

    assert!(!schema.matches(&bad1));
    assert!(!schema.matches(&bad2));
    assert!(!schema.matches(&bad3));

    let good1 = Value::of_attr(Attr::of(("name", 3)));
    let good2 = Value::of_attrs(vec![Attr::of("other"), Attr::of(("name", 3))]);

    assert!(schema.matches(&good1));
    assert!(schema.matches(&good2));
}

#[test]
fn has_optional_attributes_single_exhaustive() {
    let spec = FieldSpec::new(
        AttrSchema::new(
            TextSchema::exact("name"),
            StandardSchema::OfKind(ValueKind::Int32),
        ),
        false,
        true,
    );
    let schema = StandardSchema::HasAttributes {
        attributes: vec![spec],
        exhaustive: true,
    };

    let bad_kinds = arbitrary_without(vec![ValueKind::Record]);
    for value in bad_kinds.values() {
        assert!(!schema.matches(value));
    }

    assert!(schema.matches(&Value::empty_record()));

    let bad1 = Value::of_attr(Attr::of("name"));
    let bad2 = Value::of_attrs(vec![Attr::of("other"), Attr::of(("name", 3))]);
    let bad3 = Value::of_attrs(vec![Attr::of(("name", 3)), Attr::of(("name", 4))]);

    assert!(!schema.matches(&bad1));
    assert!(!schema.matches(&bad2));
    assert!(!schema.matches(&bad3));

    let good1 = Value::of_attr(Attr::of(("name", 3)));

    assert!(schema.matches(&good1));
}

#[test]
fn has_optional_attributes_single_non_exhaustive() {
    let spec = FieldSpec::new(
        AttrSchema::new(
            TextSchema::exact("name"),
            StandardSchema::OfKind(ValueKind::Int32),
        ),
        false,
        true,
    );
    let schema = StandardSchema::HasAttributes {
        attributes: vec![spec],
        exhaustive: false,
    };

    let bad_kinds = arbitrary_without(vec![ValueKind::Record]);
    for value in bad_kinds.values() {
        assert!(!schema.matches(value));
    }

    assert!(schema.matches(&Value::empty_record()));

    let bad1 = Value::of_attr(Attr::of("name"));
    let bad2 = Value::of_attrs(vec![Attr::of(("name", 3)), Attr::of(("name", 4))]);

    assert!(!schema.matches(&bad1));
    assert!(!schema.matches(&bad2));

    let good1 = Value::of_attr(Attr::of(("name", 3)));
    let good2 = Value::of_attrs(vec![Attr::of("other"), Attr::of(("name", 3))]);
    let good3 = Value::of_attr(Attr::of("other"));

    assert!(schema.matches(&good1));
    assert!(schema.matches(&good2));
    assert!(schema.matches(&good3));
}

#[test]
fn has_non_unique_attributes_single_exhaustive() {
    let spec = FieldSpec::new(
        AttrSchema::new(
            TextSchema::exact("name"),
            StandardSchema::OfKind(ValueKind::Int32),
        ),
        true,
        false,
    );
    let schema = StandardSchema::HasAttributes {
        attributes: vec![spec],
        exhaustive: true,
    };

    let bad_kinds = arbitrary_without(vec![ValueKind::Record]);
    for value in bad_kinds.values() {
        assert!(!schema.matches(value));
    }

    assert!(!schema.matches(&Value::empty_record()));

    let bad1 = Value::of_attr(Attr::of("name"));
    let bad2 = Value::of_attrs(vec![Attr::of("other"), Attr::of(("name", 3))]);

    assert!(!schema.matches(&bad1));
    assert!(!schema.matches(&bad2));

    let good1 = Value::of_attr(Attr::of(("name", 3)));
    let good2 = Value::of_attrs(vec![Attr::of(("name", 3)), Attr::of(("name", 4))]);

    assert!(schema.matches(&good1));
    assert!(schema.matches(&good2));
}

#[test]
fn has_non_unique_attributes_single_non_exhaustive() {
    let spec = FieldSpec::new(
        AttrSchema::new(
            TextSchema::exact("name"),
            StandardSchema::OfKind(ValueKind::Int32),
        ),
        true,
        false,
    );
    let schema = StandardSchema::HasAttributes {
        attributes: vec![spec],
        exhaustive: false,
    };

    let bad_kinds = arbitrary_without(vec![ValueKind::Record]);
    for value in bad_kinds.values() {
        assert!(!schema.matches(value));
    }

    assert!(!schema.matches(&Value::empty_record()));

    let bad1 = Value::of_attr(Attr::of("name"));

    assert!(!schema.matches(&bad1));

    let good1 = Value::of_attr(Attr::of(("name", 3)));
    let good2 = Value::of_attrs(vec![Attr::of(("name", 3)), Attr::of(("name", 4))]);
    let good3 = Value::of_attrs(vec![Attr::of("other"), Attr::of(("name", 3))]);
    let good4 = Value::of_attrs(vec![
        Attr::of(("name", 3)),
        Attr::of("other"),
        Attr::of(("name", 4)),
    ]);

    assert!(schema.matches(&good1));
    assert!(schema.matches(&good2));
    assert!(schema.matches(&good3));
    assert!(schema.matches(&good4));
}

#[test]
fn has_attributes_multiple_exhaustive() {
    let spec1 = FieldSpec::default(AttrSchema::new(
        TextSchema::exact("name1"),
        StandardSchema::OfKind(ValueKind::Int32),
    ));
    let spec2 = FieldSpec::default(AttrSchema::new(
        TextSchema::exact("name2"),
        StandardSchema::OfKind(ValueKind::Text),
    ));
    let schema = StandardSchema::HasAttributes {
        attributes: vec![spec1, spec2],
        exhaustive: true,
    };

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
    let spec1 = FieldSpec::default(AttrSchema::new(
        TextSchema::exact("name1"),
        StandardSchema::OfKind(ValueKind::Int32),
    ));
    let spec2 = FieldSpec::default(AttrSchema::new(
        TextSchema::exact("name2"),
        StandardSchema::OfKind(ValueKind::Text),
    ));
    let schema = StandardSchema::HasAttributes {
        attributes: vec![spec1, spec2],
        exhaustive: false,
    };

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
    let spec = FieldSpec::default(SlotSchema::new(
        StandardSchema::Text(TextSchema::exact("name")),
        StandardSchema::OfKind(ValueKind::Int32),
    ));
    let schema = StandardSchema::HasSlots {
        slots: vec![spec],
        exhaustive: true,
    };

    let bad_kinds = arbitrary_without(vec![ValueKind::Record]);
    for value in bad_kinds.values() {
        assert!(!schema.matches(value));
    }

    assert!(!schema.matches(&Value::empty_record()));

    let bad1 = Value::singleton(Item::of(("other", 3)));
    let bad2 = Value::singleton(Item::of("name"));
    let bad3 = Value::from_vec(vec![("other", 12), ("name", 3)]);
    let bad4 = Value::from_vec(vec![("name", 3), ("name", 4)]);
    assert!(!schema.matches(&bad1));
    assert!(!schema.matches(&bad2));
    assert!(!schema.matches(&bad3));
    assert!(!schema.matches(&bad4));

    let good = Value::singleton(("name", 3));

    assert!(schema.matches(&good));
}

#[test]
fn has_slots_single_non_exhaustive() {
    let spec = FieldSpec::default(SlotSchema::new(
        StandardSchema::Text(TextSchema::exact("name")),
        StandardSchema::OfKind(ValueKind::Int32),
    ));
    let schema = StandardSchema::HasSlots {
        slots: vec![spec],
        exhaustive: false,
    };

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
fn has_optional_slots_single_exhaustive() {
    let spec = FieldSpec::new(
        SlotSchema::new(
            StandardSchema::Text(TextSchema::exact("name")),
            StandardSchema::OfKind(ValueKind::Int32),
        ),
        false,
        true,
    );
    let schema = StandardSchema::HasSlots {
        slots: vec![spec],
        exhaustive: true,
    };

    let bad_kinds = arbitrary_without(vec![ValueKind::Record]);
    for value in bad_kinds.values() {
        assert!(!schema.matches(value));
    }

    assert!(schema.matches(&Value::empty_record()));

    let bad1 = Value::singleton(Item::of(("other", 3)));
    let bad2 = Value::singleton(Item::of("name"));
    let bad3 = Value::from_vec(vec![("other", 12), ("name", 3)]);
    let bad4 = Value::from_vec(vec![("name", 3), ("name", 4)]);

    assert!(!schema.matches(&bad1));
    assert!(!schema.matches(&bad2));
    assert!(!schema.matches(&bad3));
    assert!(!schema.matches(&bad4));

    let good1 = Value::singleton(("name", 3));

    assert!(schema.matches(&good1));
}

#[test]
fn has_optional_slots_single_non_exhaustive() {
    let spec = FieldSpec::new(
        SlotSchema::new(
            StandardSchema::Text(TextSchema::exact("name")),
            StandardSchema::OfKind(ValueKind::Int32),
        ),
        false,
        true,
    );
    let schema = StandardSchema::HasSlots {
        slots: vec![spec],
        exhaustive: false,
    };

    let bad_kinds = arbitrary_without(vec![ValueKind::Record]);
    for value in bad_kinds.values() {
        assert!(!schema.matches(value));
    }

    assert!(schema.matches(&Value::empty_record()));

    let bad1 = Value::singleton(Item::slot("name", Value::Extant));
    let bad2 = Value::from_vec(vec![("name", 3), ("name", 4)]);

    assert!(!schema.matches(&bad1));
    assert!(!schema.matches(&bad2));

    let good1 = Value::singleton(("name", 3));
    let good2 = Value::singleton(Item::of(("other", 3)));
    let good3 = Value::singleton(Item::of(("other", 3)));
    let good4 = Value::from_vec(vec![("other", 12), ("name", 3)]);
    let good5 = Value::singleton("name");

    assert!(schema.matches(&good1));
    assert!(schema.matches(&good2));
    assert!(schema.matches(&good3));
    assert!(schema.matches(&good4));
    assert!(schema.matches(&good5));
}

#[test]
fn has_non_unique_slots_single_exhaustive() {
    let spec = FieldSpec::new(
        SlotSchema::new(
            StandardSchema::Text(TextSchema::exact("name")),
            StandardSchema::OfKind(ValueKind::Int32),
        ),
        true,
        false,
    );
    let schema = StandardSchema::HasSlots {
        slots: vec![spec],
        exhaustive: true,
    };

    let bad_kinds = arbitrary_without(vec![ValueKind::Record]);
    for value in bad_kinds.values() {
        assert!(!schema.matches(value));
    }

    assert!(!schema.matches(&Value::empty_record()));

    let bad1 = Value::singleton(Item::of(("other", 3)));
    let bad2 = Value::singleton(Item::of("name"));
    let bad3 = Value::from_vec(vec![("other", 12), ("name", 3)]);
    let bad4 = Value::from_vec(vec![("name", 3), ("other", 7), ("name", 4)]);

    assert!(!schema.matches(&bad1));
    assert!(!schema.matches(&bad2));
    assert!(!schema.matches(&bad3));
    assert!(!schema.matches(&bad4));

    let good1 = Value::singleton(("name", 3));
    let good2 = Value::from_vec(vec![("name", 3), ("name", 4)]);

    assert!(schema.matches(&good1));
    assert!(schema.matches(&good2));
}

#[test]
fn has_non_unique_slots_single_non_exhaustive() {
    let spec = FieldSpec::new(
        SlotSchema::new(
            StandardSchema::Text(TextSchema::exact("name")),
            StandardSchema::OfKind(ValueKind::Int32),
        ),
        true,
        false,
    );
    let schema = StandardSchema::HasSlots {
        slots: vec![spec],
        exhaustive: false,
    };

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
    let good2 = Value::from_vec(vec![("name", 3), ("name", 4)]);
    let good3 = Value::from_vec(vec![("other", 12), ("name", 3)]);
    let good4 = Value::from_vec(vec![("name", 3), ("other", 7), ("name", 4)]);

    assert!(schema.matches(&good1));
    assert!(schema.matches(&good2));
    assert!(schema.matches(&good3));
    assert!(schema.matches(&good4));
}

#[test]
fn has_slots_multiple_exhaustive() {
    let spec1 = FieldSpec::default(SlotSchema::new(
        StandardSchema::Text(TextSchema::exact("name1")),
        StandardSchema::OfKind(ValueKind::Int32),
    ));
    let spec2 = FieldSpec::default(SlotSchema::new(
        StandardSchema::Text(TextSchema::exact("name2")),
        StandardSchema::OfKind(ValueKind::Text),
    ));

    let schema = StandardSchema::HasSlots {
        slots: vec![spec1, spec2],
        exhaustive: true,
    };

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
    let spec1 = FieldSpec::default(SlotSchema::new(
        StandardSchema::Text(TextSchema::exact("name1")),
        StandardSchema::OfKind(ValueKind::Int32),
    ));
    let spec2 = FieldSpec::default(SlotSchema::new(
        StandardSchema::Text(TextSchema::exact("name2")),
        StandardSchema::OfKind(ValueKind::Text),
    ));

    let schema = StandardSchema::HasSlots {
        slots: vec![spec1, spec2],
        exhaustive: false,
    };

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
    let att_schema1 = AttrSchema::new(
        TextSchema::exact("name1"),
        StandardSchema::OfKind(ValueKind::Int32),
    );
    let att_schema2 = AttrSchema::new(
        TextSchema::exact("name2"),
        StandardSchema::OfKind(ValueKind::Text),
    );

    let schema = att_schema1.and_then(att_schema2.and_then(StandardSchema::NumAttrs(0)));

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
    let att_schema1 = AttrSchema::new(
        TextSchema::exact("name1"),
        StandardSchema::OfKind(ValueKind::Int32),
    );
    let att_schema2 = AttrSchema::new(
        TextSchema::exact("name2"),
        StandardSchema::OfKind(ValueKind::Text),
    );

    let schema = att_schema1.and_then(att_schema2.and_then(StandardSchema::Anything));

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
    let att_schema1 = AttrSchema::new(
        TextSchema::exact("name1"),
        StandardSchema::OfKind(ValueKind::Int32),
    );
    let att_schema2 = AttrSchema::new(
        TextSchema::exact("name2"),
        StandardSchema::OfKind(ValueKind::Text),
    );

    let schema = att_schema1.and_then(att_schema2.optionally_and_then(StandardSchema::NumAttrs(0)));

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
    let att_schema1 = AttrSchema::new(
        TextSchema::exact("name1"),
        StandardSchema::OfKind(ValueKind::Int32),
    );
    let att_schema2 = AttrSchema::new(
        TextSchema::exact("name2"),
        StandardSchema::OfKind(ValueKind::Text),
    );

    let schema = att_schema1.and_then(att_schema2.optionally_and_then(StandardSchema::Anything));

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

#[test]
fn mandatory_items_in_order_exhaustive() {
    let item_schema1 = ItemSchema::Field(SlotSchema::new(
        StandardSchema::Text(TextSchema::exact("name1")),
        StandardSchema::OfKind(ValueKind::Int32),
    ));
    let item_schema2 = ItemSchema::ValueItem(StandardSchema::OfKind(ValueKind::Int32));

    let schema = StandardSchema::Layout {
        items: vec![(item_schema1, true), (item_schema2, true)],
        exhaustive: true,
    };

    let bad_kinds = arbitrary_without(vec![ValueKind::Record]);
    for value in bad_kinds.values() {
        assert!(!schema.matches(value));
    }

    assert!(!schema.matches(&Value::empty_record()));

    let bad1 = Value::singleton(("other", 3));
    let bad2 = Value::singleton(("name1", 2));
    let bad3 = Value::from_vec(vec![Item::slot("name1", 3), Item::of(5), Item::of("other")]);
    let bad4 = Value::from_vec(vec![Item::of(5), Item::slot("name1", 3)]);
    let bad5 = Value::singleton(5);

    assert!(!schema.matches(&bad1));
    assert!(!schema.matches(&bad2));
    assert!(!schema.matches(&bad3));
    assert!(!schema.matches(&bad4));
    assert!(!schema.matches(&bad5));

    let good1 = Value::from_vec(vec![Item::slot("name1", 3), Item::of(5)]);

    assert!(schema.matches(&good1));
}

#[test]
fn mandatory_items_in_order_non_exhaustive() {
    let item_schema1 = ItemSchema::Field(SlotSchema::new(
        StandardSchema::Text(TextSchema::exact("name1")),
        StandardSchema::OfKind(ValueKind::Int32),
    ));
    let item_schema2 = ItemSchema::ValueItem(StandardSchema::OfKind(ValueKind::Int32));

    let schema = StandardSchema::Layout {
        items: vec![(item_schema1, true), (item_schema2, true)],
        exhaustive: false,
    };

    let bad_kinds = arbitrary_without(vec![ValueKind::Record]);
    for value in bad_kinds.values() {
        assert!(!schema.matches(value));
    }

    assert!(!schema.matches(&Value::empty_record()));

    let bad1 = Value::singleton(("other", 3));
    let bad2 = Value::singleton(("name1", 2));
    let bad3 = Value::from_vec(vec![Item::of(5), Item::slot("name1", 3)]);
    assert!(!schema.matches(&bad1));
    assert!(!schema.matches(&bad2));
    assert!(!schema.matches(&bad3));

    let good1 = Value::from_vec(vec![Item::slot("name1", 3), Item::of(5)]);
    let good2 = Value::from_vec(vec![Item::slot("name1", 3), Item::of(5), Item::of("other")]);

    assert!(schema.matches(&good1));
    assert!(schema.matches(&good2));
}

#[test]
fn optional_item_in_order_exhaustive() {
    let item_schema1 = ItemSchema::Field(SlotSchema::new(
        StandardSchema::Text(TextSchema::exact("name1")),
        StandardSchema::OfKind(ValueKind::Int32),
    ));
    let item_schema2 = ItemSchema::ValueItem(StandardSchema::OfKind(ValueKind::Int32));

    let schema = StandardSchema::Layout {
        items: vec![(item_schema1, false), (item_schema2, true)],
        exhaustive: true,
    };

    let bad_kinds = arbitrary_without(vec![ValueKind::Record]);
    for value in bad_kinds.values() {
        assert!(!schema.matches(value));
    }

    assert!(!schema.matches(&Value::empty_record()));

    let bad1 = Value::singleton(("other", 3));
    let bad2 = Value::singleton(("name1", 2));
    let bad3 = Value::from_vec(vec![Item::slot("name1", 3), Item::of(5), Item::of("other")]);
    let bad4 = Value::from_vec(vec![Item::of(5), Item::slot("name1", 3)]);
    assert!(!schema.matches(&bad1));
    assert!(!schema.matches(&bad2));
    assert!(!schema.matches(&bad3));
    assert!(!schema.matches(&bad4));

    let good1 = Value::from_vec(vec![Item::slot("name1", 3), Item::of(5)]);
    let good2 = Value::singleton(5);

    assert!(schema.matches(&good1));
    assert!(schema.matches(&good2));
}

#[test]
fn optional_item_in_order_non_exhaustive() {
    let item_schema1 = ItemSchema::Field(SlotSchema::new(
        StandardSchema::Text(TextSchema::exact("name1")),
        StandardSchema::OfKind(ValueKind::Int32),
    ));
    let item_schema2 = ItemSchema::ValueItem(StandardSchema::OfKind(ValueKind::Int32));

    let schema = StandardSchema::Layout {
        items: vec![(item_schema1, false), (item_schema2, true)],
        exhaustive: false,
    };

    let bad_kinds = arbitrary_without(vec![ValueKind::Record]);
    for value in bad_kinds.values() {
        assert!(!schema.matches(value));
    }

    assert!(!schema.matches(&Value::empty_record()));

    let bad1 = Value::singleton(("other", 3));
    let bad2 = Value::singleton(("name1", 2));

    assert!(!schema.matches(&bad1));
    assert!(!schema.matches(&bad2));

    let good1 = Value::from_vec(vec![Item::slot("name1", 3), Item::of(5)]);
    let good2 = Value::singleton(5);
    let good3 = Value::from_vec(vec![Item::slot("name1", 3), Item::of(5), Item::of("other")]);
    let good4 = Value::from_vec(vec![Item::of(5), Item::of("other")]);
    let good5 = Value::from_vec(vec![Item::of(5), Item::slot("name1", 3)]);

    assert!(schema.matches(&good1));
    assert!(schema.matches(&good2));
    assert!(schema.matches(&good3));
    assert!(schema.matches(&good4));
    assert!(schema.matches(&good5));
}

#[test]
fn array_of_values() {
    let schema = StandardSchema::array(StandardSchema::OfKind(ValueKind::Int32));

    assert!(schema.matches(&Value::empty_record()));
    assert!(schema.matches(&Value::from_vec(vec![1, 2, 3, 4])));
    assert!(!schema.matches(&Value::from_vec(vec![1i64, 2i64, 3i64, 4i64])));
    assert!(!schema.matches(&Value::from_vec(vec![Item::of(1), Item::of("hello")])));

    let with_attr = Value::Record(vec![Attr::of("name")], vec![Item::of(1), Item::of(10)]);
    assert!(schema.matches(&with_attr));
}

#[test]
fn map_record() {
    let schema = StandardSchema::map(
        StandardSchema::OfKind(ValueKind::Int32),
        StandardSchema::OfKind(ValueKind::Text),
    );

    let good_items = vec![Item::slot(2, "a"), Item::slot(-1, "b"), Item::slot(12, "c")];

    let bad_items = vec![
        Item::slot("a", 2),
        Item::slot(-1, false),
        Item::slot(12, "c"),
    ];

    assert!(schema.matches(&Value::empty_record()));
    assert!(schema.matches(&Value::from_vec(good_items.clone())));
    assert!(!schema.matches(&Value::from_vec(bad_items)));

    let with_attr = Value::Record(vec![Attr::of("name")], good_items.clone());
    assert!(schema.matches(&with_attr));
}

#[test]
fn record_unpacking() {
    let schema1 = AttrSchema::tag("name").and_then(StandardSchema::OfKind(ValueKind::Int32));
    let item_schema = ItemSchema::ValueItem(StandardSchema::OfKind(ValueKind::Int32));
    let schema2 = AttrSchema::tag("name").and_then(StandardSchema::Layout {
        items: vec![(item_schema, true)],
        exhaustive: true,
    });

    let record = Value::Record(vec![Attr::of("name")], vec![Item::of(3)]);

    assert!(schema1.matches(&record));
    assert!(schema2.matches(&record));
}

#[test]
fn of_kind_to_value() {
    assert_that!(
        StandardSchema::OfKind(ValueKind::Extant).to_value(),
        eq(Value::of_attr(Attr::of(("kind", "extant"))))
    );
    assert_that!(
        StandardSchema::OfKind(ValueKind::Int32).to_value(),
        eq(Value::of_attr(Attr::of(("kind", "int32"))))
    );
    assert_that!(
        StandardSchema::OfKind(ValueKind::Int64).to_value(),
        eq(Value::of_attr(Attr::of(("kind", "int64"))))
    );
    assert_that!(
        StandardSchema::OfKind(ValueKind::Float64).to_value(),
        eq(Value::of_attr(Attr::of(("kind", "float64"))))
    );
    assert_that!(
        StandardSchema::OfKind(ValueKind::Boolean).to_value(),
        eq(Value::of_attr(Attr::of(("kind", "boolean"))))
    );
    assert_that!(
        StandardSchema::OfKind(ValueKind::Text).to_value(),
        eq(Value::of_attr(Attr::of(("kind", "text"))))
    );
    assert_that!(
        StandardSchema::OfKind(ValueKind::Record).to_value(),
        eq(Value::of_attr(Attr::of(("kind", "record"))))
    );
}

#[test]
fn equal_to_value() {
    let value = StandardSchema::Equal(Value::from(0)).to_value();
    assert_that!(value, eq(Value::of_attr(("equal", 0))))
}

#[test]
fn in_int_range_to_value_min() {
    let schema = StandardSchema::InRangeInt(Range::new(Some(Bound::inclusive(12)), None));
    let value = schema.to_value();
    let expected = Value::of_attr((
        "in_range_int",
        Value::singleton((
            "min",
            Value::from_vec(vec![
                Item::slot("value", 12i64),
                Item::slot("inclusive", true),
            ]),
        )),
    ));
    assert_that!(value, eq(expected));
}

#[test]
fn in_int_range_to_value_max() {
    let schema = StandardSchema::InRangeInt(Range::new(None, Some(Bound::inclusive(12))));
    let value = schema.to_value();
    let expected = Value::of_attr((
        "in_range_int",
        Value::singleton((
            "max",
            Value::from_vec(vec![
                Item::slot("value", 12i64),
                Item::slot("inclusive", true),
            ]),
        )),
    ));
    assert_that!(value, eq(expected));
}

#[test]
fn in_int_range_to_value_both() {
    let schema = StandardSchema::InRangeInt(Range::new(
        Some(Bound::exclusive(-3)),
        Some(Bound::inclusive(12)),
    ));
    let value = schema.to_value();
    let expected = Value::of_attr((
        "in_range_int",
        Value::from_vec(vec![
            (
                "min",
                Value::from_vec(vec![
                    Item::slot("value", -3i64),
                    Item::slot("inclusive", false),
                ]),
            ),
            (
                "max",
                Value::from_vec(vec![
                    Item::slot("value", 12i64),
                    Item::slot("inclusive", true),
                ]),
            ),
        ]),
    ));
    assert_that!(value, eq(expected));
}

#[test]
fn in_float_range_to_value_min() {
    let schema = StandardSchema::InRangeFloat(Range::new(Some(Bound::exclusive(0.5)), None));
    let value = schema.to_value();
    let expected = Value::of_attr((
        "in_range_float",
        Value::singleton((
            "min",
            Value::from_vec(vec![
                Item::slot("value", 0.5),
                Item::slot("inclusive", false),
            ]),
        )),
    ));
    assert_that!(value, eq(expected));
}

#[test]
fn in_float_range_to_value_max() {
    let schema = StandardSchema::InRangeFloat(Range::new(None, Some(Bound::exclusive(0.5))));
    let value = schema.to_value();
    let expected = Value::of_attr((
        "in_range_float",
        Value::singleton((
            "max",
            Value::from_vec(vec![
                Item::slot("value", 0.5),
                Item::slot("inclusive", false),
            ]),
        )),
    ));
    assert_that!(value, eq(expected));
}

#[test]
fn in_float_range_to_value_both() {
    let schema = StandardSchema::InRangeFloat(Range::new(
        Some(Bound::inclusive(-0.5)),
        Some(Bound::exclusive(0.5)),
    ));
    let value = schema.to_value();
    let expected = Value::of_attr((
        "in_range_float",
        Value::from_vec(vec![
            (
                "min",
                Value::from_vec(vec![
                    Item::slot("value", -0.5),
                    Item::slot("inclusive", true),
                ]),
            ),
            (
                "max",
                Value::from_vec(vec![
                    Item::slot("value", 0.5),
                    Item::slot("inclusive", false),
                ]),
            ),
        ]),
    ));
    assert_that!(value, eq(expected));
}

#[test]
fn non_name_to_value() {
    assert_that!(
        StandardSchema::NonNan.to_value(),
        eq(Value::of_attr("non_nan"))
    )
}

#[test]
fn finite_to_value() {
    assert_that!(
        StandardSchema::Finite.to_value(),
        eq(Value::of_attr("finite"))
    )
}

#[test]
fn non_empty_text_to_value() {
    let schema = StandardSchema::Text(TextSchema::NonEmpty);
    let value = schema.to_value();
    let expected = Value::of_attrs(vec![Attr::of("text"), Attr::of("non_empty")]);
    assert_that!(value, eq(expected));
}

#[test]
fn specific_text_to_value() {
    let schema = StandardSchema::Text(TextSchema::Exact("hello".to_string()));
    let value = schema.to_value();
    let expected = Value::of_attrs(vec![Attr::of("text"), Attr::of(("equal", "hello"))]);
    assert_that!(value, eq(expected));
}

#[test]
fn regex_to_value() {
    let schema = StandardSchema::Text(TextSchema::regex("^ab*a$").unwrap());
    let value = schema.to_value();
    let expected = Value::of_attrs(vec![Attr::of("text"), Attr::of(("matches", "^ab*a$"))]);
    assert_that!(value, eq(expected));
}

#[test]
fn not_to_value() {
    let schema = StandardSchema::Equal(Value::from(1)).negate();
    let value = schema.to_value();
    let expected = Value::of_attr(("not", Value::of_attr(("equal", 1))));
    assert_that!(value, eq(expected));
}

#[test]
fn and_to_value() {
    let schema1 = StandardSchema::Equal(Value::from(1));
    let schema2 = StandardSchema::Equal(Value::from(2));
    let schema = schema1.and(schema2);
    let value = schema.to_value();
    let expected = Value::of_attr((
        "and",
        Value::from_vec(vec![
            Value::of_attr(("equal", 1)),
            Value::of_attr(("equal", 2)),
        ]),
    ));
    assert_that!(value, eq(expected));
}

#[test]
fn or_to_value() {
    let schema1 = StandardSchema::Equal(Value::from(1));
    let schema2 = StandardSchema::Equal(Value::from(2));
    let schema = schema1.or(schema2);
    let value = schema.to_value();
    let expected = Value::of_attr((
        "or",
        Value::from_vec(vec![
            Value::of_attr(("equal", 1)),
            Value::of_attr(("equal", 2)),
        ]),
    ));
    assert_that!(value, eq(expected));
}

#[test]
fn all_items_to_value() {
    let schema = StandardSchema::array(StandardSchema::Equal(Value::from(2)));
    let value = schema.to_value();
    let expected = Value::of_attr(("all_items", Value::of_attr(("equal", 2))));
    assert_that!(value, eq(expected));
}

#[test]
fn num_attrs_to_value() {
    let schema = StandardSchema::NumAttrs(4);
    let value = schema.to_value();
    let expected = Value::of_attr(("num_attrs", 4i64));
    assert_that!(value, eq(expected));
}

#[test]
fn num_items_to_value() {
    let schema = StandardSchema::NumItems(4);
    let value = schema.to_value();
    let expected = Value::of_attr(("num_items", 4i64));
    assert_that!(value, eq(expected));
}

#[test]
fn attr_schema_to_value() {
    let schema = AttrSchema::new(
        TextSchema::exact("name"),
        StandardSchema::Equal(Value::Int32Value(1)),
    );
    let value = schema.to_value();
    let expected = Value::of_attr((
        "attr",
        Value::from_vec(vec![
            Item::slot("name", Value::of_attr(("equal", "name"))),
            Item::slot("value", Value::of_attr(("equal", 1))),
        ]),
    ));
    assert_that!(value, eq(expected));
}

#[test]
fn head_schema_to_value() {
    let attr_schema = AttrSchema::new(
        TextSchema::exact("name"),
        StandardSchema::Equal(Value::Int32Value(1)),
    );
    let schema = StandardSchema::HeadAttribute {
        schema: Box::new(attr_schema.clone()),
        required: true,
        remainder: Box::new(StandardSchema::Anything),
    };
    let value = schema.to_value();

    let expected = Value::Record(
        vec![Attr::of(("head", Value::singleton(("required", true))))],
        vec![
            Item::slot("schema", attr_schema.to_value()),
            Item::slot("remainder", StandardSchema::Anything.to_value()),
        ],
    );

    assert_that!(value, eq(expected));
}

#[test]
fn field_spec_to_value() {
    let attr_schema = AttrSchema::new(
        TextSchema::exact("name1"),
        StandardSchema::Equal(Value::Int32Value(1)),
    );
    let field_schema = FieldSpec::new(attr_schema.clone(), true, false);
    let value = field_schema.to_value();

    let expected = Value::of_attrs(vec![
        Attr::of((
            "field",
            Value::from_vec(vec![
                Item::slot("required", true),
                Item::slot("unique", false),
            ]),
        )),
        attr_schema.to_attr(),
    ]);

    assert_that!(value, eq(expected));
}

#[test]
fn has_attributes_to_value() {
    let attr_schema1 = FieldSpec::new(
        AttrSchema::new(
            TextSchema::exact("name1"),
            StandardSchema::Equal(Value::Int32Value(1)),
        ),
        true,
        true,
    );
    let attr_schema2 = FieldSpec::new(
        AttrSchema::new(
            TextSchema::exact("name2"),
            StandardSchema::Equal(Value::Int32Value(2)),
        ),
        false,
        false,
    );

    let schema = StandardSchema::HasAttributes {
        attributes: vec![attr_schema1.clone(), attr_schema2.clone()],
        exhaustive: false,
    };
    let value = schema.to_value();

    let expected = Value::Record(
        vec![Attr::of((
            "has_attributes",
            Value::singleton(("exhaustive", false)),
        ))],
        vec![
            Item::ValueItem(attr_schema1.to_value()),
            Item::ValueItem(attr_schema2.to_value()),
        ],
    );

    assert_that!(value, eq(expected));
}

#[test]
fn slot_schema_to_value() {
    let schema = SlotSchema::new(
        StandardSchema::Text(TextSchema::exact("name")),
        StandardSchema::Equal(Value::Int32Value(1)),
    );
    let value = schema.to_value();
    let expected = Value::of_attr((
        "slot",
        Value::from_vec(vec![
            Item::slot(
                "key",
                Value::of_attrs(vec![Attr::of("text"), Attr::of(("equal", "name"))]),
            ),
            Item::slot("value", Value::of_attr(("equal", 1))),
        ]),
    ));
    assert_that!(value, eq(expected));
}

#[test]
fn has_slots_to_value() {
    let slot_schema1 = FieldSpec::new(
        SlotSchema::new(
            StandardSchema::Text(TextSchema::exact("name1")),
            StandardSchema::Equal(Value::Int32Value(1)),
        ),
        true,
        true,
    );
    let slot_schema2 = FieldSpec::new(
        SlotSchema::new(
            StandardSchema::Text(TextSchema::exact("name2")),
            StandardSchema::Equal(Value::Int32Value(2)),
        ),
        false,
        false,
    );

    let schema = StandardSchema::HasSlots {
        slots: vec![slot_schema1.clone(), slot_schema2.clone()],
        exhaustive: false,
    };
    let value = schema.to_value();

    let expected = Value::Record(
        vec![Attr::of((
            "has_slots",
            Value::singleton(("exhaustive", false)),
        ))],
        vec![
            Item::ValueItem(slot_schema1.to_value()),
            Item::ValueItem(slot_schema2.to_value()),
        ],
    );

    assert_that!(value, eq(expected));
}

#[test]
fn item_schema_to_value() {
    let slot_schema = SlotSchema::new(
        StandardSchema::Text(TextSchema::exact("name")),
        StandardSchema::Equal(Value::Int32Value(1)),
    );
    let slot_item_schema = ItemSchema::Field(slot_schema.clone());
    let value_item_schema = ItemSchema::ValueItem(StandardSchema::Anything);

    assert_that!(slot_item_schema.to_value(), eq(slot_schema.to_value()));
    assert_that!(
        value_item_schema.to_value(),
        eq(StandardSchema::Anything.to_value())
    );
}

#[test]
fn layout_to_value() {
    let slot_schema = SlotSchema::new(
        StandardSchema::Text(TextSchema::exact("name")),
        StandardSchema::Equal(Value::Int32Value(1)),
    );
    let item_schema1 = ItemSchema::Field(slot_schema.clone());
    let item_schema2 = ItemSchema::ValueItem(StandardSchema::Anything);
    let schema = StandardSchema::Layout {
        items: vec![(item_schema1.clone(), true), (item_schema2.clone(), false)],
        exhaustive: false,
    };

    let value = schema.to_value();

    let expected = Value::Record(
        vec![Attr::of((
            "layout",
            Value::from_vec(vec![("exhaustive", false)]),
        ))],
        vec![
            Item::ValueItem(item_schema1.to_value().prepend(Attr::of((
                "item",
                Value::from_vec(vec![("required", true)]),
            )))),
            Item::ValueItem(item_schema2.to_value().prepend(Attr::of((
                "item",
                Value::from_vec(vec![("required", false)]),
            )))),
        ],
    );

    assert_that!(value, eq(expected));
}

#[test]
fn anything_to_value() {
    assert_that!(
        StandardSchema::Anything.to_value(),
        eq(Value::of_attr("anything"))
    );
}

#[test]
fn nothing_to_value() {
    assert_that!(
        StandardSchema::Nothing.to_value(),
        eq(Value::of_attr("nothing"))
    );
}

#[test]
fn compare_upper_range_bounds_inclusive_same_value() {
    assert_eq!(
        RangeBound::Upper(Bound::inclusive(10)),
        RangeBound::Upper(Bound::inclusive(10))
    );
    assert_eq!(
        RangeBound::Upper(Bound::inclusive(-10)),
        RangeBound::Upper(Bound::inclusive(-10))
    );
}

#[test]
fn compare_upper_range_bounds_exclusive_same_value() {
    assert_eq!(
        RangeBound::Upper(Bound::exclusive(2.23)),
        RangeBound::Upper(Bound::exclusive(2.23))
    );
    assert_eq!(
        RangeBound::Upper(Bound::exclusive(-11.3)),
        RangeBound::Upper(Bound::exclusive(-11.3))
    );
}

#[test]
fn compare_upper_range_bounds_inclusive_diff_values() {
    assert!(RangeBound::Upper(Bound::inclusive(75.1)) > RangeBound::Upper(Bound::inclusive(-1.5)));
    assert!(
        RangeBound::Upper(Bound::inclusive(-0.11)) < RangeBound::Upper(Bound::inclusive(-0.10))
    );
}

#[test]
fn compare_upper_range_bounds_exclusive_diff_values() {
    assert!(RangeBound::Upper(Bound::exclusive(91)) > RangeBound::Upper(Bound::exclusive(1)));
    assert!(RangeBound::Upper(Bound::exclusive(-5)) < RangeBound::Upper(Bound::exclusive(-1)));
}

#[test]
fn compare_upper_range_bounds_diff_types_same_value() {
    assert!(RangeBound::Upper(Bound::exclusive(10)) < RangeBound::Upper(Bound::inclusive(10)));
    assert!(RangeBound::Upper(Bound::inclusive(0.5)) > RangeBound::Upper(Bound::exclusive(0.5)));
    assert!(RangeBound::Upper(Bound::exclusive(-78)) < RangeBound::Upper(Bound::inclusive(-78)));
    assert!(
        RangeBound::Upper(Bound::inclusive(-1.37)) > RangeBound::Upper(Bound::exclusive(-1.37))
    );
}

#[test]
fn compare_upper_range_bounds_diff_types_diff_values() {
    assert!(RangeBound::Upper(Bound::exclusive(10)) > RangeBound::Upper(Bound::inclusive(9)));
    assert!(RangeBound::Upper(Bound::inclusive(0.5)) < RangeBound::Upper(Bound::exclusive(0.6)));
    assert!(RangeBound::Upper(Bound::exclusive(-1)) > RangeBound::Upper(Bound::inclusive(-78)));
    assert!(
        RangeBound::Upper(Bound::inclusive(-1.37)) < RangeBound::Upper(Bound::exclusive(-1.00001))
    );
}

#[test]
fn compare_lower_range_bounds_inclusive_same_value() {
    assert_eq!(
        RangeBound::Lower(Bound::inclusive(15)),
        RangeBound::Lower(Bound::inclusive(15))
    );
    assert_eq!(
        RangeBound::Lower(Bound::inclusive(-15)),
        RangeBound::Lower(Bound::inclusive(-15))
    );
}

#[test]
fn compare_lower_range_bounds_exclusive_same_value() {
    assert_eq!(
        RangeBound::Lower(Bound::exclusive(17.23)),
        RangeBound::Lower(Bound::exclusive(17.23))
    );
    assert_eq!(
        RangeBound::Lower(Bound::exclusive(-16.3)),
        RangeBound::Lower(Bound::exclusive(-16.3))
    );
}

#[test]
fn compare_lower_range_bounds_inclusive_diff_values() {
    assert!(RangeBound::Lower(Bound::inclusive(75.1)) < RangeBound::Lower(Bound::inclusive(-1.5)));
    assert!(
        RangeBound::Lower(Bound::inclusive(-0.11)) > RangeBound::Lower(Bound::inclusive(-0.10))
    );
}

#[test]
fn compare_lower_range_bounds_exclusive_diff_values() {
    assert!(RangeBound::Lower(Bound::exclusive(91)) < RangeBound::Lower(Bound::exclusive(1)));
    assert!(RangeBound::Lower(Bound::exclusive(-5)) > RangeBound::Lower(Bound::exclusive(-1)));
}

#[test]
fn compare_lower_range_bounds_diff_types_same_value() {
    assert!(RangeBound::Lower(Bound::exclusive(10)) < RangeBound::Lower(Bound::inclusive(10)));
    assert!(RangeBound::Lower(Bound::inclusive(0.5)) > RangeBound::Lower(Bound::exclusive(0.5)));
    assert!(RangeBound::Lower(Bound::exclusive(-78)) < RangeBound::Lower(Bound::inclusive(-78)));
    assert!(
        RangeBound::Lower(Bound::inclusive(-1.37)) > RangeBound::Lower(Bound::exclusive(-1.37))
    );
}

#[test]
fn compare_lower_range_bounds_diff_types_diff_values() {
    assert!(RangeBound::Lower(Bound::exclusive(10)) < RangeBound::Lower(Bound::inclusive(9)));
    assert!(RangeBound::Lower(Bound::inclusive(0.5)) > RangeBound::Lower(Bound::exclusive(0.6)));
    assert!(RangeBound::Lower(Bound::exclusive(-1)) < RangeBound::Lower(Bound::inclusive(-78)));
    assert!(
        RangeBound::Lower(Bound::inclusive(-1.37)) > RangeBound::Lower(Bound::exclusive(-1.000001))
    );
}

#[test]
fn compare_unbounded_ranges() {
    let first: Range<i64> = Range::new(None, None);
    let second: Range<i64> = Range::new(None, None);

    assert_eq!(first, second)
}

#[test]
fn compare_unbounded_range_to_any() {
    let first: Range<i64> = Range::new(None, None);

    assert!(first > Range::new(None, Some(Bound::exclusive(10))));
    assert!(Range::new(None, Some(Bound::exclusive(-100))) < first);
    assert!(first > Range::new(Some(Bound::inclusive(-100)), None));
    assert!(Range::new(Some(Bound::exclusive(160)), None) < first);
    assert!(first > Range::new(Some(Bound::inclusive(11)), Some(Bound::exclusive(51))));
    assert!(Range::new(Some(Bound::exclusive(-32)), Some(Bound::inclusive(51))) < first);
}

#[test]
fn compare_bounded_ranges_equal() {
    assert_eq!(
        Range::new(Some(Bound::exclusive(10)), Some(Bound::exclusive(72))),
        Range::new(Some(Bound::exclusive(10)), Some(Bound::exclusive(72)))
    );

    assert_eq!(
        Range::new(Some(Bound::inclusive(-100)), Some(Bound::exclusive(15))),
        Range::new(Some(Bound::inclusive(-100)), Some(Bound::exclusive(15)))
    );
}

#[test]
fn compare_bounded_ranges_different() {
    assert!(
        Range::new(Some(Bound::inclusive(-5)), Some(Bound::inclusive(5)))
            < Range::new(Some(Bound::inclusive(-10)), Some(Bound::inclusive(10)))
    );

    assert!(
        Range::new(Some(Bound::inclusive(-3.5)), Some(Bound::inclusive(3.1)))
            < Range::new(Some(Bound::inclusive(-4.5)), Some(Bound::inclusive(3.2)))
    );

    assert!(
        Range::new(Some(Bound::exclusive(5)), Some(Bound::exclusive(15)))
            > Range::new(Some(Bound::exclusive(8)), Some(Bound::inclusive(12)))
    );

    assert!(
        Range::new(Some(Bound::exclusive(0.11)), Some(Bound::exclusive(0.99)))
            > Range::new(Some(Bound::exclusive(0.12)), Some(Bound::inclusive(0.88)))
    );

    assert!(
        Range::new(Some(Bound::exclusive(-5.5)), Some(Bound::inclusive(5.5)))
            < Range::new(Some(Bound::inclusive(-5.5)), Some(Bound::inclusive(15.5)))
    );

    assert!(
        Range::new(Some(Bound::inclusive(-12.2)), Some(Bound::inclusive(12.2)))
            > Range::new(Some(Bound::exclusive(-12.2)), Some(Bound::inclusive(2.2)))
    );

    assert!(
        Range::new(Some(Bound::inclusive(-5.5)), Some(Bound::exclusive(5.5)))
            < Range::new(Some(Bound::exclusive(-15.5)), Some(Bound::inclusive(5.5)))
    );

    assert!(
        Range::new(Some(Bound::inclusive(-12.2)), Some(Bound::inclusive(2.2)))
            > Range::new(Some(Bound::inclusive(-12.2)), Some(Bound::exclusive(2.2)))
    );

    assert!(
        Range::new(Some(Bound::exclusive(-5.5)), Some(Bound::exclusive(5.5)))
            < Range::new(Some(Bound::inclusive(-5.5)), Some(Bound::inclusive(5.5)))
    );

    assert!(
        Range::new(Some(Bound::inclusive(-12.2)), Some(Bound::inclusive(12.2)))
            > Range::new(Some(Bound::exclusive(-12.2)), Some(Bound::exclusive(12.2)))
    );
}

#[test]
fn compare_bounded_ranges_not_related() {
    assert!(
        Range::new(Some(Bound::inclusive(2.2)), Some(Bound::inclusive(5.5)))
            .partial_cmp(&Range::new(
                Some(Bound::inclusive(-2.2)),
                Some(Bound::inclusive(-5.5))
            ))
            .is_none()
    );

    assert!(
        Range::new(Some(Bound::inclusive(-2.2)), Some(Bound::inclusive(5.5)))
            .partial_cmp(&Range::new(
                Some(Bound::inclusive(6.6)),
                Some(Bound::inclusive(8.8))
            ))
            .is_none()
    );

    assert!(
        Range::new(Some(Bound::inclusive(-2.2)), Some(Bound::inclusive(5.5)))
            .partial_cmp(&Range::new(
                Some(Bound::inclusive(-1.1)),
                Some(Bound::inclusive(6.6))
            ))
            .is_none()
    );

    assert!(
        Range::new(Some(Bound::exclusive(-2.2)), Some(Bound::exclusive(5.5)))
            .partial_cmp(&Range::new(
                Some(Bound::exclusive(-3.3)),
                Some(Bound::exclusive(4.4))
            ))
            .is_none()
    );

    assert!(
        Range::new(Some(Bound::exclusive(-2.2)), Some(Bound::inclusive(5.5)))
            .partial_cmp(&Range::new(
                Some(Bound::inclusive(-2.2)),
                Some(Bound::inclusive(4.4))
            ))
            .is_none()
    );

    assert!(
        Range::new(Some(Bound::inclusive(33)), Some(Bound::inclusive(44)))
            .partial_cmp(&Range::new(
                Some(Bound::inclusive(22)),
                Some(Bound::exclusive(44))
            ))
            .is_none()
    );

    assert!(
        Range::new(Some(Bound::exclusive(-11)), Some(Bound::inclusive(11)))
            .partial_cmp(&Range::new(
                Some(Bound::inclusive(-11)),
                Some(Bound::exclusive(11))
            ))
            .is_none()
    );

    assert!(
        Range::new(Some(Bound::inclusive(-33)), Some(Bound::exclusive(33)))
            .partial_cmp(&Range::new(
                Some(Bound::exclusive(-33)),
                Some(Bound::inclusive(33))
            ))
            .is_none()
    );
}

#[test]
fn compare_upper_bounded_and_bounded_related() {
    assert!(
        Range::new(None, Some(Bound::inclusive(15)))
            > Range::new(Some(Bound::exclusive(-10)), Some(Bound::exclusive(10)))
    );

    assert!(
        Range::new(Some(Bound::exclusive(-1)), Some(Bound::exclusive(1)))
            < Range::new(None, Some(Bound::inclusive(11)))
    );

    assert!(
        Range::new(None, Some(Bound::inclusive(15.5)))
            > Range::new(Some(Bound::exclusive(-10.1)), Some(Bound::exclusive(15.5)))
    );

    assert!(
        Range::new(None, Some(Bound::exclusive(15.5)))
            > Range::new(Some(Bound::exclusive(-10.1)), Some(Bound::exclusive(15.5)))
    );

    assert!(
        Range::new(Some(Bound::exclusive(-1.1)), Some(Bound::exclusive(11.5)))
            < Range::new(None, Some(Bound::inclusive(11.5)))
    );

    assert!(
        Range::new(Some(Bound::exclusive(-1.1)), Some(Bound::exclusive(-0.11)))
            < Range::new(None, Some(Bound::exclusive(-0.11)))
    );
}

#[test]
fn compare_upper_bounded_and_bounded_not_related() {
    assert!(Range::new(None, Some(Bound::exclusive(10.10)))
        .partial_cmp(&Range::new(
            Some(Bound::exclusive(20.20)),
            Some(Bound::exclusive(25.25))
        ))
        .is_none());

    assert!(Range::new(
        Some(Bound::exclusive(-25.25)),
        Some(Bound::exclusive(-20.20))
    )
    .partial_cmp(&Range::new(None, Some(Bound::exclusive(-30.30))))
    .is_none());

    assert!(Range::new(None, Some(Bound::exclusive(10.10)))
        .partial_cmp(&Range::new(
            Some(Bound::inclusive(10.10)),
            Some(Bound::inclusive(25.25))
        ))
        .is_none());

    assert!(Range::new(
        Some(Bound::inclusive(-25.25)),
        Some(Bound::inclusive(-20.20))
    )
    .partial_cmp(&Range::new(None, Some(Bound::inclusive(-25.25))))
    .is_none());

    assert!(Range::new(None, Some(Bound::exclusive(33.33)))
        .partial_cmp(&Range::new(
            Some(Bound::inclusive(10.10)),
            Some(Bound::inclusive(33.33))
        ))
        .is_none());

    assert!(Range::new(
        Some(Bound::inclusive(-11.25)),
        Some(Bound::inclusive(-7.33))
    )
    .partial_cmp(&Range::new(None, Some(Bound::exclusive(-7.33))))
    .is_none());
}

#[test]
fn compare_lower_bounded_and_bounded_related() {
    assert!(
        Range::new(Some(Bound::inclusive(-100)), None)
            > Range::new(Some(Bound::exclusive(-10)), Some(Bound::exclusive(10)))
    );

    assert!(
        Range::new(Some(Bound::exclusive(-1)), Some(Bound::exclusive(1)))
            < Range::new(Some(Bound::inclusive(-22)), None)
    );

    assert!(
        Range::new(Some(Bound::inclusive(15.5)), None)
            > Range::new(Some(Bound::exclusive(15.5)), Some(Bound::exclusive(200.22)))
    );

    assert!(
        Range::new(Some(Bound::inclusive(15.5)), None)
            > Range::new(Some(Bound::inclusive(15.5)), Some(Bound::exclusive(200.22)))
    );

    assert!(
        Range::new(Some(Bound::exclusive(-11.1)), Some(Bound::exclusive(11.1)))
            < Range::new(Some(Bound::inclusive(-11.1)), None)
    );

    assert!(
        Range::new(Some(Bound::exclusive(-0.11)), Some(Bound::exclusive(0.11)))
            < Range::new(Some(Bound::exclusive(-0.11)), None)
    );
}

#[test]
fn compare_lower_bounded_and_bounded_not_related() {
    assert!(Range::new(Some(Bound::exclusive(10.10)), None)
        .partial_cmp(&Range::new(
            Some(Bound::exclusive(1.1)),
            Some(Bound::exclusive(5.5))
        ))
        .is_none());

    assert!(Range::new(
        Some(Bound::exclusive(-33.33)),
        Some(Bound::exclusive(-22.22))
    )
    .partial_cmp(&Range::new(Some(Bound::exclusive(-20.20)), None))
    .is_none());

    assert!(Range::new(Some(Bound::exclusive(12.12)), None)
        .partial_cmp(&Range::new(
            Some(Bound::inclusive(1.1)),
            Some(Bound::inclusive(12.12))
        ))
        .is_none());

    assert!(Range::new(
        Some(Bound::inclusive(-30.30)),
        Some(Bound::inclusive(-25.25))
    )
    .partial_cmp(&Range::new(Some(Bound::inclusive(-25.25)), None))
    .is_none());

    assert!(Range::new(Some(Bound::exclusive(3.3)), None)
        .partial_cmp(&Range::new(
            Some(Bound::inclusive(3.3)),
            Some(Bound::inclusive(10.10))
        ))
        .is_none());

    assert!(Range::new(
        Some(Bound::inclusive(-11.25)),
        Some(Bound::inclusive(-7.33))
    )
    .partial_cmp(&Range::new(Some(Bound::exclusive(-7.33)), None))
    .is_none());

    assert!(Range::new(Some(Bound::exclusive(10.10)), None)
        .partial_cmp(&Range::new(
            Some(Bound::inclusive(10.10)),
            Some(Bound::inclusive(25.25))
        ))
        .is_none());
}

#[test]
fn compare_upper_bounded() {
    assert_eq!(
        Range::new(None, Some(Bound::exclusive(10.10))),
        Range::new(None, Some(Bound::exclusive(10.10)))
    );

    assert!(
        Range::new(None, Some(Bound::exclusive(15.15)))
            > Range::new(None, Some(Bound::exclusive(10.10)))
    );

    assert!(
        Range::new(None, Some(Bound::exclusive(-15.15)))
            < Range::new(None, Some(Bound::exclusive(-10.10)))
    );

    assert!(
        Range::new(None, Some(Bound::inclusive(10.10)))
            > Range::new(None, Some(Bound::exclusive(10.10)))
    );

    assert!(
        Range::new(None, Some(Bound::exclusive(-10.10)))
            < Range::new(None, Some(Bound::inclusive(-10.10)))
    );
}

#[test]
fn compare_lower_bounded() {
    assert_eq!(
        Range::new(Some(Bound::exclusive(3.14)), None),
        Range::new(Some(Bound::exclusive(3.14)), None)
    );

    assert!(
        Range::new(Some(Bound::exclusive(15.15)), None)
            < Range::new(Some(Bound::exclusive(10.10)), None)
    );

    assert!(
        Range::new(Some(Bound::exclusive(-15.15)), None)
            > Range::new(Some(Bound::exclusive(-10.10)), None)
    );

    assert!(
        Range::new(Some(Bound::exclusive(10.10)), None)
            < Range::new(Some(Bound::inclusive(10.10)), None)
    );

    assert!(
        Range::new(Some(Bound::inclusive(-10.10)), None)
            > Range::new(Some(Bound::exclusive(-10.10)), None)
    );
}

#[test]
fn compare_upper_and_lower_bounded() {
    assert!(Range::new(None, Some(Bound::exclusive(10.10)))
        .partial_cmp(&Range::new(Some(Bound::exclusive(10.10)), None))
        .is_none());

    assert!(Range::new(Some(Bound::exclusive(-10.10)), None)
        .partial_cmp(&Range::new(None, Some(Bound::exclusive(10.10))))
        .is_none());
}

fn assert_greater_than(schema: StandardSchema, cmp_schemas: Vec<StandardSchema>) {
    for s in cmp_schemas {
        assert!(schema > s);
        assert!(s < schema);
    }
}
fn assert_less_than(schema: StandardSchema, cmp_schemas: Vec<StandardSchema>) {
    for s in cmp_schemas {
        assert!(schema < s);
        assert!(s > schema);
    }
}

fn assert_equal(schema: StandardSchema, cmp_schemas: Vec<StandardSchema>) {
    for s in cmp_schemas {
        assert_eq!(schema.partial_cmp(&s).unwrap(), Ordering::Equal);
        assert_eq!(s.partial_cmp(&schema).unwrap(), Ordering::Equal);
    }
}

fn assert_not_related(schema: StandardSchema, cmp_schemas: Vec<StandardSchema>) {
    for s in cmp_schemas {
        assert!(schema.partial_cmp(&s).is_none());
        assert!(s.partial_cmp(&schema).is_none());
    }
}

fn all_schemas() -> HashMap<&'static str, StandardSchema> {
    let mut map = HashMap::new();

    map.insert("of_kind", StandardSchema::OfKind(ValueKind::Extant));
    map.insert("equal", StandardSchema::Equal(Value::Extant));
    map.insert(
        "in_range_int",
        StandardSchema::InRangeInt(Range::new(
            Some(Bound::inclusive(0)),
            Some(Bound::inclusive(10)),
        )),
    );
    map.insert(
        "in_range_float",
        StandardSchema::InRangeFloat(Range::new(
            Some(Bound::inclusive(0.5)),
            Some(Bound::inclusive(10.5)),
        )),
    );
    map.insert("non_nan", StandardSchema::NonNan);
    map.insert("finite", StandardSchema::Finite);
    map.insert("text", StandardSchema::Text(TextSchema::NonEmpty));
    map.insert(
        "not",
        StandardSchema::Not(Box::from(StandardSchema::Anything)),
    );
    map.insert("and", StandardSchema::And(vec![]));
    map.insert("or", StandardSchema::Or(vec![]));
    map.insert(
        "all_items",
        StandardSchema::AllItems(Box::from(ItemSchema::ValueItem(StandardSchema::Anything))),
    );
    map.insert("num_attrs", StandardSchema::NumAttrs(5));
    map.insert("num_items", StandardSchema::NumItems(3));
    map.insert(
        "head_attribute",
        StandardSchema::HeadAttribute {
            schema: Box::from(AttrSchema::new(
                TextSchema::NonEmpty,
                StandardSchema::Anything,
            )),
            required: true,
            remainder: Box::from(StandardSchema::Anything),
        },
    );
    map.insert(
        "has_attributes",
        StandardSchema::HasAttributes {
            attributes: vec![],
            exhaustive: true,
        },
    );
    map.insert(
        "has_slots",
        StandardSchema::HasSlots {
            slots: vec![],
            exhaustive: true,
        },
    );
    map.insert(
        "layout",
        StandardSchema::Layout {
            items: vec![],
            exhaustive: true,
        },
    );
    map.insert("nothing", StandardSchema::Nothing);
    map.insert("anything", StandardSchema::Anything);

    map
}

#[test]
fn compare_anything() {
    let schema = StandardSchema::Anything;
    let mut cmp_schemas = all_schemas();
    cmp_schemas.remove("anything");

    assert_greater_than(
        schema,
        cmp_schemas.into_iter().map(|(_, schema)| schema).collect(),
    );
}

#[test]
fn compare_nothing() {
    let schema = StandardSchema::Nothing;
    let mut cmp_schemas = all_schemas();
    cmp_schemas.remove("nothing");

    assert_less_than(
        schema,
        cmp_schemas.into_iter().map(|(_, schema)| schema).collect(),
    );
}

#[test]
fn compare_of_kind_extant() {
    let schema = StandardSchema::OfKind(ValueKind::Extant);
    let equal_schemas = vec![StandardSchema::Equal(Value::Extant)];

    assert_equal(schema, equal_schemas)
}

#[test]
fn compare_of_kind_i32() {
    let schema = StandardSchema::OfKind(ValueKind::Int32);
    let greater_schemas = vec![
        StandardSchema::OfKind(ValueKind::Int64),
        StandardSchema::InRangeInt(Range::new(
            Some(Bound::exclusive(i32::MIN as i64 - 1)),
            Some(Bound::exclusive(i32::MAX as i64 + 1)),
        )),
    ];

    let lesser_schemas = vec![
        StandardSchema::Equal(Value::Int32Value(10)),
        StandardSchema::Equal(Value::Int64Value(20)),
        StandardSchema::InRangeInt(Range::new(
            Some(Bound::inclusive(10)),
            Some(Bound::inclusive(20)),
        )),
    ];

    let equal_schemas = vec![
        StandardSchema::OfKind(ValueKind::Int32),
        StandardSchema::InRangeInt(Range::new(
            Some(Bound::inclusive(i32::MIN as i64)),
            Some(Bound::inclusive(i32::MAX as i64)),
        )),
    ];

    let not_related_schemas = vec![
        StandardSchema::Equal(Value::Int64Value(i32::MAX as i64 + 1)),
        StandardSchema::InRangeInt(Range::new(
            Some(Bound::inclusive(i32::MIN as i64 - 1)),
            Some(Bound::inclusive(20)),
        )),
    ];

    assert_less_than(schema.clone(), greater_schemas);
    assert_greater_than(schema.clone(), lesser_schemas);
    assert_equal(schema.clone(), equal_schemas);
    assert_not_related(schema, not_related_schemas);
}

#[test]
fn compare_of_kind_i64() {
    let schema = StandardSchema::OfKind(ValueKind::Int64);

    let lesser_schemas = vec![
        StandardSchema::OfKind(ValueKind::Int32),
        StandardSchema::Equal(Value::Int32Value(10)),
        StandardSchema::Equal(Value::Int64Value(20)),
        StandardSchema::InRangeInt(Range::new(
            Some(Bound::inclusive(0)),
            Some(Bound::inclusive(10)),
        )),
    ];

    let equal_schemas = vec![
        StandardSchema::OfKind(ValueKind::Int64),
        StandardSchema::InRangeInt(Range::new(
            Some(Bound::inclusive(i64::MIN)),
            Some(Bound::inclusive(i64::MAX)),
        )),
    ];

    assert_greater_than(schema.clone(), lesser_schemas);
    assert_equal(schema, equal_schemas);
}

#[test]
fn compare_of_kind_f64() {
    let schema = StandardSchema::OfKind(ValueKind::Float64);
    let lesser_schemas = vec![
        StandardSchema::Finite,
        StandardSchema::NonNan,
        StandardSchema::InRangeFloat(Range::new(
            Some(Bound::inclusive(0.0)),
            Some(Bound::inclusive(10.0)),
        )),
        StandardSchema::Equal(Value::Float64Value(10.0)),
    ];

    let equal_schemas = vec![
        StandardSchema::OfKind(ValueKind::Float64),
        StandardSchema::InRangeFloat(Range::new(
            Some(Bound::inclusive(f64::MIN)),
            Some(Bound::inclusive(f64::MAX)),
        )),
    ];

    assert_greater_than(schema.clone(), lesser_schemas);
    assert_equal(schema, equal_schemas);
}

#[test]
fn compare_of_kind_boolean() {
    let schema = StandardSchema::OfKind(ValueKind::Boolean);

    let lesser_schemas = vec![
        StandardSchema::Equal(Value::BooleanValue(true)),
        StandardSchema::Equal(Value::BooleanValue(false)),
    ];

    let equal_schemas = vec![StandardSchema::OfKind(ValueKind::Boolean)];

    assert_greater_than(schema.clone(), lesser_schemas);
    assert_equal(schema, equal_schemas);
}

#[test]
fn compare_of_kind_text() {
    let schema = StandardSchema::OfKind(ValueKind::Text);
    let lesser_schemas = vec![
        StandardSchema::Text(TextSchema::NonEmpty),
        StandardSchema::Text(TextSchema::Exact("foo".to_string())),
        StandardSchema::Text(TextSchema::regex("^ab*a$").unwrap()),
        StandardSchema::Equal(Value::Text("qux".to_string())),
    ];

    let equal_schemas = vec![StandardSchema::OfKind(ValueKind::Text)];

    assert_greater_than(schema.clone(), lesser_schemas);
    assert_equal(schema, equal_schemas);
}

#[test]
fn compare_of_kind_record() {
    let schema = StandardSchema::OfKind(ValueKind::Record);

    let lesser_schemas = vec![
        StandardSchema::Equal(Value::Record(vec![], vec![])),
        StandardSchema::AllItems(Box::from(ItemSchema::ValueItem(StandardSchema::Anything))),
        StandardSchema::NumItems(5),
        StandardSchema::NumAttrs(10),
        StandardSchema::HeadAttribute {
            schema: Box::from(AttrSchema::new(
                TextSchema::NonEmpty,
                StandardSchema::Anything,
            )),
            required: true,
            remainder: Box::from(StandardSchema::Anything),
        },
        StandardSchema::HasAttributes {
            attributes: vec![],
            exhaustive: true,
        },
        StandardSchema::HasSlots {
            slots: vec![],
            exhaustive: true,
        },
        StandardSchema::Layout {
            items: vec![],
            exhaustive: true,
        },
    ];
    let equal_schemas = vec![StandardSchema::OfKind(ValueKind::Record)];

    assert_greater_than(schema.clone(), lesser_schemas);
    assert_equal(schema, equal_schemas);
}

#[test]
fn compare_equal_i32() {
    let schema = StandardSchema::Equal(Value::Int32Value(42));

    let greater_schemas = vec![StandardSchema::InRangeInt(Range::new(
        Some(Bound::inclusive(10)),
        Some(Bound::exclusive(55)),
    ))];
    let equal_schemas = vec![
        StandardSchema::Equal(Value::Int32Value(42)),
        StandardSchema::Equal(Value::Int64Value(42)),
    ];

    assert_less_than(schema.clone(), greater_schemas);
    assert_equal(schema, equal_schemas);
}

#[test]
fn compare_equal_i64() {
    let schema = StandardSchema::Equal(Value::Int64Value(24));

    let greater_schemas = vec![StandardSchema::InRangeInt(Range::new(
        Some(Bound::inclusive(24)),
        Some(Bound::exclusive(30)),
    ))];
    let equal_schemas = vec![
        StandardSchema::Equal(Value::Int32Value(24)),
        StandardSchema::Equal(Value::Int64Value(24)),
    ];

    assert_less_than(schema.clone(), greater_schemas);
    assert_equal(schema, equal_schemas);
}

#[test]
fn compare_equal_f64() {
    let schema = StandardSchema::Equal(Value::Float64Value(15.15));

    let greater_schemas = vec![
        StandardSchema::InRangeFloat(Range::new(
            Some(Bound::inclusive(-10.10)),
            Some(Bound::exclusive(30.30)),
        )),
        StandardSchema::NonNan,
        StandardSchema::Finite,
    ];

    let equal_schemas = vec![StandardSchema::Equal(Value::Float64Value(15.15))];

    assert_less_than(schema.clone(), greater_schemas);
    assert_equal(schema.clone(), equal_schemas);
}

#[test]
fn compare_equal_text() {
    let schema = StandardSchema::Equal(Value::Text("this_is_a_test".to_string()));

    let greater_schemas = vec![
        StandardSchema::Text(TextSchema::NonEmpty),
        StandardSchema::Text(TextSchema::Matches(Regex::new("\\w+").unwrap())),
    ];

    let equal_schemas = vec![
        StandardSchema::Equal(Value::Text("this_is_a_test".to_string())),
        StandardSchema::Text(TextSchema::Exact("this_is_a_test".to_string())),
    ];

    assert_less_than(schema.clone(), greater_schemas);
    assert_equal(schema.clone(), equal_schemas);
}

#[test]
fn compare_equal_record() {
    let schema = StandardSchema::Equal(Value::Record(
        vec![Attr::of(("foo", 1)), Attr::of("1235"), Attr::of("1234")],
        vec![
            Item::Slot(Value::Int32Value(12), Value::Int32Value(23)),
            Item::Slot(Value::Int32Value(34), Value::Int32Value(45)),
        ],
    ));

    let greater_schemas = vec![
        StandardSchema::AllItems(Box::new(ItemSchema::Field(SlotSchema::new(
            StandardSchema::OfKind(ValueKind::Int32),
            StandardSchema::OfKind(ValueKind::Int32),
        )))),
        StandardSchema::NumAttrs(3),
        StandardSchema::NumItems(2),
        StandardSchema::HeadAttribute {
            schema: Box::new(AttrSchema::new(
                TextSchema::NonEmpty,
                StandardSchema::InRangeInt(Range::new(
                    Some(Bound::inclusive(0)),
                    Some(Bound::inclusive(15)),
                )),
            )),
            required: true,
            remainder: Box::new(StandardSchema::NumAttrs(2)),
        },
        StandardSchema::HasAttributes {
            attributes: vec![
                FieldSpec::new(
                    AttrSchema::new(TextSchema::NonEmpty, StandardSchema::Anything),
                    true,
                    false,
                ),
                FieldSpec::new(
                    AttrSchema::new(
                        TextSchema::Matches(Regex::new("1234").unwrap()),
                        StandardSchema::Anything,
                    ),
                    true,
                    false,
                ),
                FieldSpec::new(
                    AttrSchema::new(
                        TextSchema::Matches(Regex::new("123.").unwrap()),
                        StandardSchema::Anything,
                    ),
                    true,
                    false,
                ),
            ],
            exhaustive: true,
        },
        StandardSchema::HasSlots {
            slots: vec![
                FieldSpec::new(
                    SlotSchema::new(
                        StandardSchema::Equal(Value::Int32Value(12)),
                        StandardSchema::OfKind(ValueKind::Int32),
                    ),
                    true,
                    false,
                ),
                FieldSpec::new(
                    SlotSchema::new(
                        StandardSchema::Equal(Value::Int32Value(34)),
                        StandardSchema::OfKind(ValueKind::Int32),
                    ),
                    true,
                    false,
                ),
            ],
            exhaustive: true,
        },
        StandardSchema::Layout {
            items: vec![
                (
                    ItemSchema::Field(SlotSchema::new(
                        StandardSchema::Equal(Value::Int32Value(12)),
                        StandardSchema::Equal(Value::Int32Value(23)),
                    )),
                    true,
                ),
                (
                    ItemSchema::Field(SlotSchema::new(
                        StandardSchema::Equal(Value::Int32Value(34)),
                        StandardSchema::Equal(Value::Int32Value(45)),
                    )),
                    true,
                ),
            ],
            exhaustive: true,
        },
    ];

    let equal_schemas = vec![schema.clone()];

    assert_less_than(schema.clone(), greater_schemas);
    assert_equal(schema.clone(), equal_schemas);
}
