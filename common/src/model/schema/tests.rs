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
use crate::model::blob::Blob;
use hamcrest2::assert_that;
use hamcrest2::prelude::*;
use regex::Regex;
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

const KINDS: [ValueKind; 8] = [
    ValueKind::Extant,
    ValueKind::Int32,
    ValueKind::Int64,
    ValueKind::Float64,
    ValueKind::Boolean,
    ValueKind::Text,
    ValueKind::Record,
    ValueKind::Data,
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
    map.insert(ValueKind::Data, Value::Data(Blob::encode("swimming")));
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
fn optional_attributes_partial_exhaustive() {
    let schema = StandardSchema::HasAttributes {
        attributes: vec![
            FieldSpec::new(
                AttrSchema::new(
                    TextSchema::NonEmpty,
                    StandardSchema::Equal(Value::Int32Value(1)),
                ),
                false,
                true,
            ),
            FieldSpec::new(
                AttrSchema::new(
                    TextSchema::NonEmpty,
                    StandardSchema::Equal(Value::Int32Value(2)),
                ),
                false,
                true,
            ),
        ],
        exhaustive: true,
    };

    let good1 = Value::Record(vec![Attr::of(("foo", 1)), Attr::of(("bar", 2))], vec![]);
    let good2 = Value::Record(vec![Attr::of(("foo", 1)), Attr::of(("foo", 2))], vec![]);

    assert!(schema.matches(&good1));
    assert!(schema.matches(&good2));

    let bad1 = Value::Record(vec![Attr::of(("foo", "bar")), Attr::of(("bar", 2))], vec![]);
    assert!(!schema.matches(&bad1));
}

#[test]
fn optional_attributes_partial_non_exhaustive() {
    let schema = StandardSchema::HasAttributes {
        attributes: vec![
            FieldSpec::new(
                AttrSchema::new(
                    TextSchema::NonEmpty,
                    StandardSchema::Equal(Value::Int32Value(1)),
                ),
                false,
                false,
            ),
            FieldSpec::new(
                AttrSchema::new(
                    TextSchema::NonEmpty,
                    StandardSchema::Equal(Value::Int32Value(2)),
                ),
                false,
                true,
            ),
        ],
        exhaustive: false,
    };

    let good1 = Value::Record(vec![Attr::of(("foo", 1)), Attr::of(("bar", 2))], vec![]);
    let good2 = Value::Record(vec![Attr::of(("foo", 1)), Attr::of(("foo", 2))], vec![]);
    let good3 = Value::Record(vec![Attr::of(("foo", 1)), Attr::of(("foo", 1))], vec![]);
    let good4 = Value::Record(vec![Attr::of(("foo", 1))], vec![]);
    let good5 = Value::Record(vec![Attr::of(("bar", 2))], vec![]);

    assert!(schema.matches(&good1));
    assert!(schema.matches(&good2));
    assert!(schema.matches(&good3));
    assert!(schema.matches(&good4));
    assert!(schema.matches(&good5));

    let bad1 = Value::Record(vec![Attr::of(("foo", "bar")), Attr::of(("bar", 2))], vec![]);
    let bad2 = Value::Record(vec![Attr::of(("foo", 2)), Attr::of(("bar", 2))], vec![]);
    assert!(!schema.matches(&bad1));
    assert!(!schema.matches(&bad2));
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
fn optional_slots_partial_exhaustive() {
    let schema = StandardSchema::HasSlots {
        slots: vec![
            FieldSpec::new(
                SlotSchema::new(
                    StandardSchema::OfKind(ValueKind::Int32),
                    StandardSchema::Equal(Value::Int32Value(23)),
                ),
                false,
                true,
            ),
            FieldSpec::new(
                SlotSchema::new(
                    StandardSchema::OfKind(ValueKind::Int32),
                    StandardSchema::Equal(Value::Int32Value(45)),
                ),
                false,
                true,
            ),
        ],
        exhaustive: true,
    };

    let good1 = Value::Record(vec![], vec![Item::of((10, 23))]);
    let good2 = Value::Record(vec![], vec![Item::of((10, 23)), Item::of((9, 45))]);

    assert!(schema.matches(&good1));
    assert!(schema.matches(&good2));

    let bad1 = Value::Record(vec![], vec![Item::of(("bar", 23)), Item::of((9, 45))]);
    let bad2 = Value::Record(vec![], vec![Item::of((10, 24)), Item::of((9, 45))]);
    let bad3 = Value::Record(
        vec![],
        vec![Item::of((10, 23)), Item::of((9, 45)), Item::of((8, 23))],
    );
    assert!(!schema.matches(&bad1));
    assert!(!schema.matches(&bad2));
    assert!(!schema.matches(&bad3));
}

#[test]
fn optional_slots_partial_non_exhaustive() {
    let schema = StandardSchema::HasSlots {
        slots: vec![
            FieldSpec::new(
                SlotSchema::new(
                    StandardSchema::OfKind(ValueKind::Int32),
                    StandardSchema::Equal(Value::Int32Value(23)),
                ),
                false,
                false,
            ),
            FieldSpec::new(
                SlotSchema::new(
                    StandardSchema::OfKind(ValueKind::Int32),
                    StandardSchema::Equal(Value::Int32Value(45)),
                ),
                false,
                false,
            ),
        ],
        exhaustive: false,
    };

    let good1 = Value::Record(vec![], vec![Item::of((10, 23))]);
    let good2 = Value::Record(vec![], vec![Item::of((10, 45))]);
    let good3 = Value::Record(vec![], vec![Item::of((1, 23)), Item::of((1, 23))]);
    let good4 = Value::Record(vec![], vec![Item::of((1, 23)), Item::of((1, 45))]);
    let good5 = Value::Record(
        vec![],
        vec![Item::of((1, 23)), Item::of((1, 45)), Item::of((1, 45))],
    );

    assert!(schema.matches(&good1));
    assert!(schema.matches(&good2));
    assert!(schema.matches(&good3));
    assert!(schema.matches(&good4));
    assert!(schema.matches(&good5));

    let bad1 = Value::Record(vec![], vec![Item::of((10, 222))]);
    let bad2 = Value::Record(vec![], vec![Item::of((10, 23)), Item::of((10, 46))]);

    assert!(!schema.matches(&bad1));
    assert!(!schema.matches(&bad2));
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
    let schema = StandardSchema::InRangeInt(Range::<i64>::lower_bounded(Bound::inclusive(12)));
    let value = schema.to_value();
    let expected = Value::of_attr((
        "in_range_int",
        Value::singleton(("min", Value::from_vec(vec![Item::slot("value", 12i64)]))),
    ));
    assert_that!(value, eq(expected));
}

#[test]
fn in_int_range_to_value_max() {
    let schema = StandardSchema::InRangeInt(Range::<i64>::upper_bounded(Bound::inclusive(12)));
    let value = schema.to_value();
    let expected = Value::of_attr((
        "in_range_int",
        Value::singleton(("max", Value::from_vec(vec![Item::slot("value", 12i64)]))),
    ));
    assert_that!(value, eq(expected));
}

#[test]
fn in_int_range_to_value_both() {
    let schema = StandardSchema::InRangeInt(Range::<i64>::bounded(
        Bound::exclusive(-4),
        Bound::inclusive(12),
    ));
    let value = schema.to_value();
    let expected = Value::of_attr((
        "in_range_int",
        Value::from_vec(vec![
            ("min", Value::from_vec(vec![Item::slot("value", -3i64)])),
            ("max", Value::from_vec(vec![Item::slot("value", 12i64)])),
        ]),
    ));
    assert_that!(value, eq(expected));
}

#[test]
fn in_float_range_to_value_min() {
    let schema = StandardSchema::InRangeFloat(Range::<f64>::lower_bounded(Bound::exclusive(0.5)));
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
    let schema = StandardSchema::InRangeFloat(Range::<f64>::upper_bounded(Bound::exclusive(0.5)));
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
    let schema = StandardSchema::InRangeFloat(Range::<f64>::bounded(
        Bound::inclusive(-0.5),
        Bound::exclusive(0.5),
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
fn compare_unbounded_ranges() {
    let first: Range<i64> = Range::<i64>::unbounded();
    let second: Range<i64> = Range::<i64>::unbounded();

    assert_eq!(first, second)
}

#[test]
fn compare_unbounded_range_to_any() {
    let first: Range<i64> = Range::<i64>::unbounded();

    assert!(first > Range::<i64>::upper_bounded(Bound::exclusive(10)));
    assert!(Range::<i64>::upper_bounded(Bound::exclusive(-100)) < first);
    assert!(first > Range::<i64>::lower_bounded(Bound::inclusive(-100)));
    assert!(Range::<i64>::lower_bounded(Bound::exclusive(160)) < first);
    assert!(first > Range::<i64>::bounded(Bound::inclusive(11), Bound::exclusive(51)));
    assert!(Range::<i64>::bounded(Bound::exclusive(-32), Bound::inclusive(51)) < first);
}

#[test]
fn compare_bounded_ranges_equal() {
    assert_eq!(
        Range::<i64>::bounded(Bound::exclusive(10), Bound::exclusive(72)),
        Range::<i64>::bounded(Bound::exclusive(10), Bound::exclusive(72))
    );

    assert_eq!(
        Range::<i64>::bounded(Bound::inclusive(-100), Bound::exclusive(15)),
        Range::<i64>::bounded(Bound::inclusive(-100), Bound::exclusive(15))
    );
}

#[test]
fn compare_bounded_ranges_different() {
    assert!(
        Range::<i64>::bounded(Bound::inclusive(-5), Bound::inclusive(5))
            < Range::<i64>::bounded(Bound::inclusive(-10), Bound::inclusive(10))
    );

    assert!(
        Range::<f64>::bounded(Bound::inclusive(-3.5), Bound::inclusive(3.1))
            < Range::<f64>::bounded(Bound::inclusive(-4.5), Bound::inclusive(3.2))
    );

    assert!(
        Range::<i64>::bounded(Bound::exclusive(5), Bound::exclusive(15))
            > Range::<i64>::bounded(Bound::exclusive(8), Bound::inclusive(12))
    );

    assert!(
        Range::<f64>::bounded(Bound::exclusive(0.11), Bound::exclusive(0.99))
            > Range::<f64>::bounded(Bound::exclusive(0.12), Bound::inclusive(0.88))
    );

    assert!(
        Range::<f64>::bounded(Bound::exclusive(-5.5), Bound::inclusive(5.5))
            < Range::<f64>::bounded(Bound::exclusive(-5.5), Bound::inclusive(15.5))
    );

    assert!(
        Range::<f64>::bounded(Bound::inclusive(-12.2), Bound::inclusive(12.2))
            > Range::<f64>::bounded(Bound::exclusive(-12.2), Bound::inclusive(2.2))
    );

    assert!(
        Range::<f64>::bounded(Bound::inclusive(-5.5), Bound::exclusive(5.5))
            < Range::<f64>::bounded(Bound::exclusive(-15.5), Bound::inclusive(5.5))
    );

    assert!(
        Range::<f64>::bounded(Bound::inclusive(-12.2), Bound::inclusive(2.2))
            > Range::<f64>::bounded(Bound::inclusive(-12.2), Bound::exclusive(2.2))
    );

    assert!(
        Range::<f64>::bounded(Bound::exclusive(-5.5), Bound::exclusive(5.5))
            < Range::<f64>::bounded(Bound::inclusive(-5.5), Bound::inclusive(5.5))
    );

    assert!(
        Range::<f64>::bounded(Bound::inclusive(-12.2), Bound::inclusive(12.2))
            > Range::<f64>::bounded(Bound::exclusive(-12.2), Bound::exclusive(12.2))
    );
}

#[test]
fn compare_bounded_ranges_not_related() {
    assert!(
        Range::<f64>::bounded(Bound::inclusive(2.2), Bound::inclusive(5.5))
            .partial_cmp(&Range::<f64>::bounded(
                Bound::inclusive(-2.2),
                Bound::inclusive(-5.5)
            ))
            .is_none()
    );

    assert!(
        Range::<f64>::bounded(Bound::inclusive(-2.2), Bound::inclusive(5.5))
            .partial_cmp(&Range::<f64>::bounded(
                Bound::inclusive(6.6),
                Bound::inclusive(8.8)
            ))
            .is_none()
    );

    assert!(
        Range::<f64>::bounded(Bound::inclusive(-2.2), Bound::inclusive(5.5))
            .partial_cmp(&Range::<f64>::bounded(
                Bound::inclusive(-1.1),
                Bound::inclusive(6.6)
            ))
            .is_none()
    );

    assert!(
        Range::<f64>::bounded(Bound::exclusive(-2.2), Bound::exclusive(5.5))
            .partial_cmp(&Range::<f64>::bounded(
                Bound::exclusive(-3.3),
                Bound::exclusive(4.4)
            ))
            .is_none()
    );

    assert!(
        Range::<f64>::bounded(Bound::exclusive(-2.2), Bound::inclusive(5.5))
            .partial_cmp(&Range::<f64>::bounded(
                Bound::inclusive(-2.2),
                Bound::inclusive(4.4)
            ))
            .is_none()
    );

    assert!(
        Range::<i64>::bounded(Bound::inclusive(33), Bound::inclusive(44))
            .partial_cmp(&Range::<i64>::bounded(
                Bound::inclusive(22),
                Bound::exclusive(44)
            ))
            .is_none()
    );

    assert!(
        Range::<i64>::bounded(Bound::exclusive(-11), Bound::inclusive(11))
            .partial_cmp(&Range::<i64>::bounded(
                Bound::inclusive(-11),
                Bound::exclusive(11)
            ))
            .is_none()
    );

    assert!(
        Range::<i64>::bounded(Bound::inclusive(-33), Bound::exclusive(33))
            .partial_cmp(&Range::<i64>::bounded(
                Bound::exclusive(-33),
                Bound::inclusive(33)
            ))
            .is_none()
    );
}

#[test]
fn compare_upper_bounded_and_bounded_related() {
    assert!(
        Range::<i64>::upper_bounded(Bound::inclusive(15))
            > Range::<i64>::bounded(Bound::exclusive(-10), Bound::exclusive(10))
    );

    assert!(
        Range::<i64>::bounded(Bound::exclusive(-1), Bound::exclusive(1))
            < Range::<i64>::upper_bounded(Bound::inclusive(11))
    );

    assert!(
        Range::<f64>::upper_bounded(Bound::inclusive(15.5))
            > Range::<f64>::bounded(Bound::exclusive(-10.1), Bound::exclusive(15.5))
    );

    assert!(
        Range::<f64>::upper_bounded(Bound::exclusive(15.5))
            > Range::<f64>::bounded(Bound::exclusive(-10.1), Bound::exclusive(15.5))
    );

    assert!(
        Range::<f64>::bounded(Bound::exclusive(-1.1), Bound::exclusive(11.5))
            < Range::<f64>::upper_bounded(Bound::inclusive(11.5))
    );

    assert!(
        Range::<f64>::bounded(Bound::exclusive(-1.1), Bound::exclusive(-0.11))
            < Range::<f64>::upper_bounded(Bound::exclusive(-0.11))
    );
}

#[test]
fn compare_upper_bounded_and_bounded_not_related() {
    assert!(Range::<f64>::upper_bounded(Bound::exclusive(10.10))
        .partial_cmp(&Range::<f64>::bounded(
            Bound::exclusive(20.20),
            Bound::exclusive(25.25)
        ))
        .is_none());

    assert!(
        Range::<f64>::bounded(Bound::exclusive(-25.25), Bound::exclusive(-20.20))
            .partial_cmp(&Range::<f64>::upper_bounded(Bound::exclusive(-30.30)))
            .is_none()
    );

    assert!(Range::<f64>::upper_bounded(Bound::exclusive(10.10))
        .partial_cmp(&Range::<f64>::bounded(
            Bound::inclusive(10.10),
            Bound::inclusive(25.25)
        ))
        .is_none());

    assert!(
        Range::<f64>::bounded(Bound::inclusive(-25.25), Bound::inclusive(-20.20))
            .partial_cmp(&Range::<f64>::upper_bounded(Bound::inclusive(-25.25)))
            .is_none()
    );

    assert!(Range::<f64>::upper_bounded(Bound::exclusive(33.33))
        .partial_cmp(&Range::<f64>::bounded(
            Bound::inclusive(10.10),
            Bound::inclusive(33.33)
        ))
        .is_none());

    assert!(
        Range::<f64>::bounded(Bound::inclusive(-11.25), Bound::inclusive(-7.33))
            .partial_cmp(&Range::<f64>::upper_bounded(Bound::exclusive(-7.33)))
            .is_none()
    );
}

#[test]
fn compare_lower_bounded_and_bounded_related() {
    assert!(
        Range::<i64>::lower_bounded(Bound::inclusive(-100))
            > Range::<i64>::bounded(Bound::exclusive(-10), Bound::exclusive(10))
    );

    assert!(
        Range::<i64>::bounded(Bound::exclusive(-1), Bound::exclusive(1))
            < Range::<i64>::lower_bounded(Bound::inclusive(-22))
    );

    assert!(
        Range::<f64>::lower_bounded(Bound::inclusive(15.5))
            > Range::<f64>::bounded(Bound::exclusive(15.5), Bound::exclusive(200.22))
    );

    assert!(
        Range::<f64>::lower_bounded(Bound::inclusive(15.5))
            > Range::<f64>::bounded(Bound::inclusive(15.5), Bound::exclusive(200.22))
    );

    assert!(
        Range::<f64>::bounded(Bound::exclusive(-11.1), Bound::exclusive(11.1))
            < Range::<f64>::lower_bounded(Bound::inclusive(-11.1))
    );

    assert!(
        Range::<f64>::bounded(Bound::exclusive(-0.11), Bound::exclusive(0.11))
            < Range::<f64>::lower_bounded(Bound::exclusive(-0.11))
    );
}

#[test]
fn compare_lower_bounded_and_bounded_not_related() {
    assert!(Range::<f64>::lower_bounded(Bound::exclusive(10.10))
        .partial_cmp(&Range::<f64>::bounded(
            Bound::exclusive(1.1),
            Bound::exclusive(5.5)
        ))
        .is_none());

    assert!(
        Range::<f64>::bounded(Bound::exclusive(-33.33), Bound::exclusive(-22.22))
            .partial_cmp(&Range::<f64>::lower_bounded(Bound::exclusive(-20.20)))
            .is_none()
    );

    assert!(Range::<f64>::lower_bounded(Bound::exclusive(12.12))
        .partial_cmp(&Range::<f64>::bounded(
            Bound::inclusive(1.1),
            Bound::inclusive(12.12)
        ))
        .is_none());

    assert!(
        Range::<f64>::bounded(Bound::inclusive(-30.30), Bound::inclusive(-25.25))
            .partial_cmp(&Range::<f64>::lower_bounded(Bound::inclusive(-25.25)))
            .is_none()
    );

    assert!(Range::<f64>::lower_bounded(Bound::exclusive(3.3))
        .partial_cmp(&Range::<f64>::bounded(
            Bound::inclusive(3.3),
            Bound::inclusive(10.10)
        ))
        .is_none());

    assert!(
        Range::<f64>::bounded(Bound::inclusive(-11.25), Bound::inclusive(-7.33))
            .partial_cmp(&Range::<f64>::lower_bounded(Bound::exclusive(-7.33)))
            .is_none()
    );

    assert!(Range::<f64>::lower_bounded(Bound::exclusive(10.10))
        .partial_cmp(&Range::<f64>::bounded(
            Bound::inclusive(10.10),
            Bound::inclusive(25.25)
        ))
        .is_none());
}

#[test]
fn compare_upper_bounded() {
    assert_eq!(
        Range::<f64>::upper_bounded(Bound::exclusive(10.10)),
        Range::<f64>::upper_bounded(Bound::exclusive(10.10))
    );

    assert!(
        Range::<f64>::upper_bounded(Bound::exclusive(15.15))
            > Range::<f64>::upper_bounded(Bound::exclusive(10.10))
    );

    assert!(
        Range::<f64>::upper_bounded(Bound::exclusive(-15.15))
            < Range::<f64>::upper_bounded(Bound::exclusive(-10.10))
    );

    assert!(
        Range::<f64>::upper_bounded(Bound::inclusive(10.10))
            > Range::<f64>::upper_bounded(Bound::exclusive(10.10))
    );

    assert!(
        Range::<f64>::upper_bounded(Bound::exclusive(-10.10))
            < Range::<f64>::upper_bounded(Bound::inclusive(-10.10))
    );
}

#[test]
fn compare_lower_bounded() {
    assert_eq!(
        Range::<f64>::lower_bounded(Bound::exclusive(3.14)),
        Range::<f64>::lower_bounded(Bound::exclusive(3.14))
    );

    assert!(
        Range::<f64>::lower_bounded(Bound::exclusive(15.15))
            < Range::<f64>::lower_bounded(Bound::exclusive(10.10))
    );

    assert!(
        Range::<f64>::lower_bounded(Bound::exclusive(-15.15))
            > Range::<f64>::lower_bounded(Bound::exclusive(-10.10))
    );

    assert!(
        Range::<f64>::lower_bounded(Bound::exclusive(10.10))
            < Range::<f64>::lower_bounded(Bound::inclusive(10.10))
    );

    assert!(
        Range::<f64>::lower_bounded(Bound::inclusive(-10.10))
            > Range::<f64>::lower_bounded(Bound::exclusive(-10.10))
    );
}

#[test]
fn test_vec_schemas_compare() {
    let empty_vec = Vec::<i32>::new();
    let non_empty_vec = vec![1];

    assert_eq!(
        vec_schemas_cmp(false, false, empty_vec.is_empty(), empty_vec.is_empty()).unwrap(),
        Ordering::Equal
    );

    assert_eq!(
        vec_schemas_cmp(true, false, empty_vec.is_empty(), empty_vec.is_empty()).unwrap(),
        Ordering::Less
    );

    assert_eq!(
        vec_schemas_cmp(false, true, empty_vec.is_empty(), empty_vec.is_empty()).unwrap(),
        Ordering::Greater
    );

    assert_eq!(
        vec_schemas_cmp(false, false, non_empty_vec.is_empty(), empty_vec.is_empty()).unwrap(),
        Ordering::Less
    );

    assert_eq!(
        vec_schemas_cmp(false, false, empty_vec.is_empty(), non_empty_vec.is_empty()).unwrap(),
        Ordering::Greater
    );

    assert!(vec_schemas_cmp(
        false,
        false,
        non_empty_vec.is_empty(),
        non_empty_vec.is_empty()
    )
    .is_none());
    assert!(vec_schemas_cmp(true, true, empty_vec.is_empty(), empty_vec.is_empty()).is_none());
    assert!(vec_schemas_cmp(
        true,
        true,
        non_empty_vec.is_empty(),
        non_empty_vec.is_empty()
    )
    .is_none());
}

#[test]
fn test_fields_are_related() {
    let sup_fields = vec![
        FieldSpec::new(TextSchema::Exact("first".to_string()), false, false),
        FieldSpec::new(TextSchema::Exact("second".to_string()), false, true),
        FieldSpec::new(TextSchema::Exact("third".to_string()), true, false),
    ];

    let sub_fields_valid_first = vec![
        FieldSpec::new(TextSchema::Exact("first".to_string()), false, false),
        FieldSpec::new(TextSchema::Exact("third".to_string()), true, false),
    ];

    let sub_fields_valid_second = vec![
        FieldSpec::new(TextSchema::Exact("first".to_string()), false, false),
        FieldSpec::new(TextSchema::Exact("second".to_string()), false, true),
        FieldSpec::new(TextSchema::Exact("third".to_string()), true, false),
    ];

    let sub_fields_valid_third = vec![FieldSpec::new(
        TextSchema::Exact("third".to_string()),
        true,
        false,
    )];

    let sub_fields_invalid_first = vec![FieldSpec::new(
        TextSchema::Exact("fourth".to_string()),
        false,
        false,
    )];

    let sub_fields_invalid_second = vec![FieldSpec::new(
        TextSchema::Exact("first".to_string()),
        true,
        false,
    )];

    let sub_fields_invalid_third = vec![FieldSpec::new(
        TextSchema::Exact("first".to_string()),
        false,
        true,
    )];

    let sub_fields_invalid_fourth = vec![
        FieldSpec::new(TextSchema::Exact("first".to_string()), false, false),
        FieldSpec::new(TextSchema::Exact("fourth".to_string()), false, true),
    ];

    let sub_fields_invalid_fifth = vec![
        FieldSpec::new(TextSchema::Exact("first".to_string()), false, false),
        FieldSpec::new(TextSchema::Exact("fifth".to_string()), false, true),
        FieldSpec::new(TextSchema::Exact("third".to_string()), true, false),
    ];

    let sub_fields_invalid_sixth = vec![
        FieldSpec::new(TextSchema::Exact("first".to_string()), false, false),
        FieldSpec::new(TextSchema::Exact("second".to_string()), false, false),
        FieldSpec::new(TextSchema::Exact("third".to_string()), true, false),
    ];

    // Valid if non exhaustive, invalid otherwise
    let sub_fields_mixed = vec![
        FieldSpec::new(TextSchema::Exact("first".to_string()), false, false),
        FieldSpec::new(TextSchema::Exact("second".to_string()), false, true),
    ];

    assert!(fields_are_related(
        &sup_fields,
        &sub_fields_valid_first,
        true
    ));
    assert!(fields_are_related(
        &sup_fields,
        &sub_fields_valid_second,
        true
    ));
    assert!(fields_are_related(
        &sup_fields,
        &sub_fields_valid_third,
        true
    ));

    assert!(!fields_are_related(&vec![], &sup_fields, true));
    assert!(!fields_are_related(
        &sup_fields,
        &sub_fields_invalid_first,
        true
    ));
    assert!(!fields_are_related(
        &sup_fields,
        &sub_fields_invalid_second,
        true
    ));
    assert!(!fields_are_related(
        &sup_fields,
        &sub_fields_invalid_third,
        true
    ));
    assert!(!fields_are_related(
        &sup_fields,
        &sub_fields_invalid_fourth,
        true
    ));
    assert!(!fields_are_related(
        &sup_fields,
        &sub_fields_invalid_fifth,
        true
    ));
    assert!(!fields_are_related(
        &sup_fields,
        &sub_fields_invalid_sixth,
        true
    ));

    assert!(fields_are_related(&sup_fields, &sub_fields_mixed, false));

    assert!(!fields_are_related(&sup_fields, &sub_fields_mixed, true));
}

#[test]
fn test_ordered_items_are_related() {
    let sup_items = vec![
        (
            ItemSchema::ValueItem(StandardSchema::Text(TextSchema::Exact("first".to_string()))),
            false,
        ),
        (
            ItemSchema::ValueItem(StandardSchema::Text(TextSchema::Exact(
                "second".to_string(),
            ))),
            true,
        ),
        (
            ItemSchema::ValueItem(StandardSchema::Text(TextSchema::Exact("third".to_string()))),
            false,
        ),
    ];

    let sub_items_valid_first = vec![
        (
            ItemSchema::ValueItem(StandardSchema::Text(TextSchema::Exact("first".to_string()))),
            false,
        ),
        (
            ItemSchema::ValueItem(StandardSchema::Text(TextSchema::Exact(
                "second".to_string(),
            ))),
            true,
        ),
    ];

    let sub_items_valid_second = vec![
        (
            ItemSchema::ValueItem(StandardSchema::Text(TextSchema::Exact("first".to_string()))),
            false,
        ),
        (
            ItemSchema::ValueItem(StandardSchema::Text(TextSchema::Exact(
                "second".to_string(),
            ))),
            true,
        ),
        (
            ItemSchema::ValueItem(StandardSchema::Text(TextSchema::Exact("third".to_string()))),
            false,
        ),
    ];

    let sub_items_invalid_first = vec![(
        ItemSchema::ValueItem(StandardSchema::Text(TextSchema::Exact("forth".to_string()))),
        false,
    )];

    let sub_items_invalid_second = vec![(
        ItemSchema::ValueItem(StandardSchema::Text(TextSchema::Exact("first".to_string()))),
        true,
    )];

    let sub_items_invalid_third = vec![(
        ItemSchema::ValueItem(StandardSchema::Text(TextSchema::Exact(
            "second".to_string(),
        ))),
        true,
    )];

    let sub_items_invalid_fourth = vec![
        (
            ItemSchema::ValueItem(StandardSchema::Text(TextSchema::Exact("first".to_string()))),
            false,
        ),
        (
            ItemSchema::ValueItem(StandardSchema::Text(TextSchema::Exact("third".to_string()))),
            false,
        ),
    ];

    let sub_items_invalid_fifth = vec![
        (
            ItemSchema::ValueItem(StandardSchema::Text(TextSchema::Exact("first".to_string()))),
            false,
        ),
        (
            ItemSchema::ValueItem(StandardSchema::Text(TextSchema::Exact(
                "second".to_string(),
            ))),
            true,
        ),
        (
            ItemSchema::ValueItem(StandardSchema::Text(TextSchema::Exact("third".to_string()))),
            true,
        ),
    ];

    let sub_items_invalid_sixth = vec![
        (
            ItemSchema::ValueItem(StandardSchema::Text(TextSchema::Exact("first".to_string()))),
            false,
        ),
        (
            ItemSchema::ValueItem(StandardSchema::Text(TextSchema::Exact("fifth".to_string()))),
            true,
        ),
        (
            ItemSchema::ValueItem(StandardSchema::Text(TextSchema::Exact("third".to_string()))),
            false,
        ),
    ];

    // Valid if non exhaustive, invalid otherwise
    let sub_items_mixed = vec![(
        ItemSchema::ValueItem(StandardSchema::Text(TextSchema::Exact("first".to_string()))),
        false,
    )];

    assert!(ordered_items_are_related(
        &sup_items,
        &sub_items_valid_first,
        true
    ));
    assert!(ordered_items_are_related(
        &sup_items,
        &sub_items_valid_second,
        true
    ));

    assert!(!ordered_items_are_related(&vec![], &sup_items, true));
    assert!(!ordered_items_are_related(
        &sup_items,
        &sub_items_invalid_first,
        true
    ));
    assert!(!ordered_items_are_related(
        &sup_items,
        &sub_items_invalid_second,
        true
    ));
    assert!(!ordered_items_are_related(
        &sup_items,
        &sub_items_invalid_third,
        true
    ));
    assert!(!ordered_items_are_related(
        &sup_items,
        &sub_items_invalid_fourth,
        true
    ));
    assert!(!ordered_items_are_related(
        &sup_items,
        &sub_items_invalid_fifth,
        true
    ));
    assert!(!ordered_items_are_related(
        &sup_items,
        &sub_items_invalid_sixth,
        true
    ));

    assert!(ordered_items_are_related(
        &sup_items,
        &sub_items_mixed,
        false
    ));
    assert!(!ordered_items_are_related(
        &sup_items,
        &sub_items_mixed,
        true
    ));
}

#[test]
fn combine_ordering_equal() {
    assert_eq!(
        combine_orderings(Ordering::Equal, Ordering::Equal).unwrap(),
        Ordering::Equal
    );
    assert_eq!(
        combine_orderings(
            Ordering::Equal,
            combine_orderings(Ordering::Equal, Ordering::Equal).unwrap()
        )
        .unwrap(),
        Ordering::Equal
    );
}

#[test]
fn combine_ordering_greater() {
    assert_eq!(
        combine_orderings(Ordering::Greater, Ordering::Greater).unwrap(),
        Ordering::Greater
    );
    assert_eq!(
        combine_orderings(
            Ordering::Equal,
            combine_orderings(Ordering::Equal, Ordering::Greater).unwrap()
        )
        .unwrap(),
        Ordering::Greater
    );

    assert_eq!(
        combine_orderings(
            Ordering::Greater,
            combine_orderings(Ordering::Equal, Ordering::Equal).unwrap()
        )
        .unwrap(),
        Ordering::Greater
    );

    assert_eq!(
        combine_orderings(
            Ordering::Equal,
            combine_orderings(Ordering::Greater, Ordering::Equal).unwrap()
        )
        .unwrap(),
        Ordering::Greater
    );

    assert_eq!(
        combine_orderings(
            Ordering::Greater,
            combine_orderings(Ordering::Greater, Ordering::Equal).unwrap()
        )
        .unwrap(),
        Ordering::Greater
    );
}

#[test]
fn combine_ordering_less() {
    assert_eq!(
        combine_orderings(Ordering::Less, Ordering::Less).unwrap(),
        Ordering::Less
    );
    assert_eq!(
        combine_orderings(
            Ordering::Equal,
            combine_orderings(Ordering::Equal, Ordering::Less).unwrap()
        )
        .unwrap(),
        Ordering::Less
    );

    assert_eq!(
        combine_orderings(
            Ordering::Less,
            combine_orderings(Ordering::Equal, Ordering::Equal).unwrap()
        )
        .unwrap(),
        Ordering::Less
    );

    assert_eq!(
        combine_orderings(
            Ordering::Equal,
            combine_orderings(Ordering::Less, Ordering::Equal).unwrap()
        )
        .unwrap(),
        Ordering::Less
    );

    assert_eq!(
        combine_orderings(
            Ordering::Less,
            combine_orderings(Ordering::Less, Ordering::Equal).unwrap()
        )
        .unwrap(),
        Ordering::Less
    );
}

#[test]
fn combine_ordering_not_related() {
    assert_eq!(combine_orderings(Ordering::Less, Ordering::Greater), None);

    assert_eq!(
        combine_orderings(
            Ordering::Less,
            combine_orderings(Ordering::Greater, Ordering::Greater).unwrap()
        ),
        None
    );

    assert_eq!(
        combine_orderings(
            combine_orderings(Ordering::Equal, Ordering::Less).unwrap(),
            Ordering::Greater
        ),
        None
    );

    assert_eq!(
        combine_orderings(
            combine_orderings(Ordering::Greater, Ordering::Equal).unwrap(),
            Ordering::Less
        ),
        None
    );

    assert_eq!(
        combine_orderings(
            combine_orderings(Ordering::Equal, Ordering::Greater).unwrap(),
            Ordering::Less
        ),
        None
    );
}

#[test]
fn compare_slot_schemas_equal() {
    let schema = SlotSchema::new(
        StandardSchema::Equal(Value::Int32Value(5)),
        StandardSchema::Equal(Value::Int32Value(10)),
    );

    let equal_schema_1 = SlotSchema::new(
        StandardSchema::Equal(Value::Int32Value(5)),
        StandardSchema::Equal(Value::Int32Value(10)),
    );

    let equal_schema_2 = SlotSchema::new(
        StandardSchema::Equal(Value::Int32Value(5)),
        StandardSchema::Equal(Value::Int64Value(10)),
    );

    let equal_schema_3 = SlotSchema::new(
        StandardSchema::Equal(Value::Int64Value(5)),
        StandardSchema::Equal(Value::Int32Value(10)),
    );

    assert_eq!(
        schema.partial_cmp(&equal_schema_1).unwrap(),
        Ordering::Equal
    );
    assert_eq!(
        equal_schema_1.partial_cmp(&schema).unwrap(),
        Ordering::Equal
    );

    assert_eq!(
        schema.partial_cmp(&equal_schema_2).unwrap(),
        Ordering::Equal
    );
    assert_eq!(
        equal_schema_2.partial_cmp(&schema).unwrap(),
        Ordering::Equal
    );

    assert_eq!(
        schema.partial_cmp(&equal_schema_3).unwrap(),
        Ordering::Equal
    );
    assert_eq!(
        equal_schema_3.partial_cmp(&schema).unwrap(),
        Ordering::Equal
    );
}

#[test]
fn compare_slot_schemas_greater() {
    let schema = SlotSchema::new(
        StandardSchema::Equal(Value::Int32Value(10)),
        StandardSchema::Equal(Value::Int32Value(15)),
    );

    let greater_schema_1 = SlotSchema::new(
        StandardSchema::OfKind(ValueKind::Int32),
        StandardSchema::Equal(Value::Int32Value(15)),
    );

    let greater_schema_2 = SlotSchema::new(
        StandardSchema::Equal(Value::Int32Value(10)),
        StandardSchema::OfKind(ValueKind::Int32),
    );

    let greater_schema_3 = SlotSchema::new(
        StandardSchema::OfKind(ValueKind::Int32),
        StandardSchema::OfKind(ValueKind::Int32),
    );

    assert!(schema < greater_schema_1);
    assert!(greater_schema_1 > schema);
    assert!(schema < greater_schema_2);
    assert!(greater_schema_2 > schema);
    assert!(schema < greater_schema_2);
    assert!(greater_schema_3 > schema);
}

#[test]
fn compare_slot_schemas_lesser() {
    let schema = SlotSchema::new(
        StandardSchema::InRangeInt(Range::<i64>::bounded(
            Bound::inclusive(-20),
            Bound::inclusive(-10),
        )),
        StandardSchema::InRangeInt(Range::<i64>::bounded(
            Bound::inclusive(10),
            Bound::inclusive(20),
        )),
    );

    let lesser_schema_1 = SlotSchema::new(
        StandardSchema::Equal(Value::Int64Value(-15)),
        StandardSchema::InRangeInt(Range::<i64>::bounded(
            Bound::inclusive(10),
            Bound::inclusive(20),
        )),
    );

    let lesser_schema_2 = SlotSchema::new(
        StandardSchema::InRangeInt(Range::<i64>::bounded(
            Bound::inclusive(-20),
            Bound::inclusive(-10),
        )),
        StandardSchema::Equal(Value::Int64Value(15)),
    );

    let lesser_schema_3 = SlotSchema::new(
        StandardSchema::Equal(Value::Int64Value(-15)),
        StandardSchema::Equal(Value::Int64Value(15)),
    );

    assert!(schema > lesser_schema_1);
    assert!(lesser_schema_1 < schema);
    assert!(schema > lesser_schema_2);
    assert!(lesser_schema_2 < schema);
    assert!(schema > lesser_schema_2);
    assert!(lesser_schema_3 < schema);
}

#[test]
fn compare_slot_schemas_not_related() {
    let schema = SlotSchema::new(
        StandardSchema::InRangeInt(Range::<i64>::bounded(
            Bound::inclusive(-20),
            Bound::inclusive(-10),
        )),
        StandardSchema::InRangeInt(Range::<i64>::bounded(
            Bound::inclusive(10),
            Bound::inclusive(20),
        )),
    );

    let not_related_schema_1 = SlotSchema::new(
        StandardSchema::Equal(Value::Int64Value(-30)),
        StandardSchema::InRangeInt(Range::<i64>::bounded(
            Bound::inclusive(10),
            Bound::inclusive(20),
        )),
    );

    let not_related_schema_2 = SlotSchema::new(
        StandardSchema::InRangeInt(Range::<i64>::bounded(
            Bound::inclusive(-20),
            Bound::inclusive(-10),
        )),
        StandardSchema::Equal(Value::Int64Value(50)),
    );

    let not_related_schema_3 = SlotSchema::new(
        StandardSchema::Equal(Value::Int64Value(-30)),
        StandardSchema::Equal(Value::Int64Value(50)),
    );

    assert_eq!(schema.partial_cmp(&not_related_schema_1), None);
    assert_eq!(not_related_schema_1.partial_cmp(&schema), None);
    assert_eq!(schema.partial_cmp(&not_related_schema_2), None);
    assert_eq!(not_related_schema_2.partial_cmp(&schema), None);
    assert_eq!(schema.partial_cmp(&not_related_schema_3), None);
    assert_eq!(not_related_schema_3.partial_cmp(&schema), None);
}

#[test]
fn compare_item_schemas_equal() {
    let value_item_schema = ItemSchema::ValueItem(StandardSchema::Equal(Value::Int32Value(5)));
    let field_schema = ItemSchema::Field(SlotSchema::new(
        StandardSchema::Equal(Value::Int32Value(10)),
        StandardSchema::Equal(Value::Int32Value(20)),
    ));

    let value_item_equal_schema_1 =
        ItemSchema::ValueItem(StandardSchema::Equal(Value::Int32Value(5)));

    let value_item_equal_schema_2 =
        ItemSchema::ValueItem(StandardSchema::Equal(Value::Int64Value(5)));

    let field_equal_schema_1 = ItemSchema::Field(SlotSchema::new(
        StandardSchema::Equal(Value::Int32Value(10)),
        StandardSchema::Equal(Value::Int32Value(20)),
    ));

    let field_equal_schema_2 = ItemSchema::Field(SlotSchema::new(
        StandardSchema::Equal(Value::Int64Value(10)),
        StandardSchema::Equal(Value::Int64Value(20)),
    ));

    assert_eq!(
        value_item_schema
            .partial_cmp(&value_item_equal_schema_1)
            .unwrap(),
        Ordering::Equal
    );
    assert_eq!(
        value_item_equal_schema_1
            .partial_cmp(&value_item_schema)
            .unwrap(),
        Ordering::Equal
    );

    assert_eq!(
        value_item_schema
            .partial_cmp(&value_item_equal_schema_2)
            .unwrap(),
        Ordering::Equal
    );
    assert_eq!(
        value_item_equal_schema_2
            .partial_cmp(&value_item_schema)
            .unwrap(),
        Ordering::Equal
    );

    assert_eq!(
        field_schema.partial_cmp(&field_equal_schema_1).unwrap(),
        Ordering::Equal
    );
    assert_eq!(
        field_equal_schema_1.partial_cmp(&field_schema).unwrap(),
        Ordering::Equal
    );

    assert_eq!(
        field_schema.partial_cmp(&field_equal_schema_2).unwrap(),
        Ordering::Equal
    );
    assert_eq!(
        field_equal_schema_2.partial_cmp(&field_schema).unwrap(),
        Ordering::Equal
    );
}

#[test]
fn compare_item_schemas_greater() {
    let value_item_schema = ItemSchema::ValueItem(StandardSchema::Equal(Value::Int32Value(5)));
    let field_schema = ItemSchema::Field(SlotSchema::new(
        StandardSchema::Equal(Value::Int32Value(10)),
        StandardSchema::Equal(Value::Int32Value(20)),
    ));

    let value_item_greater_schema = ItemSchema::ValueItem(StandardSchema::InRangeInt(
        Range::<i64>::bounded(Bound::inclusive(0), Bound::inclusive(20)),
    ));

    let field_greater_schema = ItemSchema::Field(SlotSchema::new(
        StandardSchema::InRangeInt(Range::<i64>::bounded(
            Bound::inclusive(5),
            Bound::inclusive(15),
        )),
        StandardSchema::InRangeInt(Range::<i64>::bounded(
            Bound::inclusive(15),
            Bound::inclusive(25),
        )),
    ));

    assert!(value_item_schema < value_item_greater_schema);
    assert!(value_item_greater_schema > value_item_schema);
    assert!(field_schema < field_greater_schema);
    assert!(field_greater_schema > field_schema);
}

#[test]
fn compare_item_schemas_lesser() {
    let value_item_schema = ItemSchema::ValueItem(StandardSchema::InRangeInt(
        Range::<i64>::bounded(Bound::inclusive(5), Bound::inclusive(15)),
    ));

    let field_schema = ItemSchema::Field(SlotSchema::new(
        StandardSchema::InRangeInt(Range::<i64>::bounded(
            Bound::inclusive(5),
            Bound::inclusive(15),
        )),
        StandardSchema::InRangeInt(Range::<i64>::bounded(
            Bound::inclusive(15),
            Bound::inclusive(25),
        )),
    ));

    let value_item_lesser_schema =
        ItemSchema::ValueItem(StandardSchema::Equal(Value::Int64Value(10)));

    let field_lesser_schema = ItemSchema::Field(SlotSchema::new(
        StandardSchema::Equal(Value::Int64Value(10)),
        StandardSchema::Equal(Value::Int64Value(20)),
    ));

    assert!(value_item_schema > value_item_lesser_schema);
    assert!(value_item_lesser_schema < value_item_schema);
    assert!(field_schema > field_lesser_schema);
    assert!(field_lesser_schema < field_schema);
}

#[test]
fn compare_item_schemas_not_related() {
    let value_item_schema = ItemSchema::ValueItem(StandardSchema::InRangeInt(
        Range::<i64>::bounded(Bound::inclusive(5), Bound::inclusive(15)),
    ));

    let field_schema = ItemSchema::Field(SlotSchema::new(
        StandardSchema::InRangeInt(Range::<i64>::bounded(
            Bound::inclusive(5),
            Bound::inclusive(15),
        )),
        StandardSchema::InRangeInt(Range::<i64>::bounded(
            Bound::inclusive(15),
            Bound::inclusive(25),
        )),
    ));

    let value_item_not_related_schema_1 =
        ItemSchema::ValueItem(StandardSchema::Equal(Value::Int64Value(-10)));

    let value_item_not_related_schema_2 = ItemSchema::Field(SlotSchema::new(
        StandardSchema::Equal(Value::Int64Value(10)),
        StandardSchema::Equal(Value::Int64Value(11)),
    ));

    let field_not_related_schema_1 = ItemSchema::Field(SlotSchema::new(
        StandardSchema::Equal(Value::Int64Value(55)),
        StandardSchema::Equal(Value::Int64Value(105)),
    ));

    let field_not_related_schema_2 =
        ItemSchema::ValueItem(StandardSchema::Equal(Value::Int64Value(15)));

    assert_eq!(
        value_item_schema.partial_cmp(&value_item_not_related_schema_1),
        None
    );
    assert_eq!(
        value_item_not_related_schema_1.partial_cmp(&value_item_schema),
        None
    );
    assert_eq!(
        value_item_schema.partial_cmp(&value_item_not_related_schema_2),
        None
    );
    assert_eq!(
        value_item_not_related_schema_2.partial_cmp(&value_item_schema),
        None
    );
    assert_eq!(field_schema.partial_cmp(&field_not_related_schema_1), None);
    assert_eq!(field_not_related_schema_1.partial_cmp(&field_schema), None);
    assert_eq!(field_schema.partial_cmp(&field_not_related_schema_2), None);
    assert_eq!(field_not_related_schema_2.partial_cmp(&field_schema), None);
}

#[test]
fn compare_attr_schemas_equal() {
    let schema = AttrSchema::new(
        TextSchema::Matches(Regex::new("\\w+").unwrap()),
        StandardSchema::Equal(Value::Int32Value(10)),
    );

    let equal_schema_1 = AttrSchema::new(
        TextSchema::Matches(Regex::new("\\w+").unwrap()),
        StandardSchema::Equal(Value::Int32Value(10)),
    );

    let equal_schema_2 = AttrSchema::new(
        TextSchema::Matches(Regex::new("\\w+").unwrap()),
        StandardSchema::Equal(Value::Int64Value(10)),
    );

    assert_eq!(
        schema.partial_cmp(&equal_schema_1).unwrap(),
        Ordering::Equal
    );
    assert_eq!(
        equal_schema_1.partial_cmp(&schema).unwrap(),
        Ordering::Equal
    );

    assert_eq!(
        schema.partial_cmp(&equal_schema_2).unwrap(),
        Ordering::Equal
    );
    assert_eq!(
        equal_schema_2.partial_cmp(&schema).unwrap(),
        Ordering::Equal
    );
}

#[test]
fn compare_attr_schemas_greater() {
    let schema = AttrSchema::new(
        TextSchema::Matches(Regex::new("\\w+").unwrap()),
        StandardSchema::Equal(Value::Int32Value(10)),
    );

    let greater_schema_1 = AttrSchema::new(
        TextSchema::NonEmpty,
        StandardSchema::Equal(Value::Int32Value(10)),
    );

    let greater_schema_2 = AttrSchema::new(
        TextSchema::Matches(Regex::new("\\w+").unwrap()),
        StandardSchema::OfKind(ValueKind::Int32),
    );

    let greater_schema_3 = AttrSchema::new(
        TextSchema::NonEmpty,
        StandardSchema::OfKind(ValueKind::Int32),
    );

    assert!(schema < greater_schema_1);
    assert!(greater_schema_1 > schema);
    assert!(schema < greater_schema_2);
    assert!(greater_schema_2 > schema);
    assert!(schema < greater_schema_2);
    assert!(greater_schema_3 > schema);
}

#[test]
fn compare_attr_schemas_lesser() {
    let schema = AttrSchema::new(
        TextSchema::Matches(Regex::new("\\w+").unwrap()),
        StandardSchema::InRangeInt(Range::<i64>::bounded(
            Bound::inclusive(10),
            Bound::inclusive(20),
        )),
    );

    let lesser_schema_1 = AttrSchema::new(
        TextSchema::Exact("a".to_string()),
        StandardSchema::InRangeInt(Range::<i64>::bounded(
            Bound::inclusive(10),
            Bound::inclusive(20),
        )),
    );

    let lesser_schema_2 = AttrSchema::new(
        TextSchema::Matches(Regex::new("\\w+").unwrap()),
        StandardSchema::Equal(Value::Int64Value(15)),
    );

    let lesser_schema_3 = AttrSchema::new(
        TextSchema::Exact("a".to_string()),
        StandardSchema::Equal(Value::Int64Value(15)),
    );

    assert!(schema > lesser_schema_1);
    assert!(lesser_schema_1 < schema);
    assert!(schema > lesser_schema_2);
    assert!(lesser_schema_2 < schema);
    assert!(schema > lesser_schema_2);
    assert!(lesser_schema_3 < schema);
}

#[test]
fn compare_attr_schemas_not_related() {
    let schema = AttrSchema::new(
        TextSchema::Matches(Regex::new("\\w+").unwrap()),
        StandardSchema::InRangeInt(Range::<i64>::bounded(
            Bound::inclusive(10),
            Bound::inclusive(20),
        )),
    );

    let not_related_schema_1 = AttrSchema::new(
        TextSchema::Exact("@".to_string()),
        StandardSchema::InRangeInt(Range::<i64>::bounded(
            Bound::inclusive(10),
            Bound::inclusive(20),
        )),
    );

    let not_related_schema_2 = AttrSchema::new(
        TextSchema::Matches(Regex::new("\\w+").unwrap()),
        StandardSchema::Equal(Value::Int64Value(-15)),
    );

    let not_related_schema_3 = AttrSchema::new(
        TextSchema::Exact("@".to_string()),
        StandardSchema::Equal(Value::Int64Value(-15)),
    );

    assert_eq!(schema.partial_cmp(&not_related_schema_1), None);
    assert_eq!(not_related_schema_1.partial_cmp(&schema), None);
    assert_eq!(schema.partial_cmp(&not_related_schema_2), None);
    assert_eq!(not_related_schema_2.partial_cmp(&schema), None);
    assert_eq!(schema.partial_cmp(&not_related_schema_3), None);
    assert_eq!(not_related_schema_3.partial_cmp(&schema), None);
}

#[test]
fn compare_upper_and_lower_bounded() {
    assert!(Range::<f64>::upper_bounded(Bound::exclusive(10.10))
        .partial_cmp(&Range::<f64>::lower_bounded(Bound::exclusive(10.10)))
        .is_none());

    assert!(Range::<f64>::lower_bounded(Bound::exclusive(-10.10))
        .partial_cmp(&Range::<f64>::upper_bounded(Bound::exclusive(10.10)))
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
        StandardSchema::InRangeInt(Range::<i64>::bounded(
            Bound::inclusive(0),
            Bound::inclusive(10),
        )),
    );
    map.insert(
        "in_range_float",
        StandardSchema::InRangeFloat(Range::<f64>::bounded(
            Bound::inclusive(0.5),
            Bound::inclusive(10.5),
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
        StandardSchema::InRangeInt(Range::<i64>::bounded(
            Bound::inclusive(i32::MIN as i64 - 1),
            Bound::inclusive(i32::MAX as i64 + 1),
        )),
    ];

    let lesser_schemas = vec![
        StandardSchema::Equal(Value::Int32Value(10)),
        StandardSchema::Equal(Value::Int64Value(20)),
        StandardSchema::InRangeInt(Range::<i64>::bounded(
            Bound::inclusive(10),
            Bound::inclusive(20),
        )),
    ];

    let equal_schemas = vec![
        StandardSchema::OfKind(ValueKind::Int32),
        StandardSchema::InRangeInt(Range::<i64>::bounded(
            Bound::inclusive(i32::MIN as i64),
            Bound::inclusive(i32::MAX as i64),
        )),
    ];

    let not_related_schemas = vec![
        StandardSchema::Equal(Value::Int64Value(i32::MAX as i64 + 1)),
        StandardSchema::InRangeInt(Range::<i64>::bounded(
            Bound::inclusive(i32::MIN as i64 - 1),
            Bound::inclusive(20),
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
        StandardSchema::InRangeInt(Range::<i64>::bounded(
            Bound::inclusive(0),
            Bound::inclusive(10),
        )),
    ];

    let equal_schemas = vec![
        StandardSchema::OfKind(ValueKind::Int64),
        StandardSchema::InRangeInt(Range::<i64>::bounded(
            Bound::inclusive(i64::MIN),
            Bound::inclusive(i64::MAX),
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
        StandardSchema::InRangeFloat(Range::<f64>::bounded(
            Bound::inclusive(0.0),
            Bound::inclusive(10.0),
        )),
        StandardSchema::Equal(Value::Float64Value(10.0)),
    ];

    let equal_schemas = vec![
        StandardSchema::OfKind(ValueKind::Float64),
        StandardSchema::InRangeFloat(Range::<f64>::bounded(
            Bound::inclusive(f64::MIN),
            Bound::inclusive(f64::MAX),
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

    let greater_schemas = vec![StandardSchema::InRangeInt(Range::<i64>::bounded(
        Bound::inclusive(10),
        Bound::exclusive(55),
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

    let greater_schemas = vec![StandardSchema::InRangeInt(Range::<i64>::bounded(
        Bound::inclusive(24),
        Bound::exclusive(30),
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
        StandardSchema::InRangeFloat(Range::<f64>::bounded(
            Bound::inclusive(-10.10),
            Bound::exclusive(30.30),
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
                StandardSchema::InRangeInt(Range::<i64>::bounded(
                    Bound::inclusive(0),
                    Bound::inclusive(15),
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

#[test]
fn compare_in_range_int() {
    let schema = StandardSchema::InRangeInt(Range::<i64>::bounded(
        Bound::inclusive(5),
        Bound::inclusive(15),
    ));

    let equal_schemas = vec![
        StandardSchema::InRangeInt(Range::<i64>::bounded(
            Bound::inclusive(5),
            Bound::inclusive(15),
        )),
        StandardSchema::InRangeInt(Range::<i64>::bounded(
            Bound::exclusive(4),
            Bound::exclusive(16),
        )),
    ];

    let greater_schemas = vec![
        StandardSchema::InRangeInt(Range::<i64>::bounded(
            Bound::inclusive(4),
            Bound::inclusive(16),
        )),
        StandardSchema::InRangeInt(Range::<i64>::unbounded()),
    ];

    let lesser_schemas = vec![
        StandardSchema::InRangeInt(Range::<i64>::bounded(
            Bound::exclusive(5),
            Bound::exclusive(15),
        )),
        StandardSchema::InRangeInt(Range::<i64>::bounded(
            Bound::exclusive(6),
            Bound::exclusive(11),
        )),
    ];

    let not_related_schemas = vec![
        StandardSchema::InRangeInt(Range::<i64>::bounded(
            Bound::exclusive(-5),
            Bound::exclusive(-15),
        )),
        StandardSchema::InRangeInt(Range::<i64>::bounded(
            Bound::exclusive(6),
            Bound::exclusive(255),
        )),
    ];

    assert_equal(schema.clone(), equal_schemas);
    assert_less_than(schema.clone(), greater_schemas);
    assert_greater_than(schema.clone(), lesser_schemas);
    assert_not_related(schema, not_related_schemas);
}

#[test]
fn compare_in_range_float() {
    let schema = StandardSchema::InRangeFloat(Range::<f64>::bounded(
        Bound::inclusive(5.5),
        Bound::inclusive(15.15),
    ));

    let equal_schemas = vec![StandardSchema::InRangeFloat(Range::<f64>::bounded(
        Bound::inclusive(5.5),
        Bound::inclusive(15.15),
    ))];

    let greater_schemas = vec![
        StandardSchema::InRangeFloat(Range::<f64>::bounded(
            Bound::inclusive(4.4),
            Bound::inclusive(16.16),
        )),
        StandardSchema::InRangeFloat(Range::unbounded()),
    ];

    let lesser_schemas = vec![
        StandardSchema::InRangeFloat(Range::<f64>::bounded(
            Bound::exclusive(5.5),
            Bound::exclusive(15.15),
        )),
        StandardSchema::InRangeFloat(Range::<f64>::bounded(
            Bound::exclusive(6.6),
            Bound::exclusive(11.11),
        )),
    ];

    let not_related_schemas = vec![
        StandardSchema::InRangeFloat(Range::<f64>::bounded(
            Bound::exclusive(-5.5),
            Bound::exclusive(-15.15),
        )),
        StandardSchema::InRangeFloat(Range::<f64>::bounded(
            Bound::exclusive(6.6),
            Bound::exclusive(255.255),
        )),
    ];

    assert_equal(schema.clone(), equal_schemas);
    assert_less_than(schema.clone(), greater_schemas);
    assert_greater_than(schema.clone(), lesser_schemas);
    assert_not_related(schema, not_related_schemas);
}

#[test]
fn compare_in_range_float_finite() {
    let bounded = StandardSchema::InRangeFloat(Range::<f64>::bounded(
        Bound::inclusive(5.5),
        Bound::inclusive(15.15),
    ));
    let upper_bounded =
        StandardSchema::InRangeFloat(Range::<f64>::upper_bounded(Bound::inclusive(15.15)));
    let lower_bounded =
        StandardSchema::InRangeFloat(Range::<f64>::lower_bounded(Bound::inclusive(5.5)));
    let unbounded = StandardSchema::InRangeFloat(Range::<f64>::unbounded());

    assert_less_than(bounded, vec![StandardSchema::Finite]);
    assert_not_related(upper_bounded, vec![StandardSchema::Finite]);
    assert_not_related(lower_bounded, vec![StandardSchema::Finite]);
    assert_not_related(unbounded, vec![StandardSchema::Finite]);
}

#[test]
fn compare_in_range_float_non_nan() {
    let bounded = StandardSchema::InRangeFloat(Range::<f64>::bounded(
        Bound::inclusive(5.5),
        Bound::inclusive(15.15),
    ));
    let upper_bounded =
        StandardSchema::InRangeFloat(Range::<f64>::upper_bounded(Bound::inclusive(15.15)));
    let lower_bounded =
        StandardSchema::InRangeFloat(Range::<f64>::lower_bounded(Bound::inclusive(5.5)));
    let unbounded = StandardSchema::InRangeFloat(Range::unbounded());

    assert_less_than(bounded, vec![StandardSchema::NonNan]);
    assert_less_than(upper_bounded, vec![StandardSchema::NonNan]);
    assert_less_than(lower_bounded, vec![StandardSchema::NonNan]);
    assert_not_related(unbounded, vec![StandardSchema::NonNan]);
}

#[test]
fn compare_non_nan() {
    let schema = StandardSchema::NonNan;

    let equal_schemas = vec![StandardSchema::NonNan];
    let lesser_schemas = vec![StandardSchema::Finite];

    assert_equal(schema.clone(), equal_schemas);
    assert_greater_than(schema, lesser_schemas);
}

#[test]
fn compare_text_non_empty() {
    let schema = StandardSchema::Text(TextSchema::NonEmpty);

    let equal_schemas = vec![StandardSchema::Text(TextSchema::NonEmpty)];
    let lesser_schemas = vec![
        StandardSchema::Text(TextSchema::Exact("foo".to_string())),
        StandardSchema::Text(TextSchema::Matches(Regex::new("\\w+").unwrap())),
    ];

    let not_related_schemas = vec![
        StandardSchema::Text(TextSchema::Exact("".to_string())),
        StandardSchema::Text(TextSchema::Matches(Regex::new("\\w*").unwrap())),
    ];

    assert_equal(schema.clone(), equal_schemas);
    assert_greater_than(schema.clone(), lesser_schemas);
    assert_not_related(schema, not_related_schemas)
}

#[test]
fn compare_text_exact() {
    let schema = StandardSchema::Text(TextSchema::Exact("test".to_string()));

    let equal_schemas = vec![StandardSchema::Text(TextSchema::Exact("test".to_string()))];
    let greater_schemas = vec![
        StandardSchema::Text(TextSchema::NonEmpty),
        StandardSchema::Text(TextSchema::Matches(Regex::new("\\w*").unwrap())),
    ];

    let not_related_schemas = vec![
        StandardSchema::Text(TextSchema::Exact("not_a_test".to_string())),
        StandardSchema::Text(TextSchema::Matches(Regex::new("\\w*_").unwrap())),
    ];

    assert_equal(schema.clone(), equal_schemas);
    assert_less_than(schema.clone(), greater_schemas);
    assert_not_related(schema, not_related_schemas);

    assert_not_related(
        StandardSchema::Text(TextSchema::Exact("".to_string())),
        vec![StandardSchema::Text(TextSchema::NonEmpty)],
    )
}

#[test]
fn compare_text_matches() {
    let schema = StandardSchema::Text(TextSchema::Matches(Regex::new("\\w+").unwrap()));

    let equal_schemas = vec![StandardSchema::Text(TextSchema::Matches(
        Regex::new("\\w+").unwrap(),
    ))];

    let greater_schemas = vec![StandardSchema::Text(TextSchema::NonEmpty)];
    let lesser_schemas = vec![StandardSchema::Text(TextSchema::Exact("foo".to_string()))];
    let not_related_schemas = vec![StandardSchema::Text(TextSchema::Exact("@@@".to_string()))];

    assert_equal(schema.clone(), equal_schemas);
    assert_greater_than(schema.clone(), lesser_schemas);
    assert_less_than(schema.clone(), greater_schemas);
    assert_not_related(schema, not_related_schemas);

    assert_not_related(
        StandardSchema::Text(TextSchema::Matches(Regex::new("\\w*").unwrap())),
        vec![StandardSchema::Text(TextSchema::NonEmpty)],
    )
}

#[test]
fn compare_all_items_all_items() {
    let schema =
        StandardSchema::AllItems(Box::new(ItemSchema::ValueItem(StandardSchema::InRangeInt(
            Range::<i64>::bounded(Bound::inclusive(5), Bound::inclusive(15)),
        ))));

    let equal_schemas = vec![
        StandardSchema::AllItems(Box::new(ItemSchema::ValueItem(StandardSchema::InRangeInt(
            Range::<i64>::bounded(Bound::inclusive(5), Bound::inclusive(15)),
        )))),
        StandardSchema::AllItems(Box::new(ItemSchema::ValueItem(StandardSchema::InRangeInt(
            Range::<i64>::bounded(Bound::exclusive(4), Bound::exclusive(16)),
        )))),
    ];

    let greater_schemas = vec![StandardSchema::AllItems(Box::new(ItemSchema::ValueItem(
        StandardSchema::InRangeInt(Range::<i64>::bounded(
            Bound::exclusive(1),
            Bound::exclusive(20),
        )),
    )))];

    let lesser_schemas = vec![StandardSchema::AllItems(Box::new(ItemSchema::ValueItem(
        StandardSchema::InRangeInt(Range::<i64>::bounded(
            Bound::exclusive(10),
            Bound::exclusive(12),
        )),
    )))];

    let not_related_schemas = vec![StandardSchema::AllItems(Box::new(ItemSchema::ValueItem(
        StandardSchema::InRangeInt(Range::<i64>::bounded(
            Bound::exclusive(-11),
            Bound::exclusive(-2),
        )),
    )))];

    assert_equal(schema.clone(), equal_schemas);
    assert_less_than(schema.clone(), greater_schemas);
    assert_greater_than(schema.clone(), lesser_schemas);
    assert_not_related(schema, not_related_schemas);
}

#[test]
fn compare_all_items_head_attribute() {
    let schema =
        StandardSchema::AllItems(Box::new(ItemSchema::ValueItem(StandardSchema::InRangeInt(
            Range::<i64>::bounded(Bound::inclusive(5), Bound::inclusive(15)),
        ))));

    let lesser_schemas = vec![StandardSchema::HeadAttribute {
        schema: Box::new(AttrSchema::new(
            TextSchema::NonEmpty,
            StandardSchema::Equal(Value::Int32Value(10)),
        )),
        required: true,
        remainder: Box::new(StandardSchema::AllItems(Box::new(ItemSchema::ValueItem(
            StandardSchema::InRangeInt(Range::<i64>::bounded(
                Bound::exclusive(10),
                Bound::exclusive(12),
            )),
        )))),
    }];
    let not_related_schemas = vec![StandardSchema::HeadAttribute {
        schema: Box::new(AttrSchema::new(
            TextSchema::NonEmpty,
            StandardSchema::Equal(Value::Int32Value(10)),
        )),
        required: true,
        remainder: Box::new(StandardSchema::AllItems(Box::new(ItemSchema::ValueItem(
            StandardSchema::InRangeInt(Range::<i64>::bounded(
                Bound::exclusive(-10),
                Bound::exclusive(40),
            )),
        )))),
    }];

    assert_greater_than(schema.clone(), lesser_schemas);
    assert_not_related(schema, not_related_schemas);
}

#[test]
fn compare_all_items_has_attributes() {
    let schema =
        StandardSchema::AllItems(Box::new(ItemSchema::ValueItem(StandardSchema::InRangeInt(
            Range::<i64>::bounded(Bound::inclusive(5), Bound::inclusive(15)),
        ))));

    let greater_schemas = vec![StandardSchema::HasAttributes {
        attributes: vec![],
        exhaustive: false,
    }];
    let not_related_schemas = vec![
        StandardSchema::HasAttributes {
            attributes: vec![],
            exhaustive: true,
        },
        StandardSchema::HasAttributes {
            attributes: vec![FieldSpec::new(
                AttrSchema::new(
                    TextSchema::NonEmpty,
                    StandardSchema::OfKind(ValueKind::Int32),
                ),
                true,
                false,
            )],
            exhaustive: false,
        },
    ];

    assert_less_than(schema.clone(), greater_schemas);
    assert_not_related(schema, not_related_schemas);
}

#[test]
fn compare_all_items_has_slots() {
    let schema = StandardSchema::AllItems(Box::new(ItemSchema::Field(SlotSchema::new(
        StandardSchema::Text(TextSchema::Exact("foo".to_string())),
        StandardSchema::InRangeInt(Range::<i64>::bounded(
            Bound::inclusive(5),
            Bound::inclusive(15),
        )),
    ))));

    let greater_schemas = vec![StandardSchema::HasSlots {
        slots: vec![],
        exhaustive: false,
    }];
    let lesser_schemas = vec![
        StandardSchema::HasSlots {
            slots: vec![FieldSpec::new(
                SlotSchema::new(
                    StandardSchema::Text(TextSchema::Exact("foo".to_string())),
                    StandardSchema::Equal(Value::Int32Value(6)),
                ),
                true,
                false,
            )],
            exhaustive: true,
        },
        StandardSchema::HasSlots {
            slots: vec![
                FieldSpec::new(
                    SlotSchema::new(
                        StandardSchema::Text(TextSchema::Exact("foo".to_string())),
                        StandardSchema::Equal(Value::Int32Value(6)),
                    ),
                    true,
                    false,
                ),
                FieldSpec::new(
                    SlotSchema::new(
                        StandardSchema::Text(TextSchema::Exact("foo".to_string())),
                        StandardSchema::Equal(Value::Int32Value(7)),
                    ),
                    true,
                    false,
                ),
                FieldSpec::new(
                    SlotSchema::new(
                        StandardSchema::Text(TextSchema::Exact("foo".to_string())),
                        StandardSchema::Equal(Value::Int32Value(8)),
                    ),
                    true,
                    true,
                ),
            ],
            exhaustive: true,
        },
    ];
    let not_related_schemas = vec![
        StandardSchema::HasSlots {
            slots: vec![FieldSpec::new(
                SlotSchema::new(
                    StandardSchema::OfKind(ValueKind::Int32),
                    StandardSchema::OfKind(ValueKind::Int32),
                ),
                true,
                false,
            )],
            exhaustive: false,
        },
        StandardSchema::HasSlots {
            slots: vec![
                FieldSpec::new(
                    SlotSchema::new(
                        StandardSchema::Text(TextSchema::Exact("foo".to_string())),
                        StandardSchema::Equal(Value::Int32Value(6)),
                    ),
                    true,
                    false,
                ),
                FieldSpec::new(
                    SlotSchema::new(
                        StandardSchema::Text(TextSchema::NonEmpty),
                        StandardSchema::Equal(Value::Int32Value(7)),
                    ),
                    true,
                    false,
                ),
            ],
            exhaustive: true,
        },
    ];

    assert_greater_than(schema.clone(), lesser_schemas);
    assert_less_than(schema.clone(), greater_schemas);
    assert_not_related(schema, not_related_schemas);
}

#[test]
fn compare_all_items_layout() {
    let schema =
        StandardSchema::AllItems(Box::new(ItemSchema::ValueItem(StandardSchema::InRangeInt(
            Range::<i64>::bounded(Bound::inclusive(5), Bound::inclusive(15)),
        ))));

    let greater_schemas = vec![StandardSchema::Layout {
        items: vec![],
        exhaustive: false,
    }];

    let lesser_schemas = vec![
        StandardSchema::Layout {
            items: vec![(
                ItemSchema::ValueItem(StandardSchema::InRangeInt(Range::<i64>::bounded(
                    Bound::inclusive(5),
                    Bound::inclusive(15),
                ))),
                false,
            )],
            exhaustive: true,
        },
        StandardSchema::Layout {
            items: vec![
                (
                    ItemSchema::ValueItem(StandardSchema::InRangeInt(Range::<i64>::bounded(
                        Bound::inclusive(5),
                        Bound::inclusive(15),
                    ))),
                    true,
                ),
                (
                    ItemSchema::ValueItem(StandardSchema::InRangeInt(Range::<i64>::bounded(
                        Bound::inclusive(6),
                        Bound::inclusive(8),
                    ))),
                    false,
                ),
                (
                    ItemSchema::ValueItem(StandardSchema::InRangeInt(Range::<i64>::bounded(
                        Bound::inclusive(7),
                        Bound::inclusive(9),
                    ))),
                    true,
                ),
            ],
            exhaustive: true,
        },
    ];
    let not_related_schemas = vec![
        StandardSchema::Layout {
            items: vec![(
                ItemSchema::ValueItem(StandardSchema::Text(TextSchema::NonEmpty)),
                false,
            )],
            exhaustive: false,
        },
        StandardSchema::Layout {
            items: vec![
                (
                    ItemSchema::ValueItem(StandardSchema::InRangeInt(Range::<i64>::bounded(
                        Bound::inclusive(5),
                        Bound::inclusive(15),
                    ))),
                    true,
                ),
                (
                    ItemSchema::ValueItem(StandardSchema::InRangeInt(Range::<i64>::bounded(
                        Bound::inclusive(6),
                        Bound::inclusive(8),
                    ))),
                    false,
                ),
                (
                    ItemSchema::ValueItem(StandardSchema::InRangeInt(Range::<i64>::bounded(
                        Bound::inclusive(-1),
                        Bound::inclusive(0),
                    ))),
                    true,
                ),
            ],
            exhaustive: true,
        },
    ];

    assert_greater_than(schema.clone(), lesser_schemas);
    assert_less_than(schema.clone(), greater_schemas);
    assert_not_related(schema, not_related_schemas);
}

#[test]
fn compare_num_attrs_head_attribute() {
    let schema = StandardSchema::NumAttrs(3);

    let equal_schemas = vec![StandardSchema::NumAttrs(3)];

    let lesser_schemas = vec![
        StandardSchema::HeadAttribute {
            schema: Box::new(AttrSchema::new(
                TextSchema::NonEmpty,
                StandardSchema::Equal(Value::Int32Value(10)),
            )),
            required: true,
            remainder: Box::new(StandardSchema::NumAttrs(2)),
        },
        StandardSchema::HeadAttribute {
            schema: Box::new(AttrSchema::new(
                TextSchema::NonEmpty,
                StandardSchema::Equal(Value::Int32Value(10)),
            )),
            required: true,
            remainder: Box::new(StandardSchema::HeadAttribute {
                schema: Box::new(AttrSchema::new(
                    TextSchema::NonEmpty,
                    StandardSchema::Equal(Value::Int32Value(10)),
                )),
                required: true,
                remainder: Box::new(StandardSchema::NumAttrs(1)),
            }),
        },
    ];
    let not_related_schemas = vec![
        StandardSchema::HeadAttribute {
            schema: Box::new(AttrSchema::new(
                TextSchema::NonEmpty,
                StandardSchema::Equal(Value::Int32Value(10)),
            )),
            required: true,
            remainder: Box::new(StandardSchema::NumAttrs(1)),
        },
        StandardSchema::HeadAttribute {
            schema: Box::new(AttrSchema::new(
                TextSchema::NonEmpty,
                StandardSchema::Equal(Value::Int32Value(10)),
            )),
            required: true,
            remainder: Box::new(StandardSchema::HasSlots {
                slots: vec![],
                exhaustive: false,
            }),
        },
    ];

    assert_equal(schema.clone(), equal_schemas);
    assert_greater_than(schema.clone(), lesser_schemas);
    assert_not_related(schema, not_related_schemas);
}

#[test]
fn compare_num_attrs_has_attributes() {
    let schema = StandardSchema::NumAttrs(3);

    let greater_schemas = vec![StandardSchema::HasAttributes {
        attributes: vec![],
        exhaustive: false,
    }];

    let lesser_schemas = vec![StandardSchema::HasAttributes {
        attributes: vec![
            FieldSpec::new(
                AttrSchema::new(
                    TextSchema::Exact("first".to_string()),
                    StandardSchema::Equal(Value::Int32Value(3)),
                ),
                true,
                true,
            ),
            FieldSpec::new(
                AttrSchema::new(
                    TextSchema::Exact("second".to_string()),
                    StandardSchema::Equal(Value::Int32Value(24)),
                ),
                true,
                false,
            ),
            FieldSpec::new(
                AttrSchema::new(
                    TextSchema::Exact("third".to_string()),
                    StandardSchema::Equal(Value::Int32Value(12)),
                ),
                true,
                true,
            ),
        ],
        exhaustive: true,
    }];

    let not_related_schemas = vec![
        StandardSchema::HasAttributes {
            attributes: vec![
                FieldSpec::new(
                    AttrSchema::new(
                        TextSchema::Exact("first".to_string()),
                        StandardSchema::Equal(Value::Int32Value(3)),
                    ),
                    true,
                    true,
                ),
                FieldSpec::new(
                    AttrSchema::new(
                        TextSchema::Exact("second".to_string()),
                        StandardSchema::Equal(Value::Int32Value(24)),
                    ),
                    false,
                    false,
                ),
                FieldSpec::new(
                    AttrSchema::new(
                        TextSchema::Exact("third".to_string()),
                        StandardSchema::Equal(Value::Int32Value(12)),
                    ),
                    true,
                    true,
                ),
            ],
            exhaustive: true,
        },
        StandardSchema::HasAttributes {
            attributes: vec![
                FieldSpec::new(
                    AttrSchema::new(
                        TextSchema::Exact("first".to_string()),
                        StandardSchema::Equal(Value::Int32Value(3)),
                    ),
                    true,
                    true,
                ),
                FieldSpec::new(
                    AttrSchema::new(
                        TextSchema::Exact("second".to_string()),
                        StandardSchema::Equal(Value::Int32Value(24)),
                    ),
                    true,
                    false,
                ),
                FieldSpec::new(
                    AttrSchema::new(
                        TextSchema::Exact("third".to_string()),
                        StandardSchema::Equal(Value::Int32Value(12)),
                    ),
                    true,
                    true,
                ),
                FieldSpec::new(
                    AttrSchema::new(
                        TextSchema::Exact("third".to_string()),
                        StandardSchema::Equal(Value::Int32Value(12)),
                    ),
                    true,
                    true,
                ),
            ],
            exhaustive: true,
        },
        StandardSchema::HasAttributes {
            attributes: vec![FieldSpec::new(
                AttrSchema::new(
                    TextSchema::Exact("first".to_string()),
                    StandardSchema::Equal(Value::Int32Value(3)),
                ),
                true,
                true,
            )],
            exhaustive: true,
        },
        StandardSchema::HasAttributes {
            attributes: vec![
                FieldSpec::new(
                    AttrSchema::new(
                        TextSchema::Exact("first".to_string()),
                        StandardSchema::Equal(Value::Int32Value(3)),
                    ),
                    true,
                    true,
                ),
                FieldSpec::new(
                    AttrSchema::new(
                        TextSchema::Exact("second".to_string()),
                        StandardSchema::Equal(Value::Int32Value(24)),
                    ),
                    true,
                    false,
                ),
                FieldSpec::new(
                    AttrSchema::new(
                        TextSchema::Exact("third".to_string()),
                        StandardSchema::Equal(Value::Int32Value(12)),
                    ),
                    true,
                    true,
                ),
            ],
            exhaustive: false,
        },
    ];

    assert_greater_than(schema.clone(), lesser_schemas);
    assert_less_than(schema.clone(), greater_schemas);
    assert_not_related(schema, not_related_schemas);
}

#[test]
fn compare_num_attrs_has_slots() {
    let schema = StandardSchema::NumAttrs(3);

    let greater_schemas = vec![StandardSchema::HasSlots {
        slots: vec![],
        exhaustive: false,
    }];
    let not_related_schemas = vec![
        StandardSchema::HasSlots {
            slots: vec![],
            exhaustive: true,
        },
        StandardSchema::HasSlots {
            slots: vec![FieldSpec::new(
                SlotSchema::new(
                    StandardSchema::OfKind(ValueKind::Text),
                    StandardSchema::OfKind(ValueKind::Text),
                ),
                false,
                false,
            )],
            exhaustive: false,
        },
    ];

    assert_less_than(schema.clone(), greater_schemas);
    assert_not_related(schema, not_related_schemas);
}

#[test]
fn compare_num_attrs_layout() {
    let schema = StandardSchema::NumAttrs(3);

    let greater_schemas = vec![StandardSchema::Layout {
        items: vec![],
        exhaustive: false,
    }];
    let not_related_schemas = vec![
        StandardSchema::Layout {
            items: vec![],
            exhaustive: true,
        },
        StandardSchema::Layout {
            items: vec![(
                ItemSchema::Field(SlotSchema::new(
                    StandardSchema::OfKind(ValueKind::Text),
                    StandardSchema::OfKind(ValueKind::Text),
                )),
                false,
            )],
            exhaustive: false,
        },
        StandardSchema::Layout {
            items: vec![
                (
                    ItemSchema::Field(SlotSchema::new(
                        StandardSchema::OfKind(ValueKind::Text),
                        StandardSchema::OfKind(ValueKind::Text),
                    )),
                    true,
                ),
                (
                    ItemSchema::Field(SlotSchema::new(
                        StandardSchema::OfKind(ValueKind::Text),
                        StandardSchema::OfKind(ValueKind::Text),
                    )),
                    true,
                ),
            ],
            exhaustive: false,
        },
    ];

    assert_less_than(schema.clone(), greater_schemas);
    assert_not_related(schema, not_related_schemas);
}

#[test]
fn compare_num_items_head_attribute() {
    let schema = StandardSchema::NumItems(3);

    let equal_schemas = vec![StandardSchema::NumItems(3)];

    let lesser_schemas = vec![
        StandardSchema::HeadAttribute {
            schema: Box::new(AttrSchema::new(
                TextSchema::NonEmpty,
                StandardSchema::Equal(Value::Int32Value(10)),
            )),
            required: true,
            remainder: Box::new(StandardSchema::NumItems(3)),
        },
        StandardSchema::HeadAttribute {
            schema: Box::new(AttrSchema::new(
                TextSchema::NonEmpty,
                StandardSchema::Equal(Value::Int32Value(10)),
            )),
            required: true,
            remainder: Box::new(StandardSchema::HasSlots {
                slots: vec![
                    FieldSpec::new(
                        SlotSchema::new(
                            StandardSchema::Equal(Value::Int32Value(1)),
                            StandardSchema::Equal(Value::Int32Value(1)),
                        ),
                        true,
                        false,
                    ),
                    FieldSpec::new(
                        SlotSchema::new(
                            StandardSchema::Equal(Value::Int32Value(1)),
                            StandardSchema::Equal(Value::Int32Value(1)),
                        ),
                        true,
                        false,
                    ),
                    FieldSpec::new(
                        SlotSchema::new(
                            StandardSchema::Equal(Value::Int32Value(1)),
                            StandardSchema::Equal(Value::Int32Value(1)),
                        ),
                        true,
                        false,
                    ),
                ],
                exhaustive: true,
            }),
        },
    ];

    let not_related_schemas = vec![StandardSchema::HeadAttribute {
        schema: Box::new(AttrSchema::new(
            TextSchema::NonEmpty,
            StandardSchema::Equal(Value::Int32Value(10)),
        )),
        required: true,
        remainder: Box::new(StandardSchema::NumItems(4)),
    }];

    assert_equal(schema.clone(), equal_schemas);
    assert_greater_than(schema.clone(), lesser_schemas);
    assert_not_related(schema, not_related_schemas);
}

#[test]
fn compare_num_items_has_attributes() {
    let schema = StandardSchema::NumItems(3);

    let greater_schemas = vec![StandardSchema::HasAttributes {
        attributes: vec![],
        exhaustive: false,
    }];
    let not_related_schemas = vec![
        StandardSchema::HasAttributes {
            attributes: vec![],
            exhaustive: true,
        },
        StandardSchema::HasAttributes {
            attributes: vec![FieldSpec::new(
                AttrSchema::new(
                    TextSchema::NonEmpty,
                    StandardSchema::OfKind(ValueKind::Text),
                ),
                false,
                false,
            )],
            exhaustive: false,
        },
    ];

    assert_less_than(schema.clone(), greater_schemas);
    assert_not_related(schema, not_related_schemas);
}

#[test]
fn compare_num_items_has_slots() {
    let schema = StandardSchema::NumItems(3);

    let greater_schemas = vec![StandardSchema::HasSlots {
        slots: vec![],
        exhaustive: false,
    }];

    let lesser_schemas = vec![StandardSchema::HasSlots {
        slots: vec![
            FieldSpec::new(
                SlotSchema::new(
                    StandardSchema::Equal(Value::Int32Value(3)),
                    StandardSchema::Equal(Value::Int32Value(3)),
                ),
                true,
                true,
            ),
            FieldSpec::new(
                SlotSchema::new(
                    StandardSchema::Equal(Value::Int32Value(3)),
                    StandardSchema::Equal(Value::Int32Value(24)),
                ),
                true,
                false,
            ),
            FieldSpec::new(
                SlotSchema::new(
                    StandardSchema::Equal(Value::Int32Value(3)),
                    StandardSchema::Equal(Value::Int32Value(12)),
                ),
                true,
                true,
            ),
        ],
        exhaustive: true,
    }];

    let not_related_schemas = vec![
        StandardSchema::HasSlots {
            slots: vec![
                FieldSpec::new(
                    SlotSchema::new(
                        StandardSchema::Equal(Value::Int32Value(3)),
                        StandardSchema::Equal(Value::Int32Value(3)),
                    ),
                    true,
                    true,
                ),
                FieldSpec::new(
                    SlotSchema::new(
                        StandardSchema::Equal(Value::Int32Value(3)),
                        StandardSchema::Equal(Value::Int32Value(24)),
                    ),
                    false,
                    false,
                ),
                FieldSpec::new(
                    SlotSchema::new(
                        StandardSchema::Equal(Value::Int32Value(3)),
                        StandardSchema::Equal(Value::Int32Value(12)),
                    ),
                    true,
                    true,
                ),
            ],
            exhaustive: true,
        },
        StandardSchema::HasSlots {
            slots: vec![
                FieldSpec::new(
                    SlotSchema::new(
                        StandardSchema::Equal(Value::Int32Value(3)),
                        StandardSchema::Equal(Value::Int32Value(3)),
                    ),
                    true,
                    true,
                ),
                FieldSpec::new(
                    SlotSchema::new(
                        StandardSchema::Equal(Value::Int32Value(3)),
                        StandardSchema::Equal(Value::Int32Value(24)),
                    ),
                    true,
                    false,
                ),
                FieldSpec::new(
                    SlotSchema::new(
                        StandardSchema::Equal(Value::Int32Value(3)),
                        StandardSchema::Equal(Value::Int32Value(12)),
                    ),
                    true,
                    true,
                ),
                FieldSpec::new(
                    SlotSchema::new(
                        StandardSchema::Equal(Value::Int32Value(3)),
                        StandardSchema::Equal(Value::Int32Value(12)),
                    ),
                    true,
                    true,
                ),
            ],
            exhaustive: true,
        },
        StandardSchema::HasSlots {
            slots: vec![FieldSpec::new(
                SlotSchema::new(
                    StandardSchema::Equal(Value::Int32Value(3)),
                    StandardSchema::Equal(Value::Int32Value(3)),
                ),
                true,
                true,
            )],
            exhaustive: true,
        },
        StandardSchema::HasSlots {
            slots: vec![
                FieldSpec::new(
                    SlotSchema::new(
                        StandardSchema::Equal(Value::Int32Value(3)),
                        StandardSchema::Equal(Value::Int32Value(3)),
                    ),
                    true,
                    true,
                ),
                FieldSpec::new(
                    SlotSchema::new(
                        StandardSchema::Equal(Value::Int32Value(3)),
                        StandardSchema::Equal(Value::Int32Value(24)),
                    ),
                    true,
                    false,
                ),
                FieldSpec::new(
                    SlotSchema::new(
                        StandardSchema::Equal(Value::Int32Value(3)),
                        StandardSchema::Equal(Value::Int32Value(12)),
                    ),
                    true,
                    true,
                ),
            ],
            exhaustive: false,
        },
    ];

    assert_greater_than(schema.clone(), lesser_schemas);
    assert_less_than(schema.clone(), greater_schemas);
    assert_not_related(schema, not_related_schemas);
}

#[test]
fn compare_num_items_layout() {
    let schema = StandardSchema::NumItems(3);

    let greater_schemas = vec![StandardSchema::Layout {
        items: vec![],
        exhaustive: false,
    }];

    let lesser_schemas = vec![StandardSchema::Layout {
        items: vec![
            (
                ItemSchema::ValueItem(StandardSchema::Text(TextSchema::NonEmpty)),
                true,
            ),
            (
                ItemSchema::ValueItem(StandardSchema::Text(TextSchema::NonEmpty)),
                true,
            ),
            (
                ItemSchema::ValueItem(StandardSchema::Text(TextSchema::NonEmpty)),
                true,
            ),
        ],
        exhaustive: true,
    }];

    let not_related_schemas = vec![
        StandardSchema::Layout {
            items: vec![],
            exhaustive: true,
        },
        StandardSchema::Layout {
            items: vec![(
                ItemSchema::Field(SlotSchema::new(
                    StandardSchema::OfKind(ValueKind::Text),
                    StandardSchema::OfKind(ValueKind::Text),
                )),
                false,
            )],
            exhaustive: false,
        },
        StandardSchema::Layout {
            items: vec![
                (
                    ItemSchema::Field(SlotSchema::new(
                        StandardSchema::OfKind(ValueKind::Text),
                        StandardSchema::OfKind(ValueKind::Text),
                    )),
                    true,
                ),
                (
                    ItemSchema::Field(SlotSchema::new(
                        StandardSchema::OfKind(ValueKind::Text),
                        StandardSchema::OfKind(ValueKind::Text),
                    )),
                    true,
                ),
            ],
            exhaustive: false,
        },
        StandardSchema::Layout {
            items: vec![
                (
                    ItemSchema::ValueItem(StandardSchema::Text(TextSchema::NonEmpty)),
                    true,
                ),
                (
                    ItemSchema::ValueItem(StandardSchema::Text(TextSchema::NonEmpty)),
                    true,
                ),
                (
                    ItemSchema::ValueItem(StandardSchema::Text(TextSchema::NonEmpty)),
                    true,
                ),
            ],
            exhaustive: false,
        },
        StandardSchema::Layout {
            items: vec![
                (
                    ItemSchema::ValueItem(StandardSchema::Text(TextSchema::NonEmpty)),
                    true,
                ),
                (
                    ItemSchema::ValueItem(StandardSchema::Text(TextSchema::NonEmpty)),
                    false,
                ),
                (
                    ItemSchema::ValueItem(StandardSchema::Text(TextSchema::NonEmpty)),
                    true,
                ),
            ],
            exhaustive: true,
        },
        StandardSchema::Layout {
            items: vec![
                (
                    ItemSchema::ValueItem(StandardSchema::Text(TextSchema::NonEmpty)),
                    true,
                ),
                (
                    ItemSchema::ValueItem(StandardSchema::Text(TextSchema::NonEmpty)),
                    true,
                ),
            ],
            exhaustive: true,
        },
        StandardSchema::Layout {
            items: vec![
                (
                    ItemSchema::ValueItem(StandardSchema::Text(TextSchema::NonEmpty)),
                    true,
                ),
                (
                    ItemSchema::ValueItem(StandardSchema::Text(TextSchema::NonEmpty)),
                    true,
                ),
                (
                    ItemSchema::ValueItem(StandardSchema::Text(TextSchema::NonEmpty)),
                    true,
                ),
                (
                    ItemSchema::ValueItem(StandardSchema::Text(TextSchema::NonEmpty)),
                    true,
                ),
            ],
            exhaustive: true,
        },
    ];

    assert_less_than(schema.clone(), greater_schemas);
    assert_greater_than(schema.clone(), lesser_schemas);
    assert_not_related(schema, not_related_schemas);
}

#[test]
fn compare_head_attr_head_attr() {
    let schema = StandardSchema::HeadAttribute {
        schema: Box::new(AttrSchema::new(
            TextSchema::NonEmpty,
            StandardSchema::Equal(Value::Int32Value(10)),
        )),
        required: true,
        remainder: Box::new(StandardSchema::Equal(Value::Float64Value(20.20))),
    };

    let equal_schemas = vec![
        StandardSchema::HeadAttribute {
            schema: Box::new(AttrSchema::new(
                TextSchema::NonEmpty,
                StandardSchema::Equal(Value::Int32Value(10)),
            )),
            required: true,
            remainder: Box::new(StandardSchema::Equal(Value::Float64Value(20.20))),
        },
        StandardSchema::HeadAttribute {
            schema: Box::new(AttrSchema::new(
                TextSchema::NonEmpty,
                StandardSchema::Equal(Value::Int64Value(10)),
            )),
            required: true,
            remainder: Box::new(StandardSchema::Equal(Value::Float64Value(20.20))),
        },
    ];

    let greater_schemas = vec![
        StandardSchema::HeadAttribute {
            schema: Box::new(AttrSchema::new(
                TextSchema::NonEmpty,
                StandardSchema::OfKind(ValueKind::Int32),
            )),
            required: true,
            remainder: Box::new(StandardSchema::Equal(Value::Float64Value(20.20))),
        },
        StandardSchema::HeadAttribute {
            schema: Box::new(AttrSchema::new(
                TextSchema::NonEmpty,
                StandardSchema::Equal(Value::Int32Value(10)),
            )),
            required: false,
            remainder: Box::new(StandardSchema::Equal(Value::Float64Value(20.20))),
        },
        StandardSchema::HeadAttribute {
            schema: Box::new(AttrSchema::new(
                TextSchema::NonEmpty,
                StandardSchema::Equal(Value::Int32Value(10)),
            )),
            required: true,
            remainder: Box::new(StandardSchema::OfKind(ValueKind::Float64)),
        },
        StandardSchema::HeadAttribute {
            schema: Box::new(AttrSchema::new(
                TextSchema::NonEmpty,
                StandardSchema::OfKind(ValueKind::Int32),
            )),
            required: false,
            remainder: Box::new(StandardSchema::OfKind(ValueKind::Float64)),
        },
    ];

    let lesser_schemas = vec![StandardSchema::HeadAttribute {
        schema: Box::new(AttrSchema::new(
            TextSchema::Exact("test".to_string()),
            StandardSchema::Equal(Value::Int32Value(10)),
        )),
        required: true,
        remainder: Box::new(StandardSchema::Equal(Value::Float64Value(20.20))),
    }];

    let not_related_schemas = vec![
        StandardSchema::HeadAttribute {
            schema: Box::new(AttrSchema::new(
                TextSchema::Exact("test".to_string()),
                StandardSchema::Equal(Value::Int32Value(10)),
            )),
            required: false,
            remainder: Box::new(StandardSchema::Equal(Value::Float64Value(20.20))),
        },
        StandardSchema::HeadAttribute {
            schema: Box::new(AttrSchema::new(
                TextSchema::Exact("test".to_string()),
                StandardSchema::Equal(Value::Int32Value(10)),
            )),
            required: true,
            remainder: Box::new(StandardSchema::OfKind(ValueKind::Float64)),
        },
        StandardSchema::HeadAttribute {
            schema: Box::new(AttrSchema::new(
                TextSchema::Exact("test".to_string()),
                StandardSchema::Equal(Value::Int32Value(10)),
            )),
            required: false,
            remainder: Box::new(StandardSchema::OfKind(ValueKind::Float64)),
        },
    ];

    assert_equal(schema.clone(), equal_schemas);
    assert_less_than(schema.clone(), greater_schemas);
    assert_greater_than(schema.clone(), lesser_schemas);
    assert_not_related(schema, not_related_schemas);
}

#[test]
fn compare_head_attr_has_attrs() {
    let schema = StandardSchema::HeadAttribute {
        schema: Box::new(AttrSchema::new(
            TextSchema::Exact("foo".to_string()),
            StandardSchema::Equal(Value::Int32Value(10)),
        )),
        required: true,
        remainder: Box::new(StandardSchema::HasAttributes {
            attributes: vec![FieldSpec::new(
                AttrSchema::new(
                    TextSchema::Exact("foo".to_string()),
                    StandardSchema::Equal(Value::Int32Value(10)),
                ),
                false,
                false,
            )],
            exhaustive: true,
        }),
    };

    let equal_schemas = vec![StandardSchema::HasAttributes {
        attributes: vec![FieldSpec::new(
            AttrSchema::new(
                TextSchema::Exact("foo".to_string()),
                StandardSchema::Equal(Value::Int32Value(10)),
            ),
            false,
            false,
        )],
        exhaustive: true,
    }];

    let greater_schemas = vec![
        StandardSchema::HasAttributes {
            attributes: vec![],
            exhaustive: false,
        },
        StandardSchema::HasAttributes {
            attributes: vec![
                FieldSpec::new(
                    AttrSchema::new(
                        TextSchema::Exact("foo".to_string()),
                        StandardSchema::Equal(Value::Int32Value(10)),
                    ),
                    false,
                    false,
                ),
                FieldSpec::new(
                    AttrSchema::new(
                        TextSchema::NonEmpty,
                        StandardSchema::Equal(Value::Int32Value(10)),
                    ),
                    false,
                    false,
                ),
            ],
            exhaustive: true,
        },
    ];

    let not_related_schemas = vec![
        StandardSchema::HasAttributes {
            attributes: vec![FieldSpec::new(
                AttrSchema::new(
                    TextSchema::NonEmpty,
                    StandardSchema::OfKind(ValueKind::Int32),
                ),
                true,
                true,
            )],
            exhaustive: false,
        },
        StandardSchema::HasAttributes {
            attributes: vec![FieldSpec::new(
                AttrSchema::new(
                    TextSchema::Exact("foo".to_string()),
                    StandardSchema::Equal(Value::Int32Value(10)),
                ),
                false,
                false,
            )],
            exhaustive: false,
        },
    ];

    assert_equal(schema.clone(), equal_schemas);
    assert_less_than(schema.clone(), greater_schemas);
    assert_not_related(schema, not_related_schemas);
}

#[test]
fn compare_head_attr_has_slots() {
    let schema = StandardSchema::HeadAttribute {
        schema: Box::new(AttrSchema::new(
            TextSchema::NonEmpty,
            StandardSchema::Equal(Value::Int32Value(10)),
        )),
        required: true,
        remainder: Box::new(StandardSchema::Equal(Value::Float64Value(20.20))),
    };

    let greater_schemas = vec![StandardSchema::HasSlots {
        slots: vec![],
        exhaustive: false,
    }];

    let not_related_schemas = vec![
        StandardSchema::HasSlots {
            slots: vec![],
            exhaustive: true,
        },
        StandardSchema::HasSlots {
            slots: vec![FieldSpec::new(
                SlotSchema::new(
                    StandardSchema::Equal(Value::Int32Value(10)),
                    StandardSchema::Equal(Value::Int32Value(10)),
                ),
                false,
                false,
            )],
            exhaustive: false,
        },
    ];

    assert_less_than(schema.clone(), greater_schemas);
    assert_not_related(schema, not_related_schemas);
}

#[test]
fn compare_head_attr_layout() {
    let schema = StandardSchema::HeadAttribute {
        schema: Box::new(AttrSchema::new(
            TextSchema::NonEmpty,
            StandardSchema::Equal(Value::Int32Value(10)),
        )),
        required: true,
        remainder: Box::new(StandardSchema::Equal(Value::Float64Value(20.20))),
    };

    let greater_schemas = vec![StandardSchema::Layout {
        items: vec![],
        exhaustive: false,
    }];

    let not_related_schemas = vec![
        StandardSchema::Layout {
            items: vec![],
            exhaustive: true,
        },
        StandardSchema::Layout {
            items: vec![(
                ItemSchema::ValueItem(StandardSchema::Equal(Value::Int32Value(10))),
                false,
            )],
            exhaustive: false,
        },
    ];

    assert_less_than(schema.clone(), greater_schemas);
    assert_not_related(schema, not_related_schemas);
}

#[test]
fn compare_has_attrs_has_attrs_exhaustive() {
    let schema = StandardSchema::HasAttributes {
        attributes: vec![
            FieldSpec::new(
                AttrSchema::new(
                    TextSchema::Exact("ten".to_string()),
                    StandardSchema::Equal(Value::Int32Value(10)),
                ),
                false,
                false,
            ),
            FieldSpec::new(
                AttrSchema::new(
                    TextSchema::Exact("twenty".to_string()),
                    StandardSchema::Equal(Value::Int32Value(20)),
                ),
                false,
                false,
            ),
        ],
        exhaustive: true,
    };

    let equal_schemas = vec![
        StandardSchema::HasAttributes {
            attributes: vec![
                FieldSpec::new(
                    AttrSchema::new(
                        TextSchema::Exact("twenty".to_string()),
                        StandardSchema::Equal(Value::Int32Value(20)),
                    ),
                    false,
                    false,
                ),
                FieldSpec::new(
                    AttrSchema::new(
                        TextSchema::Exact("ten".to_string()),
                        StandardSchema::Equal(Value::Int32Value(10)),
                    ),
                    false,
                    false,
                ),
            ],
            exhaustive: true,
        },
        StandardSchema::HasAttributes {
            attributes: vec![
                FieldSpec::new(
                    AttrSchema::new(
                        TextSchema::Exact("twenty".to_string()),
                        StandardSchema::Equal(Value::Int32Value(20)),
                    ),
                    false,
                    false,
                ),
                FieldSpec::new(
                    AttrSchema::new(
                        TextSchema::Exact("ten".to_string()),
                        StandardSchema::Equal(Value::Int32Value(10)),
                    ),
                    false,
                    false,
                ),
            ],
            exhaustive: true,
        },
    ];

    let greater_schemas = vec![StandardSchema::HasAttributes {
        attributes: vec![
            FieldSpec::new(
                AttrSchema::new(
                    TextSchema::Exact("twenty".to_string()),
                    StandardSchema::Equal(Value::Int32Value(20)),
                ),
                false,
                false,
            ),
            FieldSpec::new(
                AttrSchema::new(
                    TextSchema::Exact("ten".to_string()),
                    StandardSchema::Equal(Value::Int32Value(10)),
                ),
                false,
                false,
            ),
            FieldSpec::new(
                AttrSchema::new(
                    TextSchema::Exact("thirty".to_string()),
                    StandardSchema::Equal(Value::Int32Value(30)),
                ),
                false,
                false,
            ),
        ],
        exhaustive: true,
    }];

    let lesser_schemas = vec![
        StandardSchema::HasAttributes {
            attributes: vec![FieldSpec::new(
                AttrSchema::new(
                    TextSchema::Exact("twenty".to_string()),
                    StandardSchema::Equal(Value::Int32Value(20)),
                ),
                false,
                false,
            )],
            exhaustive: true,
        },
        StandardSchema::HasAttributes {
            attributes: vec![FieldSpec::new(
                AttrSchema::new(
                    TextSchema::Exact("ten".to_string()),
                    StandardSchema::Equal(Value::Int32Value(10)),
                ),
                false,
                false,
            )],
            exhaustive: true,
        },
    ];

    let not_related_schemas = vec![
        StandardSchema::HasAttributes {
            attributes: vec![FieldSpec::new(
                AttrSchema::new(
                    TextSchema::Exact("fifty".to_string()),
                    StandardSchema::Equal(Value::Int32Value(50)),
                ),
                false,
                false,
            )],
            exhaustive: true,
        },
        StandardSchema::HasAttributes {
            attributes: vec![
                FieldSpec::new(
                    AttrSchema::new(
                        TextSchema::Exact("twenty".to_string()),
                        StandardSchema::Equal(Value::Int32Value(20)),
                    ),
                    false,
                    false,
                ),
                FieldSpec::new(
                    AttrSchema::new(
                        TextSchema::Exact("fifty".to_string()),
                        StandardSchema::Equal(Value::Int32Value(50)),
                    ),
                    false,
                    false,
                ),
                FieldSpec::new(
                    AttrSchema::new(
                        TextSchema::Exact("thirty".to_string()),
                        StandardSchema::Equal(Value::Int32Value(30)),
                    ),
                    false,
                    false,
                ),
            ],
            exhaustive: true,
        },
        StandardSchema::HasAttributes {
            attributes: vec![
                FieldSpec::new(
                    AttrSchema::new(
                        TextSchema::Exact("twenty".to_string()),
                        StandardSchema::Equal(Value::Int32Value(20)),
                    ),
                    false,
                    false,
                ),
                FieldSpec::new(
                    AttrSchema::new(
                        TextSchema::Exact("ten".to_string()),
                        StandardSchema::Equal(Value::Int32Value(10)),
                    ),
                    false,
                    false,
                ),
            ],
            exhaustive: false,
        },
        StandardSchema::HasAttributes {
            attributes: vec![
                FieldSpec::new(
                    AttrSchema::new(
                        TextSchema::Exact("twenty".to_string()),
                        StandardSchema::Equal(Value::Int32Value(20)),
                    ),
                    false,
                    false,
                ),
                FieldSpec::new(
                    AttrSchema::new(
                        TextSchema::Exact("fifty".to_string()),
                        StandardSchema::Equal(Value::Int32Value(50)),
                    ),
                    false,
                    false,
                ),
            ],
            exhaustive: true,
        },
        StandardSchema::HasAttributes {
            attributes: vec![
                FieldSpec::new(
                    AttrSchema::new(
                        TextSchema::Exact("ten".to_string()),
                        StandardSchema::Equal(Value::Int32Value(10)),
                    ),
                    false,
                    false,
                ),
                FieldSpec::new(
                    AttrSchema::new(
                        TextSchema::Exact("twenty".to_string()),
                        StandardSchema::Equal(Value::Int32Value(20)),
                    ),
                    false,
                    false,
                ),
                FieldSpec::new(
                    AttrSchema::new(
                        TextSchema::Exact("fifty".to_string()),
                        StandardSchema::Equal(Value::Int32Value(50)),
                    ),
                    true,
                    false,
                ),
            ],
            exhaustive: true,
        },
    ];

    assert_equal(schema.clone(), equal_schemas);
    assert_less_than(schema.clone(), greater_schemas);
    assert_greater_than(schema.clone(), lesser_schemas);
    assert_not_related(schema.clone(), not_related_schemas);
}

#[test]
fn compare_has_attrs_has_attrs_non_exhaustive() {
    let schema = StandardSchema::HasAttributes {
        attributes: vec![
            FieldSpec::new(
                AttrSchema::new(
                    TextSchema::Exact("ten".to_string()),
                    StandardSchema::Equal(Value::Int32Value(10)),
                ),
                false,
                false,
            ),
            FieldSpec::new(
                AttrSchema::new(
                    TextSchema::Exact("twenty".to_string()),
                    StandardSchema::Equal(Value::Int32Value(20)),
                ),
                false,
                false,
            ),
        ],
        exhaustive: false,
    };

    let equal_schemas = vec![StandardSchema::HasAttributes {
        attributes: vec![
            FieldSpec::new(
                AttrSchema::new(
                    TextSchema::Exact("twenty".to_string()),
                    StandardSchema::Equal(Value::Int32Value(20)),
                ),
                false,
                false,
            ),
            FieldSpec::new(
                AttrSchema::new(
                    TextSchema::Exact("ten".to_string()),
                    StandardSchema::Equal(Value::Int32Value(10)),
                ),
                false,
                false,
            ),
        ],
        exhaustive: false,
    }];

    let greater_schemas = vec![
        StandardSchema::HasAttributes {
            attributes: vec![FieldSpec::new(
                AttrSchema::new(
                    TextSchema::Exact("twenty".to_string()),
                    StandardSchema::Equal(Value::Int32Value(20)),
                ),
                false,
                false,
            )],
            exhaustive: false,
        },
        StandardSchema::HasAttributes {
            attributes: vec![FieldSpec::new(
                AttrSchema::new(
                    TextSchema::Exact("ten".to_string()),
                    StandardSchema::Equal(Value::Int32Value(10)),
                ),
                false,
                false,
            )],
            exhaustive: false,
        },
    ];

    let lesser_schemas = vec![StandardSchema::HasAttributes {
        attributes: vec![
            FieldSpec::new(
                AttrSchema::new(
                    TextSchema::Exact("twenty".to_string()),
                    StandardSchema::Equal(Value::Int32Value(20)),
                ),
                false,
                false,
            ),
            FieldSpec::new(
                AttrSchema::new(
                    TextSchema::Exact("ten".to_string()),
                    StandardSchema::Equal(Value::Int32Value(10)),
                ),
                false,
                false,
            ),
            FieldSpec::new(
                AttrSchema::new(
                    TextSchema::Exact("thirty".to_string()),
                    StandardSchema::Equal(Value::Int32Value(30)),
                ),
                false,
                false,
            ),
        ],
        exhaustive: false,
    }];

    let not_related_schemas = vec![
        StandardSchema::HasAttributes {
            attributes: vec![FieldSpec::new(
                AttrSchema::new(
                    TextSchema::Exact("fifty".to_string()),
                    StandardSchema::Equal(Value::Int32Value(50)),
                ),
                false,
                false,
            )],
            exhaustive: false,
        },
        StandardSchema::HasAttributes {
            attributes: vec![
                FieldSpec::new(
                    AttrSchema::new(
                        TextSchema::Exact("twenty".to_string()),
                        StandardSchema::Equal(Value::Int32Value(20)),
                    ),
                    false,
                    false,
                ),
                FieldSpec::new(
                    AttrSchema::new(
                        TextSchema::Exact("fifty".to_string()),
                        StandardSchema::Equal(Value::Int32Value(50)),
                    ),
                    false,
                    false,
                ),
                FieldSpec::new(
                    AttrSchema::new(
                        TextSchema::Exact("thirty".to_string()),
                        StandardSchema::Equal(Value::Int32Value(30)),
                    ),
                    false,
                    false,
                ),
            ],
            exhaustive: false,
        },
        StandardSchema::HasAttributes {
            attributes: vec![
                FieldSpec::new(
                    AttrSchema::new(
                        TextSchema::Exact("twenty".to_string()),
                        StandardSchema::Equal(Value::Int32Value(20)),
                    ),
                    false,
                    false,
                ),
                FieldSpec::new(
                    AttrSchema::new(
                        TextSchema::Exact("ten".to_string()),
                        StandardSchema::Equal(Value::Int32Value(10)),
                    ),
                    false,
                    false,
                ),
            ],
            exhaustive: true,
        },
    ];

    assert_equal(schema.clone(), equal_schemas);
    assert_less_than(schema.clone(), greater_schemas);
    assert_greater_than(schema.clone(), lesser_schemas);
    assert_not_related(schema.clone(), not_related_schemas);
}

#[test]
fn compare_has_attrs_has_slots() {
    assert_equal(
        StandardSchema::HasAttributes {
            attributes: vec![],
            exhaustive: false,
        },
        vec![
            StandardSchema::HasAttributes {
                attributes: vec![],
                exhaustive: false,
            },
            StandardSchema::HasSlots {
                slots: vec![],
                exhaustive: false,
            },
        ],
    );

    assert_greater_than(
        StandardSchema::HasAttributes {
            attributes: vec![],
            exhaustive: false,
        },
        vec![
            StandardSchema::HasSlots {
                slots: vec![],
                exhaustive: true,
            },
            StandardSchema::HasSlots {
                slots: vec![FieldSpec::new(
                    SlotSchema::new(
                        StandardSchema::Text(TextSchema::NonEmpty),
                        StandardSchema::Text(TextSchema::NonEmpty),
                    ),
                    false,
                    false,
                )],
                exhaustive: false,
            },
        ],
    );

    assert_less_than(
        StandardSchema::HasAttributes {
            attributes: vec![],
            exhaustive: true,
        },
        vec![StandardSchema::HasSlots {
            slots: vec![],
            exhaustive: false,
        }],
    );

    assert_less_than(
        StandardSchema::HasAttributes {
            attributes: vec![FieldSpec::new(
                AttrSchema::new(
                    TextSchema::NonEmpty,
                    StandardSchema::Text(TextSchema::NonEmpty),
                ),
                false,
                false,
            )],
            exhaustive: false,
        },
        vec![StandardSchema::HasSlots {
            slots: vec![],
            exhaustive: false,
        }],
    );

    assert_not_related(
        StandardSchema::HasAttributes {
            attributes: vec![FieldSpec::new(
                AttrSchema::new(
                    TextSchema::NonEmpty,
                    StandardSchema::Text(TextSchema::NonEmpty),
                ),
                false,
                false,
            )],
            exhaustive: false,
        },
        vec![StandardSchema::HasSlots {
            slots: vec![FieldSpec::new(
                SlotSchema::new(
                    StandardSchema::Text(TextSchema::NonEmpty),
                    StandardSchema::Text(TextSchema::NonEmpty),
                ),
                false,
                false,
            )],
            exhaustive: false,
        }],
    );
}

#[test]
fn compare_has_attrs_has_layout() {
    assert_equal(
        StandardSchema::HasAttributes {
            attributes: vec![],
            exhaustive: false,
        },
        vec![
            StandardSchema::HasAttributes {
                attributes: vec![],
                exhaustive: false,
            },
            StandardSchema::Layout {
                items: vec![],
                exhaustive: false,
            },
        ],
    );

    assert_greater_than(
        StandardSchema::HasAttributes {
            attributes: vec![],
            exhaustive: false,
        },
        vec![
            StandardSchema::Layout {
                items: vec![],
                exhaustive: true,
            },
            StandardSchema::Layout {
                items: vec![(
                    ItemSchema::ValueItem(StandardSchema::Text(TextSchema::NonEmpty)),
                    false,
                )],
                exhaustive: false,
            },
        ],
    );

    assert_less_than(
        StandardSchema::HasAttributes {
            attributes: vec![],
            exhaustive: true,
        },
        vec![StandardSchema::Layout {
            items: vec![],
            exhaustive: false,
        }],
    );

    assert_less_than(
        StandardSchema::HasAttributes {
            attributes: vec![FieldSpec::new(
                AttrSchema::new(
                    TextSchema::NonEmpty,
                    StandardSchema::Text(TextSchema::NonEmpty),
                ),
                false,
                false,
            )],
            exhaustive: false,
        },
        vec![StandardSchema::Layout {
            items: vec![],
            exhaustive: false,
        }],
    );

    assert_not_related(
        StandardSchema::HasAttributes {
            attributes: vec![FieldSpec::new(
                AttrSchema::new(
                    TextSchema::NonEmpty,
                    StandardSchema::Text(TextSchema::NonEmpty),
                ),
                false,
                false,
            )],
            exhaustive: false,
        },
        vec![StandardSchema::Layout {
            items: vec![(
                ItemSchema::ValueItem(StandardSchema::Text(TextSchema::NonEmpty)),
                false,
            )],
            exhaustive: false,
        }],
    );
}

#[test]
fn compare_has_slots_has_slots_exhaustive() {
    let schema = StandardSchema::HasSlots {
        slots: vec![
            FieldSpec::new(
                SlotSchema::new(
                    StandardSchema::Text(TextSchema::Exact("ten".to_string())),
                    StandardSchema::Equal(Value::Int32Value(10)),
                ),
                false,
                false,
            ),
            FieldSpec::new(
                SlotSchema::new(
                    StandardSchema::Text(TextSchema::Exact("twenty".to_string())),
                    StandardSchema::Equal(Value::Int32Value(20)),
                ),
                false,
                false,
            ),
        ],
        exhaustive: true,
    };

    let equal_schemas = vec![
        StandardSchema::HasSlots {
            slots: vec![
                FieldSpec::new(
                    SlotSchema::new(
                        StandardSchema::Text(TextSchema::Exact("ten".to_string())),
                        StandardSchema::Equal(Value::Int32Value(10)),
                    ),
                    false,
                    false,
                ),
                FieldSpec::new(
                    SlotSchema::new(
                        StandardSchema::Text(TextSchema::Exact("twenty".to_string())),
                        StandardSchema::Equal(Value::Int32Value(20)),
                    ),
                    false,
                    false,
                ),
            ],
            exhaustive: true,
        },
        StandardSchema::HasSlots {
            slots: vec![
                FieldSpec::new(
                    SlotSchema::new(
                        StandardSchema::Text(TextSchema::Exact("twenty".to_string())),
                        StandardSchema::Equal(Value::Int32Value(20)),
                    ),
                    false,
                    false,
                ),
                FieldSpec::new(
                    SlotSchema::new(
                        StandardSchema::Text(TextSchema::Exact("ten".to_string())),
                        StandardSchema::Equal(Value::Int32Value(10)),
                    ),
                    false,
                    false,
                ),
            ],
            exhaustive: true,
        },
    ];

    let greater_schemas = vec![StandardSchema::HasSlots {
        slots: vec![
            FieldSpec::new(
                SlotSchema::new(
                    StandardSchema::Text(TextSchema::Exact("twenty".to_string())),
                    StandardSchema::Equal(Value::Int32Value(20)),
                ),
                false,
                false,
            ),
            FieldSpec::new(
                SlotSchema::new(
                    StandardSchema::Text(TextSchema::Exact("ten".to_string())),
                    StandardSchema::Equal(Value::Int32Value(10)),
                ),
                false,
                false,
            ),
            FieldSpec::new(
                SlotSchema::new(
                    StandardSchema::Text(TextSchema::Exact("thirty".to_string())),
                    StandardSchema::Equal(Value::Int32Value(30)),
                ),
                false,
                false,
            ),
        ],
        exhaustive: true,
    }];

    let lesser_schemas = vec![
        StandardSchema::HasSlots {
            slots: vec![FieldSpec::new(
                SlotSchema::new(
                    StandardSchema::Text(TextSchema::Exact("twenty".to_string())),
                    StandardSchema::Equal(Value::Int32Value(20)),
                ),
                false,
                false,
            )],
            exhaustive: true,
        },
        StandardSchema::HasSlots {
            slots: vec![FieldSpec::new(
                SlotSchema::new(
                    StandardSchema::Text(TextSchema::Exact("ten".to_string())),
                    StandardSchema::Equal(Value::Int32Value(10)),
                ),
                false,
                false,
            )],
            exhaustive: true,
        },
    ];

    let not_related_schemas = vec![
        StandardSchema::HasSlots {
            slots: vec![FieldSpec::new(
                SlotSchema::new(
                    StandardSchema::Text(TextSchema::Exact("fifty".to_string())),
                    StandardSchema::Equal(Value::Int32Value(50)),
                ),
                false,
                false,
            )],
            exhaustive: true,
        },
        StandardSchema::HasSlots {
            slots: vec![
                FieldSpec::new(
                    SlotSchema::new(
                        StandardSchema::Text(TextSchema::Exact("twenty".to_string())),
                        StandardSchema::Equal(Value::Int32Value(20)),
                    ),
                    false,
                    false,
                ),
                FieldSpec::new(
                    SlotSchema::new(
                        StandardSchema::Text(TextSchema::Exact("fifty".to_string())),
                        StandardSchema::Equal(Value::Int32Value(50)),
                    ),
                    false,
                    false,
                ),
                FieldSpec::new(
                    SlotSchema::new(
                        StandardSchema::Text(TextSchema::Exact("thirty".to_string())),
                        StandardSchema::Equal(Value::Int32Value(30)),
                    ),
                    false,
                    false,
                ),
            ],
            exhaustive: true,
        },
        StandardSchema::HasSlots {
            slots: vec![
                FieldSpec::new(
                    SlotSchema::new(
                        StandardSchema::Text(TextSchema::Exact("twenty".to_string())),
                        StandardSchema::Equal(Value::Int32Value(20)),
                    ),
                    false,
                    false,
                ),
                FieldSpec::new(
                    SlotSchema::new(
                        StandardSchema::Text(TextSchema::Exact("ten".to_string())),
                        StandardSchema::Equal(Value::Int32Value(10)),
                    ),
                    false,
                    false,
                ),
            ],
            exhaustive: false,
        },
        StandardSchema::HasSlots {
            slots: vec![
                FieldSpec::new(
                    SlotSchema::new(
                        StandardSchema::Text(TextSchema::Exact("twenty".to_string())),
                        StandardSchema::Equal(Value::Int32Value(20)),
                    ),
                    false,
                    false,
                ),
                FieldSpec::new(
                    SlotSchema::new(
                        StandardSchema::Text(TextSchema::Exact("fifty".to_string())),
                        StandardSchema::Equal(Value::Int32Value(50)),
                    ),
                    false,
                    false,
                ),
            ],
            exhaustive: true,
        },
        StandardSchema::HasSlots {
            slots: vec![
                FieldSpec::new(
                    SlotSchema::new(
                        StandardSchema::Text(TextSchema::Exact("ten".to_string())),
                        StandardSchema::Equal(Value::Int32Value(10)),
                    ),
                    false,
                    false,
                ),
                FieldSpec::new(
                    SlotSchema::new(
                        StandardSchema::Text(TextSchema::Exact("twenty".to_string())),
                        StandardSchema::Equal(Value::Int32Value(20)),
                    ),
                    false,
                    false,
                ),
                FieldSpec::new(
                    SlotSchema::new(
                        StandardSchema::Text(TextSchema::Exact("fifty".to_string())),
                        StandardSchema::Equal(Value::Int32Value(50)),
                    ),
                    true,
                    false,
                ),
            ],
            exhaustive: true,
        },
    ];

    assert_equal(schema.clone(), equal_schemas);
    assert_less_than(schema.clone(), greater_schemas);
    assert_greater_than(schema.clone(), lesser_schemas);
    assert_not_related(schema.clone(), not_related_schemas);
}

#[test]
fn compare_has_slots_has_slots_non_exhaustive() {
    let schema = StandardSchema::HasSlots {
        slots: vec![
            FieldSpec::new(
                SlotSchema::new(
                    StandardSchema::Text(TextSchema::Exact("ten".to_string())),
                    StandardSchema::Equal(Value::Int32Value(10)),
                ),
                false,
                false,
            ),
            FieldSpec::new(
                SlotSchema::new(
                    StandardSchema::Text(TextSchema::Exact("twenty".to_string())),
                    StandardSchema::Equal(Value::Int32Value(20)),
                ),
                false,
                false,
            ),
        ],
        exhaustive: false,
    };

    let equal_schemas = vec![StandardSchema::HasSlots {
        slots: vec![
            FieldSpec::new(
                SlotSchema::new(
                    StandardSchema::Text(TextSchema::Exact("ten".to_string())),
                    StandardSchema::Equal(Value::Int32Value(10)),
                ),
                false,
                false,
            ),
            FieldSpec::new(
                SlotSchema::new(
                    StandardSchema::Text(TextSchema::Exact("twenty".to_string())),
                    StandardSchema::Equal(Value::Int32Value(20)),
                ),
                false,
                false,
            ),
        ],
        exhaustive: false,
    }];

    let greater_schemas = vec![
        StandardSchema::HasSlots {
            slots: vec![FieldSpec::new(
                SlotSchema::new(
                    StandardSchema::Text(TextSchema::Exact("ten".to_string())),
                    StandardSchema::Equal(Value::Int32Value(10)),
                ),
                false,
                false,
            )],
            exhaustive: false,
        },
        StandardSchema::HasSlots {
            slots: vec![FieldSpec::new(
                SlotSchema::new(
                    StandardSchema::Text(TextSchema::Exact("twenty".to_string())),
                    StandardSchema::Equal(Value::Int32Value(20)),
                ),
                false,
                false,
            )],
            exhaustive: false,
        },
    ];

    let lesser_schemas = vec![StandardSchema::HasSlots {
        slots: vec![
            FieldSpec::new(
                SlotSchema::new(
                    StandardSchema::Text(TextSchema::Exact("ten".to_string())),
                    StandardSchema::Equal(Value::Int32Value(10)),
                ),
                false,
                false,
            ),
            FieldSpec::new(
                SlotSchema::new(
                    StandardSchema::Text(TextSchema::Exact("twenty".to_string())),
                    StandardSchema::Equal(Value::Int32Value(20)),
                ),
                false,
                false,
            ),
            FieldSpec::new(
                SlotSchema::new(
                    StandardSchema::Text(TextSchema::Exact("thirty".to_string())),
                    StandardSchema::Equal(Value::Int32Value(30)),
                ),
                false,
                false,
            ),
        ],
        exhaustive: false,
    }];

    let not_related_schemas = vec![
        StandardSchema::HasSlots {
            slots: vec![FieldSpec::new(
                SlotSchema::new(
                    StandardSchema::Text(TextSchema::Exact("fifty".to_string())),
                    StandardSchema::Equal(Value::Int32Value(50)),
                ),
                false,
                false,
            )],
            exhaustive: false,
        },
        StandardSchema::HasSlots {
            slots: vec![
                FieldSpec::new(
                    SlotSchema::new(
                        StandardSchema::Text(TextSchema::Exact("twenty".to_string())),
                        StandardSchema::Equal(Value::Int32Value(20)),
                    ),
                    false,
                    false,
                ),
                FieldSpec::new(
                    SlotSchema::new(
                        StandardSchema::Text(TextSchema::Exact("fifty".to_string())),
                        StandardSchema::Equal(Value::Int32Value(50)),
                    ),
                    false,
                    false,
                ),
                FieldSpec::new(
                    SlotSchema::new(
                        StandardSchema::Text(TextSchema::Exact("thirty".to_string())),
                        StandardSchema::Equal(Value::Int32Value(30)),
                    ),
                    false,
                    false,
                ),
            ],
            exhaustive: false,
        },
        StandardSchema::HasSlots {
            slots: vec![
                FieldSpec::new(
                    SlotSchema::new(
                        StandardSchema::Text(TextSchema::Exact("ten".to_string())),
                        StandardSchema::Equal(Value::Int32Value(10)),
                    ),
                    false,
                    false,
                ),
                FieldSpec::new(
                    SlotSchema::new(
                        StandardSchema::Text(TextSchema::Exact("twenty".to_string())),
                        StandardSchema::Equal(Value::Int32Value(20)),
                    ),
                    false,
                    false,
                ),
            ],
            exhaustive: true,
        },
    ];

    assert_equal(schema.clone(), equal_schemas);
    assert_less_than(schema.clone(), greater_schemas);
    assert_greater_than(schema.clone(), lesser_schemas);
    assert_not_related(schema.clone(), not_related_schemas);
}

#[test]
fn compare_has_slots_layout() {
    let schema = StandardSchema::HasSlots {
        slots: vec![
            FieldSpec::new(
                SlotSchema::new(
                    StandardSchema::Text(TextSchema::Exact("ten".to_string())),
                    StandardSchema::Equal(Value::Int32Value(10)),
                ),
                false,
                false,
            ),
            FieldSpec::new(
                SlotSchema::new(
                    StandardSchema::Text(TextSchema::Exact("twenty".to_string())),
                    StandardSchema::Equal(Value::Int32Value(20)),
                ),
                false,
                false,
            ),
        ],
        exhaustive: true,
    };

    let lesser_schemas = vec![
        StandardSchema::Layout {
            items: vec![
                (
                    ItemSchema::Field(SlotSchema::new(
                        StandardSchema::Text(TextSchema::Exact("ten".to_string())),
                        StandardSchema::Equal(Value::Int32Value(10)),
                    )),
                    false,
                ),
                (
                    ItemSchema::Field(SlotSchema::new(
                        StandardSchema::Text(TextSchema::Exact("twenty".to_string())),
                        StandardSchema::Equal(Value::Int32Value(20)),
                    )),
                    false,
                ),
            ],
            exhaustive: true,
        },
        StandardSchema::Layout {
            items: vec![
                (
                    ItemSchema::Field(SlotSchema::new(
                        StandardSchema::Text(TextSchema::Exact("twenty".to_string())),
                        StandardSchema::Equal(Value::Int32Value(20)),
                    )),
                    false,
                ),
                (
                    ItemSchema::Field(SlotSchema::new(
                        StandardSchema::Text(TextSchema::Exact("ten".to_string())),
                        StandardSchema::Equal(Value::Int32Value(10)),
                    )),
                    false,
                ),
            ],
            exhaustive: true,
        },
        StandardSchema::Layout {
            items: vec![(
                ItemSchema::Field(SlotSchema::new(
                    StandardSchema::Text(TextSchema::Exact("twenty".to_string())),
                    StandardSchema::Equal(Value::Int32Value(20)),
                )),
                false,
            )],
            exhaustive: true,
        },
    ];

    let not_related_schemas = vec![
        StandardSchema::Layout {
            items: vec![
                (
                    ItemSchema::Field(SlotSchema::new(
                        StandardSchema::Text(TextSchema::Exact("twenty".to_string())),
                        StandardSchema::Equal(Value::Int32Value(20)),
                    )),
                    false,
                ),
                (
                    ItemSchema::Field(SlotSchema::new(
                        StandardSchema::Text(TextSchema::Exact("ten".to_string())),
                        StandardSchema::Equal(Value::Int32Value(10)),
                    )),
                    false,
                ),
                (
                    ItemSchema::Field(SlotSchema::new(
                        StandardSchema::Text(TextSchema::Exact("fifty".to_string())),
                        StandardSchema::Equal(Value::Int32Value(50)),
                    )),
                    false,
                ),
            ],
            exhaustive: true,
        },
        StandardSchema::Layout {
            items: vec![(
                ItemSchema::Field(SlotSchema::new(
                    StandardSchema::Text(TextSchema::Exact("thirty".to_string())),
                    StandardSchema::Equal(Value::Int32Value(30)),
                )),
                false,
            )],
            exhaustive: true,
        },
        StandardSchema::Layout {
            items: vec![(
                ItemSchema::Field(SlotSchema::new(
                    StandardSchema::Text(TextSchema::Exact("twenty".to_string())),
                    StandardSchema::Equal(Value::Int32Value(20)),
                )),
                false,
            )],
            exhaustive: false,
        },
    ];

    assert_greater_than(schema.clone(), lesser_schemas);
    assert_not_related(schema.clone(), not_related_schemas);
}

#[test]
fn compare_layout_layout_exhaustive() {
    let schema = StandardSchema::Layout {
        items: vec![
            (
                ItemSchema::Field(SlotSchema::new(
                    StandardSchema::Text(TextSchema::Exact("ten".to_string())),
                    StandardSchema::Equal(Value::Int32Value(10)),
                )),
                false,
            ),
            (
                ItemSchema::Field(SlotSchema::new(
                    StandardSchema::Text(TextSchema::Exact("twenty".to_string())),
                    StandardSchema::Equal(Value::Int32Value(20)),
                )),
                false,
            ),
        ],
        exhaustive: true,
    };

    let equal_schemas = vec![StandardSchema::Layout {
        items: vec![
            (
                ItemSchema::Field(SlotSchema::new(
                    StandardSchema::Text(TextSchema::Exact("ten".to_string())),
                    StandardSchema::Equal(Value::Int32Value(10)),
                )),
                false,
            ),
            (
                ItemSchema::Field(SlotSchema::new(
                    StandardSchema::Text(TextSchema::Exact("twenty".to_string())),
                    StandardSchema::Equal(Value::Int32Value(20)),
                )),
                false,
            ),
        ],
        exhaustive: true,
    }];

    let greater_schemas = vec![StandardSchema::Layout {
        items: vec![
            (
                ItemSchema::Field(SlotSchema::new(
                    StandardSchema::Text(TextSchema::Exact("ten".to_string())),
                    StandardSchema::Equal(Value::Int32Value(10)),
                )),
                false,
            ),
            (
                ItemSchema::Field(SlotSchema::new(
                    StandardSchema::Text(TextSchema::Exact("twenty".to_string())),
                    StandardSchema::Equal(Value::Int32Value(20)),
                )),
                false,
            ),
            (
                ItemSchema::Field(SlotSchema::new(
                    StandardSchema::Text(TextSchema::Exact("thirty".to_string())),
                    StandardSchema::Equal(Value::Int32Value(30)),
                )),
                false,
            ),
        ],
        exhaustive: true,
    }];

    let lesser_schemas = vec![StandardSchema::Layout {
        items: vec![(
            ItemSchema::Field(SlotSchema::new(
                StandardSchema::Text(TextSchema::Exact("ten".to_string())),
                StandardSchema::Equal(Value::Int32Value(10)),
            )),
            false,
        )],
        exhaustive: true,
    }];

    let not_related_schemas = vec![
        StandardSchema::Layout {
            items: vec![(
                ItemSchema::Field(SlotSchema::new(
                    StandardSchema::Text(TextSchema::Exact("fifty".to_string())),
                    StandardSchema::Equal(Value::Int32Value(50)),
                )),
                false,
            )],
            exhaustive: true,
        },
        StandardSchema::Layout {
            items: vec![
                (
                    ItemSchema::Field(SlotSchema::new(
                        StandardSchema::Text(TextSchema::Exact("ten".to_string())),
                        StandardSchema::Equal(Value::Int32Value(10)),
                    )),
                    false,
                ),
                (
                    ItemSchema::Field(SlotSchema::new(
                        StandardSchema::Text(TextSchema::Exact("fifty".to_string())),
                        StandardSchema::Equal(Value::Int32Value(50)),
                    )),
                    false,
                ),
            ],
            exhaustive: true,
        },
        StandardSchema::Layout {
            items: vec![
                (
                    ItemSchema::Field(SlotSchema::new(
                        StandardSchema::Text(TextSchema::Exact("twenty".to_string())),
                        StandardSchema::Equal(Value::Int32Value(20)),
                    )),
                    false,
                ),
                (
                    ItemSchema::Field(SlotSchema::new(
                        StandardSchema::Text(TextSchema::Exact("ten".to_string())),
                        StandardSchema::Equal(Value::Int32Value(10)),
                    )),
                    false,
                ),
            ],
            exhaustive: true,
        },
        StandardSchema::Layout {
            items: vec![
                (
                    ItemSchema::Field(SlotSchema::new(
                        StandardSchema::Text(TextSchema::Exact("ten".to_string())),
                        StandardSchema::Equal(Value::Int32Value(10)),
                    )),
                    false,
                ),
                (
                    ItemSchema::Field(SlotSchema::new(
                        StandardSchema::Text(TextSchema::Exact("twenty".to_string())),
                        StandardSchema::Equal(Value::Int32Value(20)),
                    )),
                    false,
                ),
                (
                    ItemSchema::Field(SlotSchema::new(
                        StandardSchema::Text(TextSchema::Exact("thirty".to_string())),
                        StandardSchema::Equal(Value::Int32Value(30)),
                    )),
                    true,
                ),
            ],
            exhaustive: true,
        },
    ];

    assert_equal(schema.clone(), equal_schemas);
    assert_less_than(schema.clone(), greater_schemas);
    assert_greater_than(schema.clone(), lesser_schemas);
    assert_not_related(schema.clone(), not_related_schemas);
}

#[test]
fn compare_layout_layout_non_exhaustive() {
    let schema = StandardSchema::Layout {
        items: vec![
            (
                ItemSchema::Field(SlotSchema::new(
                    StandardSchema::Text(TextSchema::Exact("ten".to_string())),
                    StandardSchema::Equal(Value::Int32Value(10)),
                )),
                false,
            ),
            (
                ItemSchema::Field(SlotSchema::new(
                    StandardSchema::Text(TextSchema::Exact("twenty".to_string())),
                    StandardSchema::Equal(Value::Int32Value(20)),
                )),
                false,
            ),
        ],
        exhaustive: false,
    };

    let equal_schemas = vec![StandardSchema::Layout {
        items: vec![
            (
                ItemSchema::Field(SlotSchema::new(
                    StandardSchema::Text(TextSchema::Exact("ten".to_string())),
                    StandardSchema::Equal(Value::Int32Value(10)),
                )),
                false,
            ),
            (
                ItemSchema::Field(SlotSchema::new(
                    StandardSchema::Text(TextSchema::Exact("twenty".to_string())),
                    StandardSchema::Equal(Value::Int32Value(20)),
                )),
                false,
            ),
        ],
        exhaustive: false,
    }];

    let greater_schemas = vec![StandardSchema::Layout {
        items: vec![(
            ItemSchema::Field(SlotSchema::new(
                StandardSchema::Text(TextSchema::Exact("ten".to_string())),
                StandardSchema::Equal(Value::Int32Value(10)),
            )),
            false,
        )],
        exhaustive: false,
    }];

    let lesser_schemas = vec![
        StandardSchema::Layout {
            items: vec![
                (
                    ItemSchema::Field(SlotSchema::new(
                        StandardSchema::Text(TextSchema::Exact("ten".to_string())),
                        StandardSchema::Equal(Value::Int32Value(10)),
                    )),
                    false,
                ),
                (
                    ItemSchema::Field(SlotSchema::new(
                        StandardSchema::Text(TextSchema::Exact("twenty".to_string())),
                        StandardSchema::Equal(Value::Int32Value(20)),
                    )),
                    false,
                ),
                (
                    ItemSchema::Field(SlotSchema::new(
                        StandardSchema::Text(TextSchema::Exact("thirty".to_string())),
                        StandardSchema::Equal(Value::Int32Value(30)),
                    )),
                    false,
                ),
            ],
            exhaustive: false,
        },
        StandardSchema::Layout {
            items: vec![
                (
                    ItemSchema::Field(SlotSchema::new(
                        StandardSchema::Text(TextSchema::Exact("ten".to_string())),
                        StandardSchema::Equal(Value::Int32Value(10)),
                    )),
                    false,
                ),
                (
                    ItemSchema::Field(SlotSchema::new(
                        StandardSchema::Text(TextSchema::Exact("twenty".to_string())),
                        StandardSchema::Equal(Value::Int32Value(20)),
                    )),
                    false,
                ),
                (
                    ItemSchema::Field(SlotSchema::new(
                        StandardSchema::Text(TextSchema::Exact("thirty".to_string())),
                        StandardSchema::Equal(Value::Int32Value(30)),
                    )),
                    true,
                ),
            ],
            exhaustive: false,
        },
    ];

    let not_related_schemas = vec![
        StandardSchema::Layout {
            items: vec![(
                ItemSchema::Field(SlotSchema::new(
                    StandardSchema::Text(TextSchema::Exact("fifty".to_string())),
                    StandardSchema::Equal(Value::Int32Value(50)),
                )),
                false,
            )],
            exhaustive: false,
        },
        StandardSchema::Layout {
            items: vec![
                (
                    ItemSchema::Field(SlotSchema::new(
                        StandardSchema::Text(TextSchema::Exact("twenty".to_string())),
                        StandardSchema::Equal(Value::Int32Value(20)),
                    )),
                    false,
                ),
                (
                    ItemSchema::Field(SlotSchema::new(
                        StandardSchema::Text(TextSchema::Exact("fifty".to_string())),
                        StandardSchema::Equal(Value::Int32Value(50)),
                    )),
                    false,
                ),
                (
                    ItemSchema::Field(SlotSchema::new(
                        StandardSchema::Text(TextSchema::Exact("thirty".to_string())),
                        StandardSchema::Equal(Value::Int32Value(30)),
                    )),
                    false,
                ),
            ],
            exhaustive: false,
        },
        StandardSchema::Layout {
            items: vec![
                (
                    ItemSchema::Field(SlotSchema::new(
                        StandardSchema::Text(TextSchema::Exact("ten".to_string())),
                        StandardSchema::Equal(Value::Int32Value(10)),
                    )),
                    false,
                ),
                (
                    ItemSchema::Field(SlotSchema::new(
                        StandardSchema::Text(TextSchema::Exact("twenty".to_string())),
                        StandardSchema::Equal(Value::Int32Value(20)),
                    )),
                    false,
                ),
            ],
            exhaustive: true,
        },
    ];

    assert_equal(schema.clone(), equal_schemas);
    assert_less_than(schema.clone(), greater_schemas);
    assert_greater_than(schema.clone(), lesser_schemas);
    assert_not_related(schema.clone(), not_related_schemas);
}

#[test]
fn schema() {
    let encoded = base64::encode_config("swimming", base64::URL_SAFE);
    let schema = StandardSchema::binary_length(encoded.len());
    let blob = Blob::from_encoded(Vec::from(encoded.as_bytes()));

    assert!(schema.matches(&Value::Data(blob)));
}
