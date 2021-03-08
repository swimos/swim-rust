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

use super::*;
use std::collections::hash_map::DefaultHasher;

mod coercion;

#[test]
fn extant_to_string() {
    assert_eq!(Value::Extant.to_string(), "");
}

#[test]
fn identifiers_to_string() {
    assert_eq!(Value::text("name").to_string(), "name");
    assert_eq!(Value::text("اسم").to_string(), "اسم");
    assert_eq!(Value::text("name2").to_string(), "name2");
    assert_eq!(Value::text("first_second").to_string(), "first_second");
    assert_eq!(Value::text("_name").to_string(), "_name");
}

#[test]
fn text_to_string() {
    assert_eq!(Value::text("").to_string(), r#""""#);
    assert_eq!(Value::text("2name").to_string(), r#""2name""#);
    assert_eq!(Value::text("true").to_string(), r#""true""#);
    assert_eq!(Value::text("false").to_string(), r#""false""#);
    assert_eq!(Value::text("two words").to_string(), r#""two words""#);
    assert_eq!(Value::text("£%^$&*").to_string(), r#""£%^$&*""#);
    assert_eq!(Value::text("\r\n\t").to_string(), r#""\r\n\t""#);
    assert_eq!(Value::text("\"\\\"").to_string(), r#""\"\\\"""#);
    assert_eq!(Value::text("\u{b}").to_string(), r#""\u000b""#);
    assert_eq!(Value::text("\u{c}").to_string(), r#""\f""#);
    assert_eq!(Value::text("\u{8}").to_string(), r#""\b""#);
}

#[test]
fn int32_value_to_string() {
    assert_eq!(Value::Int32Value(0).to_string(), "0");
    assert_eq!(Value::Int32Value(34).to_string(), "34");
    assert_eq!(Value::Int32Value(-56).to_string(), "-56");
}

#[test]
fn int64_value_to_string() {
    assert_eq!(Value::Int64Value(0).to_string(), "0");
    assert_eq!(Value::Int64Value(34).to_string(), "34");
    assert_eq!(Value::Int64Value(-56).to_string(), "-56");
    assert_eq!(
        Value::Int64Value(12_456_765_984i64).to_string(),
        "12456765984"
    );
    assert_eq!(
        Value::Int64Value(-12_456_765_984i64).to_string(),
        "-12456765984"
    );
}

#[test]
fn uint32_value_to_string() {
    assert_eq!(Value::UInt32Value(0).to_string(), "0");
    assert_eq!(Value::UInt32Value(34).to_string(), "34");
    assert_eq!(Value::UInt32Value(5000).to_string(), "5000");
}

#[test]
fn uint64_value_to_string() {
    assert_eq!(Value::UInt64Value(0).to_string(), "0");
    assert_eq!(Value::UInt64Value(34).to_string(), "34");
    assert_eq!(
        Value::UInt64Value(u64::max_value()).to_string(),
        u64::max_value().to_string()
    );
    assert_eq!(
        Value::UInt64Value(12_456_765_984u64).to_string(),
        "12456765984"
    );
}

#[test]
fn boolean_value_to_string() {
    assert_eq!(Value::BooleanValue(true).to_string(), "true");
    assert_eq!(Value::BooleanValue(false).to_string(), "false");
}

#[test]
fn float64_value_to_string() {
    assert_eq!(Value::Float64Value(0.0).to_string(), "0e0");
    assert_eq!(Value::Float64Value(0.5).to_string(), "5e-1");
    assert_eq!(Value::Float64Value(3.56e45).to_string(), "3.56e45");
    assert_eq!(Value::Float64Value(-3.56e45).to_string(), "-3.56e45");
}

#[test]
fn attribute_to_string() {
    assert_eq!(Attr::of("name").to_string(), "@name");
    assert_eq!(Attr::of("two words").to_string(), r#"@"two words""#);
    assert_eq!(Attr::of(("name", 1)).to_string(), "@name(1)");
}

#[test]
fn item_to_string() {
    assert_eq!(Item::of(0).to_string(), "0");
    let slot1: Item = ("name", 7).into();
    assert_eq!(slot1.to_string(), "name:7");
    let slot2: Item = (-5, "two words").into();
    assert_eq!(slot2.to_string(), r#"-5:"two words""#);
    let slot3 = Item::Slot("empty".into(), Value::Extant);
    assert_eq!(slot3.to_string(), "empty:");
}

#[test]
fn no_attr_record_to_string() {
    assert_eq!(Value::empty_record().to_string(), "{}");
    assert_eq!(Value::singleton(0).to_string(), "{0}");
    assert_eq!(Value::singleton(Item::slot("a", 2)).to_string(), "{a:2}");
    assert_eq!(Value::from_vec(vec![1, 2, 3]).to_string(), "{1,2,3}");
    assert_eq!(Value::from_vec(vec!["a", "b", "c"]).to_string(), "{a,b,c}");
    assert_eq!(
        Value::record(vec![("a", 1).into(), 2.into(), ("c", 3).into()]).to_string(),
        "{a:1,2,c:3}"
    );
}

#[test]
fn with_attr_record_to_string() {
    let rec1 = Value::of_attr(Attr::of("name"));
    assert_eq!(rec1.to_string(), "@name");
    let rec2 = Value::of_attrs(vec![Attr::of("name1"), Attr::of("name2")]);
    assert_eq!(rec2.to_string(), "@name1@name2");
    let rec3 = Value::Record(vec![Attr::of("name")], vec![Item::of(3)]);
    assert_eq!(rec3.to_string(), "@name{3}");
    let rec4 = Value::Record(vec![Attr::of("name")], vec![(true, -1).into()]);
    assert_eq!(rec4.to_string(), "@name{true:-1}");
    let rec5 = Value::Record(vec![("name", 1).into()], vec![("a", 1).into(), 7.into()]);
    assert_eq!(rec5.to_string(), "@name(1){a:1,7}");
}

#[test]
fn attrs_with_record_bodies_to_string() {
    let attr1: Attr = ("name", Value::empty_record()).into();
    assert_eq!(attr1.to_string(), "@name({})");
    let attr2: Attr = ("name", Value::singleton(0)).into();
    assert_eq!(attr2.to_string(), "@name({0})");
    let attr3: Attr = ("name", Value::singleton(("a", 1))).into();
    assert_eq!(attr3.to_string(), "@name(a:1)");
    let attr4: Attr = (
        "name",
        Value::record(vec![("a", 1).into(), ("b", 2).into()]),
    )
        .into();
    assert_eq!(attr4.to_string(), "@name(a:1,b:2)");
}

#[test]
fn nested_records_to_string() {
    let double_empty = Value::from_vec(vec![Value::empty_record()]);
    assert_eq!(double_empty.to_string(), "{{}}");
    let inner = Value::from_vec(vec!["a", "b", "c"]);
    let nested1 = Value::from_vec(vec![inner.clone()]);
    assert_eq!(nested1.to_string(), "{{a,b,c}}");
    let nested2 = Value::record(vec![
        ("aa", 10).into(),
        inner.clone().into(),
        ("zz", 99).into(),
    ]);
    assert_eq!(nested2.to_string(), "{aa:10,{a,b,c},zz:99}");
    let attr_inner = Value::Record(vec![("name", 1).into()], vec![]);
    let nested3 = Value::from_vec(vec![attr_inner.clone()]);
    assert_eq!(nested3.to_string(), "{@name(1)}");
    let complex_inner = Value::Record(vec![("inner", 1).into()], vec![("a", 1).into(), 7.into()]);
    let nested_attr: Attr = ("outer", complex_inner.clone()).into();
    assert_eq!(nested_attr.to_string(), "@outer(@inner(1){a:1,7})");
}

#[test]
fn kind_cmp() {
    assert_eq!(ValueKind::Int32, ValueKind::Int32);
    assert!(ValueKind::Int32 < ValueKind::Int64);
    assert!(ValueKind::Int64 > ValueKind::Int32);
    assert!(ValueKind::Int32 < ValueKind::BigInt);
    assert!(ValueKind::BigInt > ValueKind::Int32);

    assert_eq!(ValueKind::Int64, ValueKind::Int64);
    assert!(ValueKind::Int64 > ValueKind::UInt32);
    assert!(ValueKind::UInt32 < ValueKind::Int64);
    assert!(ValueKind::Int64 < ValueKind::BigInt);
    assert!(ValueKind::BigInt > ValueKind::Int64);

    assert_eq!(ValueKind::UInt32, ValueKind::UInt32);
    assert!(ValueKind::UInt32 < ValueKind::UInt64);
    assert!(ValueKind::UInt64 > ValueKind::UInt32);
    assert!(ValueKind::UInt32 < ValueKind::BigInt);
    assert!(ValueKind::BigInt > ValueKind::UInt32);
    assert!(ValueKind::UInt32 < ValueKind::BigUint);
    assert!(ValueKind::BigUint > ValueKind::UInt32);

    assert_eq!(ValueKind::UInt64, ValueKind::UInt64);
    assert!(ValueKind::UInt64 < ValueKind::BigInt);
    assert!(ValueKind::BigInt > ValueKind::UInt64);
    assert!(ValueKind::UInt64 < ValueKind::BigUint);
    assert!(ValueKind::BigUint > ValueKind::UInt64);

    assert_eq!(ValueKind::BigInt, ValueKind::BigInt);
    assert!(ValueKind::BigInt > ValueKind::BigUint);
    assert!(ValueKind::BigUint < ValueKind::BigInt);

    assert_eq!(ValueKind::BigUint, ValueKind::BigUint);
}

#[test]
fn int32_cmp() {
    assert!(Value::Int32Value(50) > Value::Int32Value(49));
    assert!(Value::Int32Value(10) < Value::Int32Value(15));
    assert_eq!(
        Value::Int32Value(13).cmp(&Value::Int32Value(13)),
        Ordering::Equal
    );

    assert!(Value::Int32Value(50) > Value::Int64Value(49));
    assert!(Value::Int32Value(10) < Value::Int64Value(15));
    assert_eq!(
        Value::Int32Value(13).cmp(&Value::Int64Value(13)),
        Ordering::Equal
    );

    assert!(Value::Int32Value(50) > Value::UInt32Value(49));
    assert!(Value::Int32Value(-5) < Value::UInt32Value(15));
    assert_eq!(
        Value::Int32Value(13).cmp(&Value::UInt32Value(13)),
        Ordering::Equal
    );

    assert!(Value::Int32Value(50) > Value::UInt64Value(49));
    assert!(Value::Int32Value(-100) < Value::UInt64Value(0));
    assert_eq!(
        Value::Int32Value(13).cmp(&Value::UInt64Value(13)),
        Ordering::Equal
    );

    assert!(Value::Int32Value(50) > Value::Float64Value(49.50));
    assert!(Value::Int32Value(-100) < Value::Float64Value(-10.12));
    assert_eq!(
        Value::Int32Value(13).cmp(&Value::Float64Value(13.0)),
        Ordering::Equal
    );

    assert!(Value::Int32Value(50) > Value::BigInt(BigInt::from(49)));
    assert!(Value::Int32Value(-100) < Value::BigInt(BigInt::from(-10)));
    assert_eq!(
        Value::Int32Value(13).cmp(&Value::BigInt(BigInt::from(13))),
        Ordering::Equal
    );

    assert!(Value::Int32Value(50) > Value::BigUint(BigUint::from(49u32)));
    assert!(Value::Int32Value(-100) < Value::BigUint(BigUint::from(10u32)));
    assert_eq!(
        Value::Int32Value(13).cmp(&Value::BigUint(BigUint::from(13u32))),
        Ordering::Equal
    );
}

#[test]
fn int64_cmp() {
    assert!(Value::Int64Value(50) > Value::Int32Value(49));
    assert!(Value::Int64Value(10) < Value::Int32Value(15));
    assert_eq!(
        Value::Int64Value(13).cmp(&Value::Int32Value(13)),
        Ordering::Equal
    );

    assert!(Value::Int64Value(50) > Value::Int64Value(49));
    assert!(Value::Int64Value(10) < Value::Int64Value(15));
    assert_eq!(
        Value::Int64Value(13).cmp(&Value::Int64Value(13)),
        Ordering::Equal
    );

    assert!(Value::Int64Value(50) > Value::UInt32Value(49));
    assert!(Value::Int64Value(-5) < Value::UInt32Value(15));
    assert_eq!(
        Value::Int64Value(13).cmp(&Value::UInt32Value(13)),
        Ordering::Equal
    );

    assert!(Value::Int64Value(50) > Value::UInt64Value(49));
    assert!(Value::Int64Value(-100) < Value::UInt64Value(0));
    assert_eq!(
        Value::Int64Value(13).cmp(&Value::UInt64Value(13)),
        Ordering::Equal
    );

    assert!(Value::Int64Value(50) > Value::Float64Value(49.50));
    assert!(Value::Int64Value(-100) < Value::Float64Value(-10.12));
    assert_eq!(
        Value::Int64Value(13).cmp(&Value::Float64Value(13.0)),
        Ordering::Equal
    );

    assert!(Value::Int64Value(50) > Value::BigInt(BigInt::from(49)));
    assert!(Value::Int64Value(-100) < Value::BigInt(BigInt::from(-10)));
    assert_eq!(
        Value::Int64Value(13).cmp(&Value::BigInt(BigInt::from(13))),
        Ordering::Equal
    );

    assert!(Value::Int64Value(50) > Value::BigUint(BigUint::from(49u32)));
    assert!(Value::Int64Value(-100) < Value::BigUint(BigUint::from(10u32)));
    assert_eq!(
        Value::Int64Value(13).cmp(&Value::BigUint(BigUint::from(13u32))),
        Ordering::Equal
    );
}

#[test]
fn uint32_cmp() {
    assert!(Value::UInt32Value(50) > Value::Int32Value(-49));
    assert!(Value::UInt32Value(10) < Value::Int32Value(15));
    assert_eq!(
        Value::UInt32Value(13).cmp(&Value::Int32Value(13)),
        Ordering::Equal
    );

    assert!(Value::UInt32Value(50) > Value::Int64Value(-49));
    assert!(Value::UInt32Value(10) < Value::Int64Value(15));
    assert_eq!(
        Value::UInt32Value(13).cmp(&Value::Int64Value(13)),
        Ordering::Equal
    );

    assert!(Value::UInt32Value(50) > Value::UInt32Value(49));
    assert!(Value::UInt32Value(5) < Value::UInt32Value(15));
    assert_eq!(
        Value::UInt32Value(13).cmp(&Value::UInt32Value(13)),
        Ordering::Equal
    );

    assert!(Value::UInt32Value(50) > Value::UInt64Value(49));
    assert!(Value::UInt32Value(0) < Value::UInt64Value(100));
    assert_eq!(
        Value::UInt32Value(13).cmp(&Value::UInt64Value(13)),
        Ordering::Equal
    );

    assert!(Value::UInt32Value(50) > Value::Float64Value(49.50));
    assert!(Value::UInt32Value(0) < Value::Float64Value(10.12));
    assert_eq!(
        Value::UInt32Value(13).cmp(&Value::Float64Value(13.0)),
        Ordering::Equal
    );

    assert!(Value::UInt32Value(50) > Value::BigInt(BigInt::from(-49)));
    assert!(Value::UInt32Value(10) < Value::BigInt(BigInt::from(100)));
    assert_eq!(
        Value::UInt32Value(13).cmp(&Value::BigInt(BigInt::from(13))),
        Ordering::Equal
    );

    assert!(Value::UInt32Value(50) > Value::BigUint(BigUint::from(49u32)));
    assert!(Value::UInt32Value(10) < Value::BigUint(BigUint::from(100u32)));
    assert_eq!(
        Value::UInt32Value(13).cmp(&Value::BigUint(BigUint::from(13u32))),
        Ordering::Equal
    );
}

#[test]
fn uint64_cmp() {
    assert!(Value::UInt64Value(50) > Value::Int32Value(-49));
    assert!(Value::UInt64Value(10) < Value::Int32Value(15));
    assert_eq!(
        Value::UInt64Value(13).cmp(&Value::Int32Value(13)),
        Ordering::Equal
    );

    assert!(Value::UInt64Value(50) > Value::Int64Value(-49));
    assert!(Value::UInt64Value(10) < Value::Int64Value(15));
    assert_eq!(
        Value::UInt64Value(13).cmp(&Value::Int64Value(13)),
        Ordering::Equal
    );

    assert!(Value::UInt64Value(50) > Value::UInt32Value(49));
    assert!(Value::UInt64Value(5) < Value::UInt32Value(15));
    assert_eq!(
        Value::UInt64Value(13).cmp(&Value::UInt32Value(13)),
        Ordering::Equal
    );

    assert!(Value::UInt64Value(50) > Value::UInt64Value(49));
    assert!(Value::UInt64Value(0) < Value::UInt64Value(100));
    assert_eq!(
        Value::UInt64Value(13).cmp(&Value::UInt64Value(13)),
        Ordering::Equal
    );

    assert!(Value::UInt64Value(50) > Value::Float64Value(49.50));
    assert!(Value::UInt64Value(0) < Value::Float64Value(10.12));
    assert_eq!(
        Value::UInt64Value(13).cmp(&Value::Float64Value(13.0)),
        Ordering::Equal
    );

    assert!(Value::UInt64Value(50) > Value::BigInt(BigInt::from(-49)));
    assert!(Value::UInt64Value(10) < Value::BigInt(BigInt::from(100)));
    assert_eq!(
        Value::UInt64Value(13).cmp(&Value::BigInt(BigInt::from(13))),
        Ordering::Equal
    );

    assert!(Value::UInt64Value(50) > Value::BigUint(BigUint::from(49u32)));
    assert!(Value::UInt64Value(10) < Value::BigUint(BigUint::from(100u32)));
    assert_eq!(
        Value::UInt64Value(13).cmp(&Value::BigUint(BigUint::from(13u32))),
        Ordering::Equal
    );
}

#[test]
fn f64_cmp() {
    assert!(Value::Float64Value(50.50) > Value::Int32Value(50));
    assert!(Value::Float64Value(-10.10) < Value::Int32Value(-10));
    assert_eq!(
        Value::Float64Value(13.0).cmp(&Value::Int32Value(13)),
        Ordering::Equal
    );

    assert!(Value::Float64Value(50.50) > Value::Int64Value(50));
    assert!(Value::Float64Value(-10.10) < Value::Int64Value(-10));
    assert_eq!(
        Value::Float64Value(13.0).cmp(&Value::Int64Value(13)),
        Ordering::Equal
    );

    assert!(Value::Float64Value(49.49) > Value::UInt32Value(49));
    assert!(Value::Float64Value(-15.15) < Value::UInt32Value(15));
    assert_eq!(
        Value::Float64Value(13.0).cmp(&Value::UInt32Value(13)),
        Ordering::Equal
    );

    assert!(Value::Float64Value(49.49) > Value::UInt64Value(49));
    assert!(Value::Float64Value(-15.5) < Value::UInt64Value(15));
    assert_eq!(
        Value::Float64Value(13.0).cmp(&Value::UInt64Value(13)),
        Ordering::Equal
    );

    assert!(Value::Float64Value(50.50) > Value::Float64Value(50.40));
    assert!(Value::Float64Value(-10.50) < Value::Float64Value(-10.40));
    assert_eq!(
        Value::Float64Value(13.0).cmp(&Value::Float64Value(13.0)),
        Ordering::Equal
    );

    assert!(Value::Float64Value(50.50) > Value::BigInt(BigInt::from(-49)));
    assert!(Value::Float64Value(10.10) < Value::BigInt(BigInt::from(100)));
    assert_eq!(
        Value::Float64Value(13.0).cmp(&Value::BigInt(BigInt::from(13))),
        Ordering::Equal
    );

    assert!(Value::Float64Value(50.50) > Value::BigUint(BigUint::from(49u32)));
    assert!(Value::Float64Value(-15.15) < Value::BigUint(BigUint::from(15u32)));
    assert_eq!(
        Value::Float64Value(13.0).cmp(&Value::BigUint(BigUint::from(13u32))),
        Ordering::Equal
    );
}

#[test]
fn big_int_cmp() {
    assert!(Value::BigInt(BigInt::from(50)) > Value::Int32Value(49));
    assert!(Value::BigInt(BigInt::from(10)) < Value::Int32Value(15));
    assert_eq!(
        Value::BigInt(BigInt::from(13)).cmp(&Value::Int32Value(13)),
        Ordering::Equal
    );

    assert!(Value::BigInt(BigInt::from(50)) > Value::Int64Value(49));
    assert!(Value::BigInt(BigInt::from(10)) < Value::Int64Value(15));
    assert_eq!(
        Value::BigInt(BigInt::from(13)).cmp(&Value::Int64Value(13)),
        Ordering::Equal
    );

    assert!(Value::BigInt(BigInt::from(50)) > Value::UInt32Value(49));
    assert!(Value::BigInt(BigInt::from(-5)) < Value::UInt32Value(15));
    assert_eq!(
        Value::BigInt(BigInt::from(13)).cmp(&Value::UInt32Value(13)),
        Ordering::Equal
    );

    assert!(Value::BigInt(BigInt::from(50)) > Value::UInt64Value(49));
    assert!(Value::BigInt(BigInt::from(-100)) < Value::UInt64Value(0));
    assert_eq!(
        Value::BigInt(BigInt::from(13)).cmp(&Value::UInt64Value(13)),
        Ordering::Equal
    );

    assert!(Value::BigInt(BigInt::from(50)) > Value::Float64Value(49.50));
    assert!(Value::BigInt(BigInt::from(-100)) < Value::Float64Value(-10.12));
    assert_eq!(
        Value::BigInt(BigInt::from(13)).cmp(&Value::Float64Value(13.0)),
        Ordering::Equal
    );

    assert!(Value::BigInt(BigInt::from(50)) > Value::BigInt(BigInt::from(49)));
    assert!(Value::BigInt(BigInt::from(-100)) < Value::BigInt(BigInt::from(-10)));
    assert_eq!(
        Value::BigInt(BigInt::from(13)).cmp(&Value::BigInt(BigInt::from(13))),
        Ordering::Equal
    );

    assert!(Value::BigInt(BigInt::from(50)) > Value::BigUint(BigUint::from(49u32)));
    assert!(Value::BigInt(BigInt::from(-100)) < Value::BigUint(BigUint::from(10u32)));
    assert_eq!(
        Value::BigInt(BigInt::from(13)).cmp(&Value::BigUint(BigUint::from(13u32))),
        Ordering::Equal
    );
}

#[test]
fn big_uint_cmp() {
    assert!(Value::BigUint(BigUint::from(50u32)) > Value::Int32Value(-49));
    assert!(Value::BigUint(BigUint::from(10u32)) < Value::Int32Value(15));
    assert_eq!(
        Value::BigUint(BigUint::from(13u32)).cmp(&Value::Int32Value(13)),
        Ordering::Equal
    );

    assert!(Value::BigUint(BigUint::from(50u32)) > Value::Int64Value(-49));
    assert!(Value::BigUint(BigUint::from(10u32)) < Value::Int64Value(15));
    assert_eq!(
        Value::BigUint(BigUint::from(13u32)).cmp(&Value::Int64Value(13)),
        Ordering::Equal
    );

    assert!(Value::BigUint(BigUint::from(50u32)) > Value::UInt32Value(49));
    assert!(Value::BigUint(BigUint::from(5u32)) < Value::UInt32Value(15));
    assert_eq!(
        Value::BigUint(BigUint::from(13u32)).cmp(&Value::UInt32Value(13)),
        Ordering::Equal
    );

    assert!(Value::BigUint(BigUint::from(50u32)) > Value::UInt64Value(49));
    assert!(Value::BigUint(BigUint::from(0u32)) < Value::UInt64Value(100));
    assert_eq!(
        Value::BigUint(BigUint::from(13u32)).cmp(&Value::UInt64Value(13)),
        Ordering::Equal
    );

    assert!(Value::BigUint(BigUint::from(50u32)) > Value::Float64Value(49.50));
    assert!(Value::BigUint(BigUint::from(0u32)) < Value::Float64Value(10.12));
    assert_eq!(
        Value::BigUint(BigUint::from(13u32)).cmp(&Value::Float64Value(13.0)),
        Ordering::Equal
    );

    assert!(Value::BigUint(BigUint::from(50u32)) > Value::BigInt(BigInt::from(-49)));
    assert!(Value::BigUint(BigUint::from(10u32)) < Value::BigInt(BigInt::from(100)));
    assert_eq!(
        Value::BigUint(BigUint::from(13u32)).cmp(&Value::BigInt(BigInt::from(13))),
        Ordering::Equal
    );

    assert!(Value::BigUint(BigUint::from(50u32)) > Value::BigUint(BigUint::from(49u32)));
    assert!(Value::BigUint(BigUint::from(10u32)) < Value::BigUint(BigUint::from(100u32)));
    assert_eq!(
        Value::BigUint(BigUint::from(13u32)).cmp(&Value::BigUint(BigUint::from(13u32))),
        Ordering::Equal
    );
}

#[test]
fn int_32_eq() {
    assert_eq!(Value::Int32Value(10), Value::Int32Value(10));
    assert_eq!(Value::Int32Value(10), Value::Int64Value(10));
    assert_eq!(Value::Int32Value(10), Value::UInt32Value(10));
    assert_eq!(Value::Int32Value(10), Value::UInt64Value(10));
    assert_eq!(Value::Int32Value(10), Value::BigInt(BigInt::from(10)));
    assert_eq!(Value::Int32Value(10), Value::BigUint(BigUint::from(10u32)));

    assert_ne!(Value::Int32Value(10), Value::Int32Value(12));
    assert_ne!(
        Value::Int32Value(-2_147_483_648),
        Value::Int64Value(2_147_483_648)
    );
    assert_ne!(Value::Int32Value(-1), Value::UInt32Value(4_294_967_295));
    assert_ne!(
        Value::Int32Value(-2_147_483_648),
        Value::UInt64Value(2_147_483_648)
    );
    assert_ne!(
        Value::Int32Value(-2_147_483_648),
        Value::BigInt(BigInt::from(2_147_483_648i64))
    );
    assert_ne!(
        Value::Int32Value(-2_147_483_648),
        Value::BigUint(BigUint::from(2_147_483_648u32))
    );
}

#[test]
fn int_64_eq() {
    assert_eq!(Value::Int64Value(11), Value::Int32Value(11));
    assert_eq!(Value::Int64Value(11), Value::Int64Value(11));
    assert_eq!(Value::Int64Value(11), Value::UInt32Value(11));
    assert_eq!(Value::Int64Value(11), Value::UInt64Value(11));
    assert_eq!(Value::Int64Value(11), Value::BigInt(BigInt::from(11)));
    assert_eq!(Value::Int64Value(11), Value::BigUint(BigUint::from(11u32)));

    assert_ne!(Value::Int64Value(11), Value::Int32Value(12));
    assert_ne!(Value::Int64Value(11), Value::Int64Value(12));
    assert_ne!(Value::Int64Value(11), Value::UInt32Value(12));
    assert_ne!(
        Value::Int64Value(-9_223_372_036_854_775_808),
        Value::UInt64Value(9_223_372_036_854_775_808)
    );
    assert_ne!(
        Value::Int64Value(-9_223_372_036_854_775_808),
        Value::BigInt(BigInt::from(9_223_372_036_854_775_808i128))
    );
    assert_ne!(
        Value::Int64Value(-9_223_372_036_854_775_808),
        Value::BigUint(BigUint::from(9_223_372_036_854_775_808u64))
    );
}

#[test]
fn uint_32_eq() {
    assert_eq!(Value::UInt32Value(12), Value::Int32Value(12));
    assert_eq!(Value::UInt32Value(12), Value::Int64Value(12));
    assert_eq!(Value::UInt32Value(12), Value::UInt32Value(12));
    assert_eq!(Value::UInt32Value(12), Value::UInt64Value(12));
    assert_eq!(Value::UInt32Value(12), Value::BigInt(BigInt::from(12)));
    assert_eq!(Value::UInt32Value(12), Value::BigUint(BigUint::from(12u32)));

    assert_ne!(Value::UInt32Value(4_294_967_295), Value::Int32Value(-1));
    assert_ne!(Value::UInt32Value(4_294_967_295), Value::Int64Value(-1));
    assert_ne!(Value::UInt32Value(12), Value::UInt32Value(13));
    assert_ne!(Value::UInt32Value(0), Value::UInt64Value(4_294_967_296));
    assert_ne!(
        Value::UInt32Value(0),
        Value::BigInt(BigInt::from(4_294_967_296i64))
    );
    assert_ne!(
        Value::UInt32Value(0),
        Value::BigUint(BigUint::from(4_294_967_296u64))
    );
}

#[test]
fn uint_64_eq() {
    assert_eq!(Value::UInt64Value(13), Value::Int32Value(13));
    assert_eq!(Value::UInt64Value(13), Value::Int64Value(13));
    assert_eq!(Value::UInt64Value(13), Value::UInt32Value(13));
    assert_eq!(Value::UInt64Value(13), Value::UInt64Value(13));
    assert_eq!(Value::UInt64Value(13), Value::BigInt(BigInt::from(13)));
    assert_eq!(Value::UInt64Value(13), Value::BigUint(BigUint::from(13u32)));

    assert_ne!(
        Value::UInt64Value(18_446_744_073_709_551_615),
        Value::Int32Value(-1)
    );
    assert_ne!(
        Value::UInt64Value(18_446_744_073_709_551_615),
        Value::Int64Value(-1)
    );
    assert_ne!(Value::UInt64Value(13), Value::UInt32Value(14));
    assert_ne!(Value::UInt64Value(13), Value::UInt64Value(14));
    assert_ne!(
        Value::UInt64Value(18_446_744_073_709_551_615),
        Value::BigInt(BigInt::from(-1))
    );
    assert_ne!(
        Value::UInt64Value(0),
        Value::BigUint(BigUint::from(18_446_744_073_709_551_616u128))
    );
}

#[test]
fn big_int_eq() {
    assert_eq!(Value::BigInt(BigInt::from(14)), Value::Int32Value(14));
    assert_eq!(Value::BigInt(BigInt::from(14)), Value::Int64Value(14));
    assert_eq!(Value::BigInt(BigInt::from(14)), Value::UInt32Value(14));
    assert_eq!(Value::BigInt(BigInt::from(14)), Value::UInt64Value(14));
    assert_eq!(
        Value::BigInt(BigInt::from(14)),
        Value::BigInt(BigInt::from(14))
    );
    assert_eq!(
        Value::BigInt(BigInt::from(14)),
        Value::BigUint(BigUint::from(14u32))
    );

    assert_ne!(
        Value::BigInt(BigInt::from(2_147_483_648i64)),
        Value::Int32Value(-2_147_483_648)
    );
    assert_ne!(
        Value::BigInt(BigInt::from(9_223_372_036_854_775_808i128)),
        Value::Int64Value(-9_223_372_036_854_775_808)
    );
    assert_ne!(
        Value::BigInt(BigInt::from(4_294_967_296u64)),
        Value::UInt32Value(0)
    );
    assert_ne!(
        Value::BigInt(BigInt::from(18_446_744_073_709_551_616u128)),
        Value::UInt64Value(0)
    );
    assert_ne!(
        Value::BigInt(BigInt::from(14)),
        Value::BigInt(BigInt::from(15))
    );
    assert_ne!(
        Value::BigInt(BigInt::from(-1)),
        Value::BigUint(BigUint::from(0u32))
    );
}

#[test]
fn big_uint_eq() {
    assert_eq!(Value::BigUint(BigUint::from(15u32)), Value::Int32Value(15));
    assert_eq!(Value::BigUint(BigUint::from(15u32)), Value::Int64Value(15));
    assert_eq!(Value::BigUint(BigUint::from(15u32)), Value::UInt32Value(15));
    assert_eq!(Value::BigUint(BigUint::from(15u32)), Value::UInt64Value(15));
    assert_eq!(
        Value::BigUint(BigUint::from(15u32)),
        Value::BigInt(BigInt::from(15))
    );
    assert_eq!(
        Value::BigUint(BigUint::from(15u32)),
        Value::BigUint(BigUint::from(15u32))
    );

    assert_ne!(
        Value::BigUint(BigUint::from(2_147_483_648u64)),
        Value::Int32Value(-2_147_483_648)
    );
    assert_ne!(
        Value::BigUint(BigUint::from(9_223_372_036_854_775_808u128)),
        Value::Int64Value(-9_223_372_036_854_775_808)
    );
    assert_ne!(
        Value::BigUint(BigUint::from(4_294_967_296u64)),
        Value::UInt32Value(0)
    );
    assert_ne!(
        Value::BigUint(BigUint::from(18_446_744_073_709_551_616u128)),
        Value::UInt64Value(0)
    );
    assert_ne!(
        Value::BigUint(BigUint::from(14u32)),
        Value::BigInt(BigInt::from(15u32))
    );
    assert_ne!(
        Value::BigUint(BigUint::from(14u32)),
        Value::BigUint(BigUint::from(15u32))
    );
}

#[test]
fn test_i32_hash() {
    let i32_hash = calculate_hash(&Value::Int32Value(10));

    assert_eq!(i32_hash, calculate_hash(&Value::Int32Value(10)));
    assert_eq!(i32_hash, calculate_hash(&Value::Int64Value(10)));
    assert_eq!(i32_hash, calculate_hash(&Value::UInt32Value(10)));
    assert_eq!(i32_hash, calculate_hash(&Value::UInt64Value(10)));
    assert_eq!(i32_hash, calculate_hash(&Value::BigInt(BigInt::from(10))));
    assert_eq!(
        i32_hash,
        calculate_hash(&Value::BigUint(BigUint::from(10u32)))
    );
}

#[test]
fn test_i64_hash() {
    let i64_hash = calculate_hash(&Value::Int64Value(20));

    assert_eq!(i64_hash, calculate_hash(&Value::Int32Value(20)));
    assert_eq!(i64_hash, calculate_hash(&Value::Int64Value(20)));
    assert_eq!(i64_hash, calculate_hash(&Value::UInt32Value(20)));
    assert_eq!(i64_hash, calculate_hash(&Value::UInt64Value(20)));
    assert_eq!(i64_hash, calculate_hash(&Value::BigInt(BigInt::from(20))));
    assert_eq!(
        i64_hash,
        calculate_hash(&Value::BigUint(BigUint::from(20u32)))
    );
}

#[test]
fn test_u32_hash() {
    let u32_hash = calculate_hash(&Value::UInt32Value(30));

    assert_eq!(u32_hash, calculate_hash(&Value::Int32Value(30)));
    assert_eq!(u32_hash, calculate_hash(&Value::Int64Value(30)));
    assert_eq!(u32_hash, calculate_hash(&Value::UInt32Value(30)));
    assert_eq!(u32_hash, calculate_hash(&Value::UInt64Value(30)));
    assert_eq!(u32_hash, calculate_hash(&Value::BigInt(BigInt::from(30))));
    assert_eq!(
        u32_hash,
        calculate_hash(&Value::BigUint(BigUint::from(30u32)))
    );
}

#[test]
fn test_u64_hash() {
    let u64_hash = calculate_hash(&Value::UInt64Value(40));

    assert_eq!(u64_hash, calculate_hash(&Value::Int32Value(40)));
    assert_eq!(u64_hash, calculate_hash(&Value::Int64Value(40)));
    assert_eq!(u64_hash, calculate_hash(&Value::UInt32Value(40)));
    assert_eq!(u64_hash, calculate_hash(&Value::UInt64Value(40)));
    assert_eq!(u64_hash, calculate_hash(&Value::BigInt(BigInt::from(40))));
    assert_eq!(
        u64_hash,
        calculate_hash(&Value::BigUint(BigUint::from(40u32)))
    );
}

#[test]
fn test_big_int_hash() {
    let big_int_hash = calculate_hash(&Value::BigInt(BigInt::from(50)));

    assert_eq!(big_int_hash, calculate_hash(&Value::Int32Value(50)));
    assert_eq!(big_int_hash, calculate_hash(&Value::Int64Value(50)));
    assert_eq!(big_int_hash, calculate_hash(&Value::UInt32Value(50)));
    assert_eq!(big_int_hash, calculate_hash(&Value::UInt64Value(50)));
    assert_eq!(
        big_int_hash,
        calculate_hash(&Value::BigInt(BigInt::from(50)))
    );
    assert_eq!(
        big_int_hash,
        calculate_hash(&Value::BigUint(BigUint::from(50u32)))
    );

    let big_int_hash = calculate_hash(&Value::BigInt(
        BigInt::from(170_141_183_460_469_231_731_687_303_715_884_105_727i128) + 1,
    ));

    assert_eq!(
        big_int_hash,
        calculate_hash(&Value::BigUint(
            BigUint::try_from(
                BigInt::from(170_141_183_460_469_231_731_687_303_715_884_105_727i128) + 1
            )
            .unwrap()
        ))
    );
}

#[test]
fn test_big_uint_hash() {
    let big_uint_hash = calculate_hash(&Value::BigUint(BigUint::from(60u32)));

    assert_eq!(big_uint_hash, calculate_hash(&Value::Int32Value(60)));
    assert_eq!(big_uint_hash, calculate_hash(&Value::Int64Value(60)));
    assert_eq!(big_uint_hash, calculate_hash(&Value::UInt32Value(60)));
    assert_eq!(big_uint_hash, calculate_hash(&Value::UInt64Value(60)));
    assert_eq!(
        big_uint_hash,
        calculate_hash(&Value::BigInt(BigInt::from(60)))
    );
    assert_eq!(
        big_uint_hash,
        calculate_hash(&Value::BigUint(BigUint::from(60u32)))
    );

    let big_uint_hash = calculate_hash(&Value::BigUint(
        BigUint::try_from(
            BigInt::from(170_141_183_460_469_231_731_687_303_715_884_105_727i128) + 1,
        )
        .unwrap(),
    ));

    assert_eq!(
        big_uint_hash,
        calculate_hash(&Value::BigInt(
            BigInt::from(170_141_183_460_469_231_731_687_303_715_884_105_727i128) + 1,
        ))
    );
}

fn calculate_hash<T: Hash>(t: &T) -> u64 {
    let mut s = DefaultHasher::new();
    t.hash(&mut s);
    s.finish()
}
