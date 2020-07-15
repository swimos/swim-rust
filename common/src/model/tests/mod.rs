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

use hamcrest2::assert_that;
use hamcrest2::prelude::*;

use super::*;
mod coercion;
use num_bigint::RandBigInt;

#[test]
fn extant_to_string() {
    assert_that!(Value::Extant.to_string(), eq(""));
}

#[test]
fn identifiers_to_string() {
    assert_that!(Value::text("name").to_string(), eq("name"));
    assert_that!(Value::text("اسم").to_string(), eq("اسم"));
    assert_that!(Value::text("name2").to_string(), eq("name2"));
    assert_that!(Value::text("first_second").to_string(), eq("first_second"));
    assert_that!(Value::text("_name").to_string(), eq("_name"));
}

#[test]
fn text_to_string() {
    assert_that!(Value::text("").to_string(), eq(r#""""#));
    assert_that!(Value::text("2name").to_string(), eq(r#""2name""#));
    assert_that!(Value::text("true").to_string(), eq(r#""true""#));
    assert_that!(Value::text("false").to_string(), eq(r#""false""#));
    assert_that!(Value::text("two words").to_string(), eq(r#""two words""#));
    assert_that!(Value::text("£%^$&*").to_string(), eq(r#""£%^$&*""#));
    assert_that!(Value::text("\r\n\t").to_string(), eq(r#""\r\n\t""#));
    assert_that!(Value::text("\"\\\"").to_string(), eq(r#""\"\\\"""#));
    assert_that!(Value::text("\u{b}").to_string(), eq(r#""\u000b""#));
    assert_that!(Value::text("\u{c}").to_string(), eq(r#""\f""#));
    assert_that!(Value::text("\u{8}").to_string(), eq(r#""\b""#));
}

#[test]
fn int32_value_to_string() {
    assert_that!(Value::Int32Value(0).to_string(), eq("0"));
    assert_that!(Value::Int32Value(34).to_string(), eq("34"));
    assert_that!(Value::Int32Value(-56).to_string(), eq("-56"));
}

#[test]
fn int64_value_to_string() {
    assert_that!(Value::Int64Value(0).to_string(), eq("0"));
    assert_that!(Value::Int64Value(34).to_string(), eq("34"));
    assert_that!(Value::Int64Value(-56).to_string(), eq("-56"));
    assert_that!(
        Value::Int64Value(12_456_765_984i64).to_string(),
        eq("12456765984")
    );
    assert_that!(
        Value::Int64Value(-12_456_765_984i64).to_string(),
        eq("-12456765984")
    );
}

#[test]
fn uint32_value_to_string() {
    assert_that!(Value::UInt32Value(0).to_string(), eq("0"));
    assert_that!(Value::UInt32Value(34).to_string(), eq("34"));
    assert_that!(Value::UInt32Value(5000).to_string(), eq("5000"));
}

#[test]
fn uint64_value_to_string() {
    assert_that!(Value::UInt64Value(0).to_string(), eq("0"));
    assert_that!(Value::UInt64Value(34).to_string(), eq("34"));
    assert_that!(
        Value::UInt64Value(u64::max_value()).to_string(),
        eq(u64::max_value().to_string())
    );
    assert_that!(
        Value::UInt64Value(12_456_765_984u64).to_string(),
        eq("12456765984")
    );
}

#[test]
fn boolean_value_to_string() {
    assert_that!(Value::BooleanValue(true).to_string(), eq("true"));
    assert_that!(Value::BooleanValue(false).to_string(), eq("false"));
}

#[test]
fn float64_value_to_string() {
    assert_that!(Value::Float64Value(0.0).to_string(), eq("0e0"));
    assert_that!(Value::Float64Value(0.5).to_string(), eq("5e-1"));
    assert_that!(Value::Float64Value(3.56e45).to_string(), eq("3.56e45"));
    assert_that!(Value::Float64Value(-3.56e45).to_string(), eq("-3.56e45"));
}

#[test]
fn attribute_to_string() {
    assert_that!(Attr::of("name").to_string(), eq("@name"));
    assert_that!(Attr::of("two words").to_string(), eq(r#"@"two words""#));
    assert_that!(Attr::of(("name", 1)).to_string(), eq("@name(1)"));
}

#[test]
fn item_to_string() {
    assert_that!(Item::of(0).to_string(), eq("0"));
    let slot1: Item = ("name", 7).into();
    assert_that!(slot1.to_string(), eq("name:7"));
    let slot2: Item = (-5, "two words").into();
    assert_that!(slot2.to_string(), eq(r#"-5:"two words""#));
    let slot3 = Item::Slot("empty".into(), Value::Extant);
    assert_that!(slot3.to_string(), eq("empty:"));
}

#[test]
fn no_attr_record_to_string() {
    assert_that!(Value::empty_record().to_string(), eq("{}"));
    assert_that!(Value::singleton(0).to_string(), eq("{0}"));
    assert_that!(
        Value::singleton(Item::slot("a", 2)).to_string(),
        eq("{a:2}")
    );
    assert_that!(Value::from_vec(vec![1, 2, 3]).to_string(), eq("{1,2,3}"));
    assert_that!(
        Value::from_vec(vec!["a", "b", "c"]).to_string(),
        eq("{a,b,c}")
    );
    assert_that!(
        Value::record(vec![("a", 1).into(), 2.into(), ("c", 3).into()]).to_string(),
        eq("{a:1,2,c:3}")
    );
}

#[test]
fn with_attr_record_to_string() {
    let rec1 = Value::of_attr(Attr::of("name"));
    assert_that!(rec1.to_string(), eq("@name"));
    let rec2 = Value::of_attrs(vec![Attr::of("name1"), Attr::of("name2")]);
    assert_that!(rec2.to_string(), eq("@name1@name2"));
    let rec3 = Value::Record(vec![Attr::of("name")], vec![Item::of(3)]);
    assert_that!(rec3.to_string(), eq("@name{3}"));
    let rec4 = Value::Record(vec![Attr::of("name")], vec![(true, -1).into()]);
    assert_that!(rec4.to_string(), eq("@name{true:-1}"));
    let rec5 = Value::Record(vec![("name", 1).into()], vec![("a", 1).into(), 7.into()]);
    assert_that!(rec5.to_string(), eq("@name(1){a:1,7}"));
}

#[test]
fn attrs_with_record_bodies_to_string() {
    let attr1: Attr = ("name", Value::empty_record()).into();
    assert_that!(attr1.to_string(), eq("@name({})"));
    let attr2: Attr = ("name", Value::singleton(0)).into();
    assert_that!(attr2.to_string(), eq("@name({0})"));
    let attr3: Attr = ("name", Value::singleton(("a", 1))).into();
    assert_that!(attr3.to_string(), eq("@name(a:1)"));
    let attr4: Attr = (
        "name",
        Value::record(vec![("a", 1).into(), ("b", 2).into()]),
    )
        .into();
    assert_that!(attr4.to_string(), eq("@name(a:1,b:2)"));
}

#[test]
fn nested_records_to_string() {
    let double_empty = Value::from_vec(vec![Value::empty_record()]);
    assert_that!(double_empty.to_string(), eq("{{}}"));
    let inner = Value::from_vec(vec!["a", "b", "c"]);
    let nested1 = Value::from_vec(vec![inner.clone()]);
    assert_that!(nested1.to_string(), eq("{{a,b,c}}"));
    let nested2 = Value::record(vec![
        ("aa", 10).into(),
        inner.clone().into(),
        ("zz", 99).into(),
    ]);
    assert_that!(nested2.to_string(), eq("{aa:10,{a,b,c},zz:99}"));
    let attr_inner = Value::Record(vec![("name", 1).into()], vec![]);
    let nested3 = Value::from_vec(vec![attr_inner.clone()]);
    assert_that!(nested3.to_string(), eq("{@name(1)}"));
    let complex_inner = Value::Record(vec![("inner", 1).into()], vec![("a", 1).into(), 7.into()]);
    let nested_attr: Attr = ("outer", complex_inner.clone()).into();
    assert_that!(nested_attr.to_string(), eq("@outer(@inner(1){a:1,7})"));
}

#[test]
fn unsigned_sort() {
    assert!(Value::UInt64Value(100) < Value::UInt32Value(10000));
    assert_eq!(
        Value::UInt64Value(1000).cmp(&Value::UInt32Value(1000)),
        Ordering::Equal
    );
    assert!(Value::UInt64Value(u64::max_value()) > Value::UInt32Value(u32::max_value()));
    assert_eq!(
        Value::Float64Value(1.0).cmp(&Value::UInt32Value(100)),
        Ordering::Less
    );
    assert_eq!(
        Value::Float64Value(100.0).cmp(&Value::UInt32Value(1)),
        Ordering::Greater
    );
}

#[test]
fn bigint_i32_cmp() {
    let mut rng = rand::thread_rng();

    let vi32 = Value::Int32Value(100);
    let bi_value = Value::BigInt(rng.gen_bigint(1));
    assert!(vi32 > bi_value);

    let bi_value = Value::BigInt(rng.gen_bigint_range(&1000.into(), &10000.into()));
    assert!(vi32 < bi_value);

    let bi_value = Value::BigInt(100.into());
    assert_eq!(vi32.cmp(&bi_value), Ordering::Equal);
}

#[test]
fn bigint_i64_cmp() {
    let mut rng = rand::thread_rng();

    let i64 = Value::Int64Value(100);
    let bi_value = Value::BigInt(rng.gen_bigint(1));
    assert!(i64 > bi_value);

    let bi_value = Value::BigInt(rng.gen_bigint_range(&1000.into(), &10000.into()));
    assert!(i64 < bi_value);

    let bi_value = Value::BigInt(100.into());
    assert_eq!(i64.cmp(&bi_value), Ordering::Equal);
}

#[test]
fn bigint_f64_cmp() {
    let mut rng = rand::thread_rng();

    let bi_value = Value::BigInt(rng.gen_bigint(1));
    assert!(Value::Float64Value(100.0) > bi_value);

    let bi_value = Value::BigInt(rng.gen_bigint(1));
    assert!(Value::Float64Value(-100.0) < bi_value);

    let bi_value = Value::BigInt(rng.gen_bigint_range(&BigInt::from(0), &BigInt::from(1000)));
    assert!(Value::Float64Value(-100.0) < bi_value);

    assert_eq!(
        Value::Float64Value(100.0).cmp(&Value::BigInt(100.into())),
        Ordering::Equal
    );
}

#[test]
fn biguint_i32_cmp() {
    let mut rng = rand::thread_rng();

    let vi32 = Value::Int32Value(100);
    let bi_value = Value::BigUint(rng.gen_biguint(1));
    assert!(vi32 > bi_value);

    let bi_value = Value::BigUint(rng.gen_biguint_range(&1000u32.into(), &10000u32.into()));
    assert!(vi32 < bi_value);

    let bi_value = Value::BigUint(100u32.into());
    assert_eq!(vi32.cmp(&bi_value), Ordering::Equal);
}

#[test]
fn biguint_i64_cmp() {
    let mut rng = rand::thread_rng();

    let i64 = Value::Int64Value(100);
    let bi_value = Value::BigUint(rng.gen_biguint(1));
    assert!(i64 > bi_value);

    let bi_value = Value::BigUint(rng.gen_biguint_range(&1000u32.into(), &10000u32.into()));
    assert!(i64 < bi_value);

    let bi_value = Value::BigUint(100u32.into());
    assert_eq!(i64.cmp(&bi_value), Ordering::Equal);
}

#[test]
fn biguint_f64_cmp() {
    let mut rng = rand::thread_rng();

    let bi_value = Value::BigUint(rng.gen_biguint(1));
    assert!(Value::Float64Value(100.0) > bi_value);

    let bi_value = Value::BigUint(rng.gen_biguint(1));
    assert!(Value::Float64Value(-100.0) < bi_value);

    let bi_value = Value::BigUint(rng.gen_biguint(1000));
    assert!(Value::Float64Value(-100.0) < bi_value);

    assert_eq!(
        Value::Float64Value(100.0).cmp(&Value::BigUint(100u32.into())),
        Ordering::Equal
    );
}

#[test]
fn bigint_cmp() {
    assert!(Value::BigInt(BigInt::from(1000)) < Value::BigInt(BigInt::from(10000)));
    assert!(Value::BigInt(BigInt::from(1)) < Value::Int32Value(100));
    assert!(Value::BigInt(BigInt::from(1)) < Value::Int64Value(100));
    assert!(Value::BigInt(BigInt::from(1)) < Value::Float64Value(100.0));
    assert!(Value::BigInt(BigInt::from(1000)) < Value::BigUint(BigUint::from(10000u32)));
}

#[test]
fn biguint_cmp() {
    assert!(Value::BigUint(BigUint::from(1000u32)) < Value::BigInt(BigInt::from(10000)));
    assert!(Value::BigUint(BigUint::from(1u32)) < Value::Int32Value(100));
    assert!(Value::BigUint(BigUint::from(1u32)) < Value::Int64Value(100));
    assert!(Value::BigUint(BigUint::from(1u32)) < Value::Float64Value(100.0));
    assert!(Value::BigUint(BigUint::from(1000u32)) < Value::BigInt(BigInt::from(10000)));
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
