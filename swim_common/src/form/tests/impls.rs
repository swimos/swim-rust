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

use std::collections::{BTreeMap, BTreeSet, BinaryHeap, HashSet, LinkedList, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicI64, AtomicU32, AtomicU64};

use im::{HashMap as ImHashMap, HashSet as ImHashSet, OrdSet};
use num_bigint::{BigInt, BigUint};

use crate::form::Form;
use crate::model::blob::Blob;
use crate::model::{Attr, Item, Value};
use crate::record;
use std::fmt::Debug;

mod swim_common {
    pub use crate::*;
}

#[test]
fn transmute_bigint() {
    #[derive(Form)]
    struct S {
        a: BigInt,
        b: BigUint,
    }

    let s = S {
        a: BigInt::from(100),
        b: BigUint::from(100u32),
    };

    assert_eq!(
        s.as_value(),
        Value::Record(
            vec![Attr::from("S")],
            vec![
                Item::from(("a", Value::BigInt(BigInt::from(100)))),
                Item::from(("b", Value::BigUint(BigUint::from(100u32)))),
            ],
        )
    )
}

#[test]
fn blob() {
    #[derive(Form)]
    struct S {
        b: Blob,
    }

    let s = S {
        b: Blob::encode("blobby"),
    };

    assert_eq!(
        s.as_value(),
        Value::Record(
            vec![Attr::of("S")],
            vec![Item::Slot(
                Value::text("b"),
                Value::Data(Blob::from_vec(vec![89, 109, 120, 118, 89, 109, 74, 53])),
            )],
        )
    )
}

mod primitive {
    use std::cell::{Cell, RefCell};
    use std::sync::Arc;

    use super::*;

    macro_rules! test_impl {
        ($test_name:ident, $id:ident, $typ:expr, $expected:expr) => {
            #[test]
            fn $test_name() {
                let value = $typ.as_value();
                assert_eq!(value, $expected);
                assert_eq!($id::try_from_value(&value), Ok($typ))
            }
        };
    }

    test_impl!(test_bool, bool, true, Value::BooleanValue(true));
    test_impl!(test_i32, i32, 100i32, Value::Int32Value(100));
    test_impl!(test_i64, i64, 100i64, Value::Int64Value(100));
    test_impl!(test_u32, u32, 100u32, Value::UInt32Value(100));
    test_impl!(test_u64, u64, 100u64, Value::UInt64Value(100));
    test_impl!(test_f64, f64, 100.0f64, Value::Float64Value(100.0));
    test_impl!(test_opt_some, Option, Some(100i32), Value::Int32Value(100));
    test_impl!(
        test_string,
        String,
        String::from("test"),
        Value::text("test")
    );
    test_impl!(
        test_bigint,
        BigInt,
        BigInt::from(100),
        Value::BigInt(BigInt::from(100))
    );
    test_impl!(
        test_biguint,
        BigUint,
        BigUint::from(100u32),
        Value::BigUint(BigUint::from(100u32))
    );

    #[test]
    fn test_atomic_bool() {
        let value = AtomicBool::new(false).as_value();

        assert_eq!(value, Value::BooleanValue(false));
        assert_eq!(
            *AtomicBool::try_from_value(&value).unwrap().get_mut(),
            false
        );
    }

    #[test]
    fn test_atomic_i32() {
        let value = AtomicI32::new(123).as_value();

        assert_eq!(value, Value::Int32Value(123));
        assert_eq!(*AtomicI32::try_from_value(&value).unwrap().get_mut(), 123);
    }

    #[test]
    fn test_atomic_u32() {
        let value = AtomicU32::new(123).as_value();

        assert_eq!(value, Value::UInt32Value(123));
        assert_eq!(*AtomicU32::try_from_value(&value).unwrap().get_mut(), 123);
    }

    #[test]
    fn test_atomic_i64() {
        let value = AtomicI64::new(-123).as_value();

        assert_eq!(value, Value::Int64Value(-123));
        assert_eq!(*AtomicI64::try_from_value(&value).unwrap().get_mut(), -123);

        assert_eq!(
            *AtomicI64::try_from_value(&Value::BigInt(BigInt::from(-10)))
                .unwrap()
                .get_mut(),
            -10
        );
    }

    #[test]
    fn test_atomic_u64() {
        let value = AtomicU64::new(123).as_value();

        assert_eq!(value, Value::UInt64Value(123));
        assert_eq!(*AtomicU64::try_from_value(&value).unwrap().get_mut(), 123);
    }

    #[test]
    fn test_unit() {
        let value = ().as_value();
        assert_eq!(value, Value::Extant);
        assert_eq!(<()>::try_from_value(&value), Ok(()));
    }

    #[test]
    fn test_cell() {
        let value = Cell::new(100).as_value();
        assert_eq!(value, Value::Int32Value(100));
        assert_eq!(Cell::try_from_value(&value), Ok(Cell::new(100)));
    }

    #[test]
    fn test_refcell() {
        let value = RefCell::new(100).as_value();
        assert_eq!(value, Value::Int32Value(100));
        assert_eq!(RefCell::try_from_value(&value), Ok(RefCell::new(100)));
    }

    #[test]
    fn test_box() {
        let value = Box::new(100).as_value();
        assert_eq!(value, Value::Int32Value(100));
        assert_eq!(Box::try_from_value(&value), Ok(Box::new(100)));
    }

    #[test]
    fn test_arc() {
        let value = Arc::new(100).as_value();
        assert_eq!(value, Value::Int32Value(100));
        assert_eq!(Arc::try_from_value(&value), Ok(Arc::new(100)));
    }
}

mod collections {
    use im::HashMap;

    use super::*;

    #[test]
    fn test_opt_none() {
        let r: Option<i32> = None;
        let value = r.as_value();
        assert_eq!(value, Value::Extant);
    }

    #[test]
    fn test_opt_some() {
        let r = Some(100);
        let value = r.as_value();
        assert_eq!(value, Value::Int32Value(100));
    }

    fn sort_record(value: &mut Value) {
        if let Value::Record(_, items) = value {
            items.sort()
        } else {
            panic!("Expected record, found: {:?}", value)
        };
    }

    #[test]
    fn test_vec() {
        let vec = vec![1, 2, 3, 4, 5];
        let value = vec.as_value();

        assert_eq!(value, expected());
        assert_eq!(Vec::try_from_value(&value), Ok(vec))
    }

    #[test]
    fn im_hashset() {
        let mut hs = ImHashSet::new();
        hs.insert(1);
        hs.insert(2);
        hs.insert(3);
        hs.insert(4);
        hs.insert(5);

        let mut value = hs.as_value();
        sort_record(&mut value);

        assert_eq!(value, expected());
        assert_eq!(ImHashSet::try_from_value(&value), Ok(hs))
    }

    #[test]
    fn test_ord_set() {
        let mut os = OrdSet::new();
        os.insert(1);
        os.insert(2);
        os.insert(3);
        os.insert(4);
        os.insert(5);

        let value = os.as_value();

        assert_eq!(value, expected());
        assert_eq!(OrdSet::try_from_value(&value), Ok(os));
    }

    #[test]
    fn test_vecdeque() {
        let mut vec = VecDeque::new();
        vec.push_back(1);
        vec.push_back(2);
        vec.push_back(3);
        vec.push_back(4);
        vec.push_back(5);

        let value = vec.as_value();

        assert_eq!(value, expected());
        assert_eq!(VecDeque::try_from_value(&value), Ok(vec));
    }

    #[test]
    fn test_binaryheap() {
        let mut bh = BinaryHeap::new();
        bh.push(1);
        bh.push(2);
        bh.push(3);
        bh.push(4);
        bh.push(5);

        let mut value = bh.as_value();
        sort_record(&mut value);

        assert_eq!(value, expected());
    }

    #[test]
    fn test_btreeset() {
        let mut bts = BTreeSet::new();
        bts.insert(1);
        bts.insert(2);
        bts.insert(3);
        bts.insert(4);
        bts.insert(5);

        let value = bts.as_value();

        assert_eq!(value, expected());
        assert_eq!(BTreeSet::try_from_value(&value), Ok(bts));
    }

    #[test]
    fn test_hashset() {
        let mut hs = HashSet::new();
        hs.insert(1);
        hs.insert(2);
        hs.insert(3);
        hs.insert(4);
        hs.insert(5);

        let mut value = hs.as_value();
        sort_record(&mut value);

        assert_eq!(value, expected());
        assert_eq!(HashSet::try_from_value(&value), Ok(hs));
    }

    #[test]
    fn test_linkedlist() {
        let mut ll = LinkedList::new();
        ll.push_back(1);
        ll.push_back(2);
        ll.push_back(3);
        ll.push_back(4);
        ll.push_back(5);

        let value = ll.as_value();

        assert_eq!(value, expected());
        assert_eq!(LinkedList::try_from_value(&value), Ok(ll))
    }

    #[test]
    fn test_imhashmap() {
        let mut hm = ImHashMap::new();
        hm.insert(String::from("1"), 1);
        hm.insert(String::from("2"), 2);
        hm.insert(String::from("3"), 3);
        hm.insert(String::from("4"), 4);
        hm.insert(String::from("5"), 5);

        let expected = Value::record(vec![
            Item::slot(String::from("1"), Value::Int32Value(1)),
            Item::slot(String::from("2"), Value::Int32Value(2)),
            Item::slot(String::from("3"), Value::Int32Value(3)),
            Item::slot(String::from("4"), Value::Int32Value(4)),
            Item::slot(String::from("5"), Value::Int32Value(5)),
        ]);

        let mut value = hm.as_value();
        sort_record(&mut value);

        assert_eq!(value, expected);
        assert_eq!(HashMap::try_from_value(&value), Ok(hm));
    }

    #[test]
    fn test_btreemap() {
        let mut btm = BTreeMap::new();
        btm.insert(1, 1);
        btm.insert(2, 2);
        btm.insert(3, 3);
        btm.insert(4, 4);
        btm.insert(5, 5);

        let mut value = btm.as_value();
        sort_record(&mut value);
        let expected = Value::record(vec![
            Item::slot(1, Value::Int32Value(1)),
            Item::slot(2, Value::Int32Value(2)),
            Item::slot(3, Value::Int32Value(3)),
            Item::slot(4, Value::Int32Value(4)),
            Item::slot(5, Value::Int32Value(5)),
        ]);

        assert_eq!(value, expected);
        assert_eq!(BTreeMap::try_from_value(&value), Ok(btm));
    }

    fn expected() -> Value {
        Value::record(vec![
            Item::of(Value::Int32Value(1)),
            Item::of(Value::Int32Value(2)),
            Item::of(Value::Int32Value(3)),
            Item::of(Value::Int32Value(4)),
            Item::of(Value::Int32Value(5)),
        ])
    }
}

mod field_collections {
    #[allow(unused_imports)]
    use form_derive::*;

    use super::*;

    fn expected() -> Value {
        Value::record(vec![
            Item::of(Value::Int32Value(1)),
            Item::of(Value::Int32Value(2)),
            Item::of(Value::Int32Value(3)),
            Item::of(Value::Int32Value(4)),
            Item::of(Value::Int32Value(5)),
        ])
    }

    macro_rules! test_impl {
        ($name:ident, $typ:ty, $initial:expr) => {
            #[test]
            fn $name() {
                #[derive(Form)]
                struct Test {
                    member: $typ,
                }

                let val = Test { member: $initial };
                let rec = Value::Record(
                    vec![Attr::of("Test")],
                    vec![Item::Slot(Value::text("member"), expected())],
                );

                assert_eq!(val.as_value(), rec);
            }
        };
    }

    test_impl!(vec, Vec::<i32>, vec![1, 2, 3, 4, 5]);
    test_impl!(im_ordered_set, OrdSet::<i32>, {
        let mut os = OrdSet::new();
        os.insert(1);
        os.insert(2);
        os.insert(3);
        os.insert(4);
        os.insert(5);
        os
    });
    test_impl!(vecdeque, VecDeque::<i32>, {
        let mut vec = VecDeque::new();
        vec.push_back(1);
        vec.push_back(2);
        vec.push_back(3);
        vec.push_back(4);
        vec.push_back(5);
        vec
    });
    test_impl!(btreeset, BTreeSet::<i32>, {
        let mut bts = BTreeSet::new();
        bts.insert(1);
        bts.insert(2);
        bts.insert(3);
        bts.insert(4);
        bts.insert(5);
        bts
    });
    test_impl!(linkedlist, LinkedList::<i32>, {
        let mut ll = LinkedList::new();
        ll.push_back(1);
        ll.push_back(2);
        ll.push_back(3);
        ll.push_back(4);
        ll.push_back(5);
        ll
    });
}

#[test]
fn test_tuples() {
    fn run_test<F: Form + PartialEq + Debug + Clone>(form: F, expected: Value) {
        assert_eq!(form.as_value(), expected);
        assert_eq!(form.clone().into_value(), expected);
        assert_eq!(F::try_from_value(&expected), Ok(form.clone()));
        assert_eq!(F::try_convert(expected), Ok(form));
    }

    run_test((1, 2), record!(1i32, 2i32));
    run_test((1, 2, 3), record!(1i32, 2i32, 3i32));
    run_test((1, 2, 3, 4), record!(1i32, 2i32, 3i32, 4i32));
    run_test((1, 2, 3, 4, 5), record!(1i32, 2i32, 3i32, 4i32, 5i32));
    run_test(
        (1, 2, 3, 4, 5, 6),
        record!(1i32, 2i32, 3i32, 4i32, 5i32, 6i32),
    );
    run_test(
        (1, 2, 3, 4, 5, 6, 7),
        record!(1i32, 2i32, 3i32, 4i32, 5i32, 6i32, 7i32),
    );
    run_test(
        (1, 2, 3, 4, 5, 6, 7, 8),
        record!(1i32, 2i32, 3i32, 4i32, 5i32, 6i32, 7i32, 8i32),
    );
    run_test(
        (1, 2, 3, 4, 5, 6, 7, 8, 9),
        record!(1i32, 2i32, 3i32, 4i32, 5i32, 6i32, 7i32, 8i32, 9i32),
    );
    run_test(
        (1, 2, 3, 4, 5, 6, 7, 8, 9, 10),
        record!(1i32, 2i32, 3i32, 4i32, 5i32, 6i32, 7i32, 8i32, 9i32, 10i32),
    );
    run_test(
        (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11),
        record!(1i32, 2i32, 3i32, 4i32, 5i32, 6i32, 7i32, 8i32, 9i32, 10i32, 11i32),
    );
    run_test(
        (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12),
        record!(1i32, 2i32, 3i32, 4i32, 5i32, 6i32, 7i32, 8i32, 9i32, 10i32, 11i32, 12i32),
    );
}
