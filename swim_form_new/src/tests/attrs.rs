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

use common::model::{Attr, Item, Value};

use crate::Form;

mod swim_form {
    pub use crate::*;
}

mod renaming {
    use super::*;

    #[test]
    fn test_rename_single() {
        #[form(Value)]
        struct F {
            #[form(ser_name = "epoch")]
            age: i32,
        }

        let fs = F { age: 30 };
        let value: Value = fs.as_value();
        let expected = Value::Record(vec![Attr::of("F")], vec![Item::slot("epoch", 30)]);

        assert_eq!(value, expected);
    }

    #[test]
    fn test_rename_multiple() {
        #[form(Value)]
        struct F {
            #[form(ser_name = "1")]
            a: i32,
            #[form(ser_name = "2")]
            b: i32,
            #[form(ser_name = "3")]
            c: i32,
            #[form(ser_name = "4")]
            d: i32,
            #[form(ser_name = "5")]
            e: i32,
        }

        let fs = F {
            a: 0,
            b: 1,
            c: 2,
            d: 3,
            e: 4,
        };

        let value: Value = fs.as_value();
        let expected = Value::Record(
            vec![Attr::of("F")],
            vec![
                Item::slot("1", 0),
                Item::slot("2", 1),
                Item::slot("3", 2),
                Item::slot("4", 3),
                Item::slot("5", 4),
            ],
        );

        assert_eq!(value, expected);
    }

    #[test]
    fn rename_duplicates() {
        #[form(Value)]
        struct F {
            #[form(ser_name = "epoch")]
            age: i32,
            #[form(ser_name = "epoch")]
            name: String,
        }

        let fs = F {
            age: 30,
            name: String::from("swim"),
        };
        let value: Value = fs.as_value();
        let expected = Value::Record(
            vec![Attr::of("F")],
            vec![Item::slot("epoch", 30), Item::slot("epoch", "swim")],
        );

        assert_eq!(value, expected);
    }
}

mod shifting {
    use super::*;

    #[test]
    fn primitive_as_attr() {
        #[form(Value)]
        struct Outer {
            #[form(attr)]
            inner: i32,
        }

        let fs = Outer { inner: 13 };
        let value: Value = fs.as_value();

        let expected = Value::Record(vec![Attr::of("Outer"), Attr::of(("inner", 13))], vec![]);

        assert_eq!(value, expected);
    }

    #[test]
    fn struct_as_attr() {
        #[form(Value)]
        struct Outer {
            #[form(attr)]
            inner: Inner,
        }

        #[form(Value)]
        struct Inner;

        let fs = Outer { inner: Inner };
        let value: Value = fs.as_value();

        let expected = Value::Record(
            vec![
                Attr::of("Outer"),
                Attr::of(("inner", Value::Record(vec![Attr::of("Inner")], Vec::new()))),
            ],
            vec![],
        );

        assert_eq!(value, expected);
    }

    #[test]
    fn collection_as_attr() {
        #[form(Value)]
        struct Outer {
            #[form(attr)]
            v: Vec<i32>,
        }

        let fs = Outer {
            v: vec![1, 2, 3, 4, 5],
        };
        let value: Value = fs.as_value();

        let expected = Value::Record(
            vec![
                Attr::of("Outer"),
                Attr::of((
                    "v",
                    Value::Record(
                        Vec::new(),
                        vec![
                            Item::of(Value::Int32Value(1)),
                            Item::of(Value::Int32Value(2)),
                            Item::of(Value::Int32Value(3)),
                            Item::of(Value::Int32Value(4)),
                            Item::of(Value::Int32Value(5)),
                        ],
                    ),
                )),
            ],
            vec![],
        );

        assert_eq!(value, expected);
    }
}
