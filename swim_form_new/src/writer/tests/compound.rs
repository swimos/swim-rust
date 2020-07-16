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

use crate::reader::{ValueReadError, ValueReader};
use crate::writer::writer::{ValueWriter, WriterState};
use crate::{Form, TransmuteValue};
use common::model::{Attr, Item, Value};

#[test]
fn test_write_struct() {
    struct S {
        a: i32,
        b: String,
    }

    impl Form for S {
        fn as_value(&self) -> Value {
            crate::writer::as_value(self)
        }

        fn try_from_value(_value: &Value) -> Result<Self, ValueReadError> {
            unimplemented!()
        }
    }

    impl TransmuteValue for S {
        fn transmute_to_value(&self, writer: &mut ValueWriter) {
            writer.transmute_struct("S", 2);
            writer.transmute_field(Some("a"), &self.a);
            writer.transmute_field(Some("b"), &self.b);

            writer.exit_nested();
        }

        fn transmute_from_value(&self, reader: &mut ValueReader) -> Result<Self, ValueReadError> {
            unimplemented!()
        }
    }

    let s = S {
        a: 1,
        b: "a string".to_string(),
    };

    let value = crate::writer::as_value(&s);

    let expected = Value::Record(
        vec![Attr::of("S")],
        vec![
            Item::slot("a", Value::Int32Value(1)),
            Item::slot("b", Value::Text(String::from("a string"))),
        ],
    );

    assert_eq!(value, expected);
}

#[test]
fn test_write_newtype_struct() {
    struct S(i32);

    impl Form for S {
        fn as_value(&self) -> Value {
            crate::writer::as_value(self)
        }

        fn try_from_value(_value: &Value) -> Result<Self, ValueReadError> {
            unimplemented!()
        }
    }

    impl TransmuteValue for S {
        fn transmute_to_value(&self, writer: &mut ValueWriter) {
            writer.transmute_struct("S", 2);
            writer.transmute_field(None, &self.0);

            writer.exit_nested();
        }

        fn transmute_from_value(&self, reader: &mut ValueReader) -> Result<Self, ValueReadError> {
            unimplemented!()
        }
    }

    let s = S(1);
    let value = crate::writer::as_value(&s);
    let expected = Value::Record(vec![Attr::of("S")], vec![Item::of(Value::Int32Value(1))]);

    assert_eq!(value, expected);
}
