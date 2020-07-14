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

use crate::deserialize::FormDeserializeErr;
use crate::serialize::serializer::{SerializerState, ValueSerializer};
use crate::serialize::{FormSerializeErr, SerializeToValue};
use crate::Form;

#[test]
fn test_serialize_struct() {
    struct S {
        a: i32,
        b: String,
    }

    impl Form for S {
        fn as_value(&self) -> Value {
            crate::serialize::as_value(self)
        }

        fn try_from_value(_value: &Value) -> Result<Self, FormDeserializeErr> {
            unimplemented!()
        }
    }

    impl SerializeToValue for S {
        fn serialize(&self, serializer: &mut ValueSerializer) {
            serializer.serialize_struct("S", 2);
            serializer.serialize_field(Some("a"), &self.a);
            serializer.serialize_field(Some("b"), &self.b);

            serializer.exit_nested();
        }
    }

    let s = S {
        a: 1,
        b: "a string".to_string(),
    };

    let value = crate::serialize::as_value(&s);

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
fn test_serialize_newtype_struct() {
    struct S(i32);

    impl Form for S {
        fn as_value(&self) -> Value {
            crate::serialize::as_value(self)
        }

        fn try_from_value(_value: &Value) -> Result<Self, FormDeserializeErr> {
            unimplemented!()
        }
    }

    impl SerializeToValue for S {
        fn serialize(&self, serializer: &mut ValueSerializer) {
            serializer.serialize_struct("S", 2);
            serializer.serialize_field(None, &self.0);

            serializer.exit_nested();
        }
    }

    let s = S(1);
    let value = crate::serialize::as_value(&s);
    let expected = Value::Record(vec![Attr::of("S")], vec![Item::of(Value::Int32Value(1))]);

    assert_eq!(value, expected);
}
