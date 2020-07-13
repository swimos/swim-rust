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

use trybuild::TestCases;

use common::model::{Attr, Item, Value};

use crate::Form;
use crate::FormDeserializeErr;
use base64::URL_SAFE;
use common::model::blob::Blob;

#[cfg(test)]
mod traits;

#[test]
fn test_derive() {
    let t = TestCases::new();

    t.compile_fail("src/tests/unimplemented/unimplemented_compound.rs");
    t.compile_fail("src/tests/unimplemented/unimplemented_nested.rs");
    t.compile_fail("src/tests/unimplemented/unimplemented_primitive.rs");
    t.compile_fail("src/tests/unimplemented/unimplemented_vector.rs");
}

// Mimics the form_derive include declarations
mod swim_form {
    pub use _deserialize;
    pub use _serialize;

    pub use crate::*;
}

#[test]
fn simple_vector() {
    #[form(Value)]
    struct FormStruct {
        a: Vec<i32>,
    }

    let _ = FormStruct {
        a: vec![1, 2, 3, 4, 5],
    };
}

#[test]
fn vector_with_compound() {
    #[form(Value)]
    struct FormStruct {
        a: Vec<Child>,
    }

    #[form(Value)]
    struct Child {
        a: i32,
    }

    let _ = FormStruct {
        a: vec![Child { a: 1 }, Child { a: 2 }, Child { a: 3 }],
    };
}

#[test]
fn nested_enum() {
    #[form(Value)]
    enum Parent {
        A,
        B(i32),
        C { c: Child },
    }

    #[form(Value)]
    enum Child {
        A,
        B(i32),
        C { a: String },
    }

    let _ = Parent::C {
        c: Child::C {
            a: String::from("A String"),
        },
    };
}

#[test]
fn single_enum() {
    #[form(Value)]
    enum SomeEnum {
        A,
        B(i32),
        C { a: String },
    }

    let _ = SomeEnum::C {
        a: String::from("A String"),
    };
}

#[test]
fn nested_derives() {
    #[form(Value)]
    struct Parent {
        a: i32,
        b: Child,
    }

    #[form(Value)]
    struct Child {
        c: i32,
    }

    let _ = Parent {
        a: 1,
        b: Child { c: 1 },
    };
}

#[test]
fn newtype() {
    #[form(Value)]
    struct FormStruct(i32);

    let _ = FormStruct(1);
}

#[test]
fn single_derve() {
    #[form(Value)]
    struct FormStruct {
        a: i32,
    }

    let _ = FormStruct { a: 1 };
}

#[test]
fn tuple_struct() {
    #[form(Value)]
    struct FormStruct(i32, String);

    let _ = FormStruct(1, String::from("hello"));
}

#[test]
fn unit_struct() {
    #[form(Value)]
    struct FormStruct;

    let _ = FormStruct;
}

#[test]
fn enum_ser_ok() {
    #[form(Value)]
    #[derive(PartialEq)]
    enum Parent {
        A,
    }

    let record = Value::of_attr(Attr::of(("A", Value::Extant)));
    let parent = Parent::A;
    let result = parent.as_value();

    assert_eq!(result, record)
}

#[test]
fn enum_ser_struct_ok() {
    #[form(Value)]
    #[derive(PartialEq)]
    enum Parent {
        A { b: i32, c: i64 },
    }

    let record = Value::Record(
        vec![Attr::of(("A", Value::Extant))],
        vec![Item::slot("b", 1), Item::slot("c", Value::Int64Value(2))],
    );

    let parent = Parent::A { b: 1, c: 2 };
    let result = parent.as_value();

    assert_eq!(result, record)
}

#[test]
fn enum_ser_tuple_ok() {
    #[form(Value)]
    #[derive(PartialEq)]
    enum Parent {
        A(i32, i32),
    }

    let record = Value::Record(
        vec![Attr::of(("A", Value::Extant))],
        vec![Item::from(1), Item::from(2)],
    );

    let parent = Parent::A(1, 2);
    let result = parent.as_value();

    assert_eq!(result, record)
}

#[test]
fn struct_deserialize_err() {
    #[form(Value)]
    #[derive(PartialEq)]
    struct Parent {
        a: i32,
    }

    let record = Value::Record(vec![Attr::from("Parent")], vec![Item::from(("a", 1.0))]);

    let result = Parent::try_from_value(&record);

    match result {
        Ok(_) => panic!(),
        Err(e) => assert_eq!(
            e,
            FormDeserializeErr::IncorrectType(String::from(
                "Expected: Value::Int32Value, found: Float64"
            ))
        ),
    }
}

#[test]
fn struct_deserialize_ok() {
    #[form(Value)]
    #[derive(PartialEq, Debug)]
    struct Parent {
        a: i32,
    }

    let record = Value::Record(vec![Attr::from("Parent")], vec![Item::from(("a", 1))]);

    let result = Parent::try_from_value(&record).unwrap();
    assert_eq!(result, Parent { a: 1 })
}

#[test]
fn newtype_de_err() {
    #[form(Value)]
    #[derive(PartialEq, Debug)]
    struct Parent(i32);

    let record = Value::Record(vec![Attr::from("Incorrect")], vec![Item::from("hello")]);

    let result = Parent::try_from_value(&record);
    match result {
        Ok(_) => panic!("Expected failure"),
        Err(e) => assert_eq!(e, FormDeserializeErr::Malformatted),
    }

    let record = Value::Record(vec![Attr::from("Parent")], vec![Item::from("hello")]);

    let result = Parent::try_from_value(&record);
    match result {
        Ok(_) => panic!("Expected failure"),
        Err(e) => assert_eq!(
            e,
            FormDeserializeErr::IncorrectType(
                "Expected: Value::Int32Value, found: Text".to_string()
            )
        ),
    }
}

#[test]
fn newtype_de_ok() {
    #[form(Value)]
    #[derive(PartialEq, Debug)]
    struct Parent(i32);

    let record = Value::Record(vec![Attr::from("Parent")], vec![Item::from(1)]);

    let result = Parent::try_from_value(&record).unwrap();
    assert_eq!(result, Parent(1))
}

#[test]
fn newtype_ser_ok() {
    #[form(Value)]
    #[derive(PartialEq)]
    struct Parent(i32);

    let record = Value::Record(vec![Attr::from("Parent")], vec![Item::from(1)]);

    let parent = Parent(1);
    let result = parent.as_value();

    assert_eq!(result, record)
}

#[test]
fn struct_serialize_ok() {
    #[form(Value)]
    #[derive(PartialEq)]
    struct Parent {
        a: i32,
    }

    let record = Value::Record(vec![Attr::from("Parent")], vec![Item::from(("a", 1))]);

    let parent = Parent { a: 1 };
    let result = parent.as_value();

    assert_eq!(result, record)
}

#[test]
fn tuple_de_err() {
    #[form(Value)]
    #[derive(PartialEq, Debug)]
    struct Parent(i32, i32);

    let record = Value::Record(
        vec![Attr::from("Incorrect")],
        vec![Item::from(1), Item::from(2)],
    );

    let result = Parent::try_from_value(&record);
    match result {
        Ok(_) => panic!("Expected failure"),
        Err(e) => assert_eq!(e, FormDeserializeErr::Malformatted),
    }

    let record = Value::Record(vec![Attr::from("Parent")], vec![Item::from("hello")]);

    let result = Parent::try_from_value(&record);
    match result {
        Ok(_) => panic!("Expected failure"),
        Err(e) => assert_eq!(
            e,
            FormDeserializeErr::IncorrectType(
                "Expected: Value::Int32Value, found: Text".to_string()
            )
        ),
    }
}

#[test]
fn tuple_de_ok() {
    #[form(Value)]
    #[derive(PartialEq, Debug)]
    struct Parent(i32, i32);

    let record = Value::Record(
        vec![Attr::from("Parent")],
        vec![Item::from(1), Item::from(2)],
    );

    let result = Parent::try_from_value(&record).unwrap();
    assert_eq!(result, Parent(1, 2))
}

#[test]
fn unit_de_err() {
    #[form(Value)]
    #[derive(PartialEq, Debug)]
    struct Parent;

    let record = Value::Record(vec![Attr::from("Incorrect")], vec![]);

    let result = Parent::try_from_value(&record);
    match result {
        Ok(_) => panic!("Expected failure"),
        Err(e) => assert_eq!(e, FormDeserializeErr::Malformatted),
    }

    let record = Value::Record(vec![Attr::from("Parent")], vec![Item::from("hello")]);

    if let Err(e) = Parent::try_from_value(&record) {
        panic!(
            "Expected deserializer to parse record and discard fields. Err: {:?}",
            e
        );
    }
}

#[test]
fn unit_de_ok() {
    #[form(Value)]
    #[derive(PartialEq, Debug)]
    struct Parent(i32);

    let record = Value::Record(vec![Attr::from("Parent")], vec![Item::from(1)]);

    let result = Parent::try_from_value(&record).unwrap();
    assert_eq!(result, Parent(1))
}

#[test]
fn unit_ser_ok() {
    #[form(Value)]
    #[derive(PartialEq)]
    struct Parent;

    let record = Value::Record(vec![Attr::from("Parent")], Vec::new());

    let parent = Parent;
    let result = parent.as_value();

    assert_eq!(result, record)
}

#[test]
fn blob_ser_ok() {
    #[form(Value)]
    #[derive(PartialEq, Debug)]
    struct S {
        #[form(blob)]
        blob: Blob,
    }
    let s = S {
        blob: Blob::encode("swimming"),
    };

    let result = s.as_value();
    let expected = Value::Record(
        vec![Attr::of(("S", Value::Extant))],
        vec![Item::slot("blob", Value::Data(Blob::encode("swimming")))],
    );
    assert_eq!(result, expected)
}

#[test]
fn blob_de_ok() {
    #[form(Value)]
    #[derive(PartialEq, Debug)]
    struct S {
        #[form(blob)]
        blob: Blob,
    }

    let value = Value::Record(
        vec![Attr::of(("S", Value::Extant))],
        vec![Item::slot("blob", Value::Data(Blob::encode("swimming")))],
    );

    let result = S::try_from_value(&value).unwrap();
    let expected = S {
        blob: Blob::encode("swimming"),
    };

    assert_eq!(result, expected)
}

#[test]
fn text_to_blob() {
    #[form(Value)]
    #[derive(PartialEq, Debug)]
    struct S {
        #[form(blob)]
        blob: Blob,
    }

    let encoded = base64::encode_config("swimming", URL_SAFE);
    let value = Value::Record(
        vec![Attr::of(("S", Value::Extant))],
        vec![Item::slot("blob", Value::Text(encoded))],
    );

    let result = S::try_from_value(&value).unwrap();
    let expected = S {
        blob: Blob::encode("swimming"),
    };

    assert_eq!(result, expected)
}
