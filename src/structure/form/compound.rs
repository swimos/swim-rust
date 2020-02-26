use std::borrow::{Borrow, BorrowMut};
use std::convert::TryInto;

use serde::{ser, Serialize};

use error::{Error, Result};

use crate::model::{Attr, Item, Value};
use crate::model::Item::{Slot, ValueItem};
use crate::model::Value::Record;

mod error {
    pub use serde::de::value::Error;

    pub type Result<T> = ::std::result::Result<T, Error>;
}

#[derive(Debug, PartialEq)]
enum State {
    /// Not read any fields yet.
    Unknown,
    ReadingItemWithAttribute,
    ReadingItem,
    ReadingSequence(Option<usize>),
}

#[derive(Debug, PartialEq)]
struct SerializerState {
    state: State,
    stack: Vec<Value>,
}

impl SerializerState {
    fn state(&self) -> &State {
        self.state.borrow()
    }

    fn stack(&mut self) -> &mut Vec<Value> {
        self.stack.as_mut()
    }
}

struct Serializer {
    output: Value,
    state: Vec<SerializerState>,
}

impl Serializer {
    fn push_state(&mut self, state: State) {
        self.state.push(SerializerState {
            state,
            stack: Vec::new(),
        });
    }

    fn push(&mut self, value: Value) {
        let v = self.state.pop();
        match v {
            Some(mut s) => {
                s.stack.push(value);
                self.state.push(s);
            }
            None => {
                self.state.push(SerializerState {
                    state: State::Unknown,
                    stack: vec![value],
                });
            }
        }

        let serializer_state = self.current_state();
        let state = &serializer_state.state;
        match state {
            State::ReadingItemWithAttribute => {}//*state = State::Unknown,
            _ => {}
        }
    }

    fn push_attr(&mut self, name: &str) {
        self.push_state(State::ReadingItemWithAttribute);
        let item = Item::Slot(Value::Text(String::from(name)), Value::Extant);

        match self.state.last_mut() {
            Some(s) => {
                s.stack().push(Value::Record(Vec::new(), vec![item]));
            }
            None => {
                //todo
                panic!()
            }
        }
    }

    fn current_state(&mut self) -> &SerializerState {
        if self.state.len() == 0 {
            let serializer_state = SerializerState {
                state: State::ReadingItemWithAttribute,
                stack: vec![],
            };
            self.state.push(serializer_state);
        }

        self.state.last().unwrap()
    }

    fn set_current(&mut self, value: Value) {
        match &mut self.output {
            Value::Record(_, ref mut items) => {
                if items.len() == 0 {
                    items.push(Item::ValueItem(value));
                } else {
                    let last = items.last_mut().unwrap();

                    match last {
                        Item::Slot(k, v) => {
                            *last = Item::Slot(k.to_owned(), value);
                        }
                        Item::ValueItem(v) => {
                            // todo
                            panic!()
                        }
                    }
                }
            }
            v_ @ _ => {
                // todo
                panic!()
            }
        }
    }

    fn drain_stack(&mut self) {
        let stack = &self.current_state().stack;
        let items = stack.iter().fold(Vec::new(), |mut vec, item| {
            vec.push(Item::ValueItem(item.to_owned()));
            vec
        });

        self.set_current(Value::Record(Vec::new(), items));
    }
}

// By convention, the public API of a Serde serializer is one or more `to_abc`
// functions such as `to_string`, `to_bytes`, or `to_writer` depending on what
// Rust types the serializer is able to produce as output.
//
// This basic serializer supports only `to_string`.
pub fn to_string<T>(value: &T) -> Result<Value>
    where
        T: Serialize,
{
    let mut serializer = Serializer {
        state: vec![SerializerState {
            state: State::ReadingItem,
            stack: Vec::new(),
        }],
        output: Value::Record(Vec::new(), Vec::new()),
    };
    value.serialize(&mut serializer)?;
    Ok(serializer.output)
}

//noinspection ALL,RsTraitImplementation
impl<'a> ser::Serializer for &'a mut Serializer {
    // The output type produced by this `Serializer` during successful
    // serialization. Most serializers that produce text or binary output should
    // set `Ok = ()` and serialize into an `io::Write` or buffer contained
    // within the `Serializer` instance, as happens here. Serializers that build
    // in-memory data structures may be simplified by using `Ok` to propagate
    // the data structure around.
    type Ok = ();

    // The error type when some error occurs during serialization.
    type Error = Error;

    // Associated types for keeping track of additional state while serializing
    // compound data structures like sequences and maps. In this case no
    // additional state is required beyond what is already stored in the
    // Serializer struct.
    type SerializeSeq = Self;
    type SerializeTuple = Self;
    type SerializeTupleStruct = Self;
    type SerializeTupleVariant = Self;
    type SerializeMap = Self;
    type SerializeStruct = Self;
    type SerializeStructVariant = Self;

    // Here we go with the simple methods. The following 12 methods receive one
    // of the primitive types of the data model and map it to JSON by appending
    // into the output string.
    fn serialize_bool(self, v: bool) -> Result<()> {
        self.push(Value::BooleanValue(v));
        Ok(())
    }

    // JSON does not distinguish between different sizes of integers, so all
    // signed integers will be serialized the same and all unsigned integers
    // will be serialized the same. Other formats, especially compact binary
    // formats, may need independent logic for the different sizes.
    fn serialize_i8(self, v: i8) -> Result<()> {
        self.serialize_i32(i32::from(v))
    }

    fn serialize_i16(self, v: i16) -> Result<()> {
        self.serialize_i32(i32::from(v))
    }

    fn serialize_i32(self, v: i32) -> Result<()> {
        self.push(Value::Int32Value(v));
        Ok(())
    }

    fn serialize_i64(self, v: i64) -> Result<()> {
        self.push(Value::Int64Value(v));
        Ok(())
    }

    fn serialize_u8(self, v: u8) -> Result<()> {
        self.serialize_u32(u32::from(v))
    }

    fn serialize_u16(self, v: u16) -> Result<()> {
        self.serialize_u32(u32::from(v))
    }

    // TODO: Unsigned types
    fn serialize_u32(self, v: u32) -> Result<()> {
        self.push(Value::Int32Value(v as i32));
        Ok(())
    }

    fn serialize_u64(self, v: u64) -> Result<()> {
        self.push(Value::Int64Value(v as i64));
        Ok(())
    }

    fn serialize_f32(self, v: f32) -> Result<()> {
        self.serialize_f64(f64::from(v))
    }

    fn serialize_f64(self, v: f64) -> Result<()> {
        self.push(Value::Float64Value(v));
        Ok(())
    }

    fn serialize_char(self, v: char) -> Result<()> {
        self.push(Value::Text(v.to_string()));
        Ok(())
    }

    fn serialize_str(self, v: &str) -> Result<()> {
        self.push(Value::Text(String::from(v)));
        Ok(())
    }

    // Serialize a byte array as an array of bytes. Could also use a base64
    // string here. Binary formats will typically represent byte arrays more
    // compactly.
    fn serialize_bytes(self, v: &[u8]) -> Result<()> {
        use serde::ser::SerializeSeq;
        let mut seq = self.serialize_seq(Some(v.len()))?;
        for byte in v {
            seq.serialize_element(byte)?;
        }
        seq.end()
    }

    // An absent optional is represented as the JSON `null`.
    fn serialize_none(self) -> Result<()> {
        self.serialize_unit()
    }

    // A present optional is represented as just the contained value. Note that
    // this is a lossy representation. For example the values `Some(())` and
    // `None` both serialize as just `null`. Unfortunately this is typically
    // what people expect when working with JSON. Other formats are encouraged
    // to behave more intelligently if possible.
    fn serialize_some<T>(self, value: &T) -> Result<()>
        where
            T: ?Sized + Serialize,
    {
        value.serialize(self)
    }

    // In Serde, unit means an anonymous value containing no data. Map this to
    // JSON as `null`.
    fn serialize_unit(self) -> Result<()> {
        self.push(Value::Extant);
        Ok(())
    }

    // Unit struct means a named value containing no data. Again, since there is
    // no data, map this to JSON as `null`. There is no need to serialize the
    // name in most formats.
    fn serialize_unit_struct(self, _name: &'static str) -> Result<()> {
        self.serialize_unit()
    }

    // When serializing a unit variant (or any other kind of variant), formats
    // can choose whether to keep track of it by index or by name. Binary
    // formats typically use the index of the variant and human-readable formats
    // typically use the name.
    fn serialize_unit_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
    ) -> Result<()> {
        self.serialize_str(variant)
    }

    // As is done here, serializers are encouraged to treat newtype structs as
    // insignificant wrappers around the data they contain.
    fn serialize_newtype_struct<T>(
        self,
        _name: &'static str,
        value: &T,
    ) -> Result<()>
        where
            T: ?Sized + Serialize,
    {
        value.serialize(self)
    }

    // Note that newtype variant (and all of the other variant serialization
    // methods) refer exclusively to the "externally tagged" enum
    // representation.
    //
    // Serialize this to JSON in externally tagged form as `{ NAME: VALUE }`.
    fn serialize_newtype_variant<T>(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
        value: &T,
    ) -> Result<()>
        where
            T: ?Sized + Serialize,
    {
        self.push_attr(variant);
        value.serialize(&mut *self)?;
        Ok(())
    }

    // Now we get to the serialization of compound types.
    //
    // The start of the sequence, each value, and the end are three separate
    // method calls. This one is responsible only for serializing the start,
    // which in JSON is `[`.
    //
    // The length of the sequence may or may not be known ahead of time. This
    // doesn't make a difference in JSON because the length is not represented
    // explicitly in the serialized form. Some serializers may only be able to
    // support sequences for which the length is known up front.
    fn serialize_seq(self, len: Option<usize>) -> Result<Self::SerializeSeq> {
        self.push_state(State::ReadingSequence(len));
        Ok(self)
    }

    // Tuples look just like sequences in JSON. Some formats may be able to
    // represent tuples more efficiently by omitting the length, since tuple
    // means that the corresponding `Deserialize implementation will know the
    // length without needing to look at the serialized data.
    fn serialize_tuple(self, len: usize) -> Result<Self::SerializeTuple> {
        self.serialize_seq(Some(len))
    }

    // Tuple structs look just like sequences in JSON.
    fn serialize_tuple_struct(
        self,
        _name: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleStruct> {
        self.serialize_seq(Some(len))
    }

    // Tuple variants are represented in JSON as `{ NAME: [DATA...] }`. Again
    // this method is only responsible for the externally tagged representation.
    fn serialize_tuple_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleVariant> {
        variant.serialize(&mut *self)?;
        Ok(self)
    }

    // Maps are represented in JSON as `{ K: V, K: V, ... }`.
    fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeMap> {
        Ok(self)
    }

    // Structs look just like maps in JSON. In particular, JSON requires that we
    // serialize the field names of the struct. Other formats may be able to
    // omit the field names when serializing structs because the corresponding
    // Deserialize implementation is required to know what the keys are without
    // looking at the serialized data.
    fn serialize_struct(
        self,
        _name: &'static str,
        len: usize,
    ) -> Result<Self::SerializeStruct> {
        self.serialize_map(Some(len))
    }

    // Struct variants are represented in JSON as `{ NAME: { K: V, ... } }`.
    // This is the externally tagged representation.
    fn serialize_struct_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStructVariant> {
        // self.output += "{";
        variant.serialize(&mut *self)?;
        // self.output += ":{";
        Ok(self)
    }
}

// The following 7 impls deal with the serialization of compound types like
// sequences and maps. Serialization of such types is begun by a Serializer
// method and followed by zero or more calls to serialize individual elements of
// the compound type and one call to end the compound type.
//
// This impl is SerializeSeq so these methods are called after `serialize_seq`
// is called on the Serializer.
impl<'a> ser::SerializeSeq for &'a mut Serializer {
    // Must match the `Ok` type of the serializer.
    type Ok = ();
    // Must match the `Error` type of the serializer.
    type Error = Error;

    // Serialize a single element of the sequence.
    fn serialize_element<T>(&mut self, value: &T) -> Result<()>
        where
            T: ?Sized + Serialize,
    {
        value.serialize(&mut **self)
    }

    // Close the sequence.
    fn end(self) -> Result<()> {
        self.push_state(State::ReadingItem);
        Ok(())
    }
}

// Same thing but for tuples.
impl<'a> ser::SerializeTuple for &'a mut Serializer {
    type Ok = ();
    type Error = Error;

    fn serialize_element<T>(&mut self, value: &T) -> Result<()>
        where
            T: ?Sized + Serialize,
    {
        value.serialize(&mut **self)
    }

    fn end(self) -> Result<()> {
        self.drain_stack();
        Ok(())
    }
}

// Same thing but for tuple structs.
impl<'a> ser::SerializeTupleStruct for &'a mut Serializer {
    type Ok = ();
    type Error = Error;

    fn serialize_field<T>(&mut self, value: &T) -> Result<()>
        where
            T: ?Sized + Serialize,
    {
        value.serialize(&mut **self)
    }

    fn end(self) -> Result<()> {
        self.drain_stack();
        Ok(())
    }
}

// Tuple variants are a little different. Refer back to the
// `serialize_tuple_variant` method above:
//
//    self.output += "{";
//    variant.serialize(&mut *self)?;
//    self.output += ":[";
//
// So the `end` method in this impl is responsible for closing both the `]` and
// the `}`.
impl<'a> ser::SerializeTupleVariant for &'a mut Serializer {
    type Ok = ();
    type Error = Error;

    fn serialize_field<T>(&mut self, value: &T) -> Result<()>
        where
            T: ?Sized + Serialize,
    {
        value.serialize(&mut **self)
    }

    fn end(self) -> Result<()> {
        self.drain_stack();
        Ok(())
    }
}

// Some `Serialize` types are not able to hold a key and value in memory at the
// same time so `SerializeMap` implementations are required to support
// `serialize_key` and `serialize_value` individually.
//
// There is a third optional method on the `SerializeMap` trait. The
// `serialize_entry` method allows serializers to optimize for the case where
// key and value are both available simultaneously. In JSON it doesn't make a
// difference so the default behavior for `serialize_entry` is fine.
impl<'a> ser::SerializeMap for &'a mut Serializer {
    type Ok = ();
    type Error = Error;

    // The Serde data model allows map keys to be any serializable type. JSON
    // only allows string keys so the implementation below will produce invalid
    // JSON if the key serializes as something other than a string.
    //
    // A real JSON serializer would need to validate that map keys are strings.
    // This can be done by using a different Serializer to serialize the key
    // (instead of `&mut **self`) and having that other serializer only
    // implement `serialize_str` and return an error on any other data type.
    fn serialize_key<T>(&mut self, key: &T) -> Result<()>
        where
            T: ?Sized + Serialize,
    {
        key.serialize(&mut **self)
    }

    // It doesn't make a difference whether the colon is printed at the end of
    // `serialize_key` or at the beginning of `serialize_value`. In this case
    // the code is a bit simpler having it here.
    fn serialize_value<T>(&mut self, value: &T) -> Result<()>
        where
            T: ?Sized + Serialize,
    {
        value.serialize(&mut **self)
    }

    fn end(self) -> Result<()> {
        self.drain_stack();
        Ok(())
    }
}

// Structs are like maps in which the keys are constrained to be compile-time
// constant strings.
impl<'a> ser::SerializeStruct for &'a mut Serializer {
    type Ok = ();
    type Error = Error;

    //noinspection DuplicatedCode
    fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<()>
        where
            T: ?Sized + Serialize,
    {
        self.push_attr(key);
        value.serialize(&mut **self)
    }

    fn end(self) -> Result<()> {
        self.drain_stack();

        Ok(())
    }
}

// Similar to `SerializeTupleVariant`, here the `end` method is responsible for
// closing both of the curly braces opened by `serialize_struct_variant`.
impl<'a> ser::SerializeStructVariant for &'a mut Serializer {
    type Ok = ();
    type Error = Error;

    //noinspection DuplicatedCode
    fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<()>
        where
            T: ?Sized + Serialize,
    {
        self.push_attr(key);
        value.serialize(&mut **self)
    }

    fn end(self) -> Result<()> {
        self.drain_stack();

        Ok(())
    }
}

#[test]
fn simple_struct() {
    #[derive(Serialize)]
    struct Test {
        a: u32,
        b: f32,
        c: i8,
        d: String,
    }

    let test = Test {
        a: 1,
        b: 2.0,
        c: 3,
        d: String::from("hello"),
    };

    let parsed_value = to_string(&test).unwrap();
    let expected = Value::Record(Vec::new(), vec![
        Item::Slot(Value::Text(String::from("a")), Value::Int32Value(1)),
        Item::Slot(Value::Text(String::from("b")), Value::Float64Value(2.0)),
        Item::Slot(Value::Text(String::from("c")), Value::Int32Value(3)),
        Item::Slot(Value::Text(String::from("d")), Value::Text(String::from("hello"))),
    ]);

    assert_eq!(parsed_value, expected);
}

#[test]
fn struct_with_vec() {
    #[derive(Serialize)]
    struct Test {
        int: u32,
        seq: Vec<&'static str>,
    }

    let test = Test {
        int: 1,
        seq: vec!["a", "b"],
    };

    let parsed_value = to_string(&test).unwrap();
    let expected = Value::Record(Vec::new(), vec![
        Item::Slot(Value::Text(String::from("int")), Value::Int32Value(1)),
        Item::Slot(Value::Text(String::from("seq")), Value::Record(Vec::new(), vec![
            ValueItem(Value::Text(String::from("a"))),
            ValueItem(Value::Text(String::from("b"))),
        ])),
    ]);

    assert_eq!(parsed_value, expected);
}

#[test]
fn vec_of_vecs() {
    #[derive(Serialize)]
    struct Test {
        seq: Vec<Vec<&'static str>>,
    }

    let test = Test {
        seq: vec![
            vec!["a", "b", "c"], //v1
            vec!["c", "d"] //v2
        ],
    };

    let parsed_value = to_string(&test).unwrap();
    let expected = Value::Record(Vec::new(), vec![
        Item::Slot(Value::Text(String::from("seq")), Value::Record(Vec::new(), vec![
            Item::ValueItem(Value::Record(Vec::new(), vec![
                Item::ValueItem(Value::Text(String::from("a"))),
                Item::ValueItem(Value::Text(String::from("b"))),
            ])),
            Item::ValueItem(Value::Record(Vec::new(), vec![
                Item::ValueItem(Value::Text(String::from("c"))),
                Item::ValueItem(Value::Text(String::from("d"))),
            ]))
        ]))]);

    assert_eq!(parsed_value, expected);
}