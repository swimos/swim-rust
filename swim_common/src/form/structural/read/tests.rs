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

use crate::form::structural::read::builder::{Builder, NoAttributes, Wrapped};
use crate::form::structural::read::{
    BodyReader, HeaderReader, ReadError, StructuralReadable, ValueReadable,
};
use crate::model::text::Text;
use crate::model::ValueKind;
use either::Either;
use num_bigint::{BigInt, BigUint};
use std::borrow::Cow;
use std::sync::Arc;

#[test]
fn read_unit() {
    assert_eq!(<() as ValueReadable>::read_extant(), Ok(()));
}

#[test]
fn read_i32() {
    assert_eq!(i32::read_i32(3), Ok(3));
}

#[test]
fn read_i64() {
    assert_eq!(i64::read_i64(3), Ok(3));
}

#[test]
fn read_u32() {
    assert_eq!(u32::read_u32(3), Ok(3));
}

#[test]
fn read_u64() {
    assert_eq!(u64::read_u64(3), Ok(3));
}

#[test]
fn read_bool() {
    assert_eq!(bool::read_bool(true), Ok(true));
}

#[test]
fn read_big_int() {
    assert_eq!(BigInt::read_big_int(BigInt::from(3)), Ok(BigInt::from(3)));
}

#[test]
fn read_big_uint() {
    assert_eq!(
        BigUint::read_big_uint(BigUint::from(3u32)),
        Ok(BigUint::from(3u32))
    );
}

#[test]
fn read_float() {
    assert!(f64::read_f64(1.5).eq(&Ok(1.5)));
}

#[test]
fn read_string() {
    assert_eq!(
        String::read_text(Cow::Borrowed("hello")),
        Ok(String::from("hello"))
    );
}

#[test]
fn read_text() {
    assert_eq!(
        Text::read_text(Cow::Borrowed("hello")),
        Ok(Text::from("hello"))
    );
}

#[test]
fn read_blob_vec() {
    let result = <Vec<u8> as ValueReadable>::read_blob(vec![1u8, 2u8, 3u8]);
    assert_eq!(result, Ok(vec![1u8, 2u8, 3u8]))
}

#[test]
fn read_blob_slice() {
    let result = <Box<[u8]> as ValueReadable>::read_blob(vec![1u8, 2u8, 3u8]);
    assert_eq!(result, Ok(vec![1u8, 2u8, 3u8].into_boxed_slice()))
}

#[test]
fn read_arc() {
    let result = <Arc<i32> as ValueReadable>::read_i32(3);
    assert_eq!(result, Ok(Arc::new(3)));
}

#[test]
fn read_optional_prim() {
    let result = <Option<i32> as ValueReadable>::read_i32(3);
    assert_eq!(result, Ok(Some(3)));

    let result = <Option<i32> as ValueReadable>::read_extant();
    assert_eq!(result, Ok(None));
}

#[test]
fn read_prim_vec() {
    let mut reader = <Vec<i32> as StructuralReadable>::make_reader()
        .unwrap()
        .start_body()
        .unwrap();
    assert!(reader.push_i32(1).is_ok());
    assert!(reader.push_i32(2).is_ok());
    assert!(reader.push_i32(3).is_ok());
    let result = <Vec<i32> as StructuralReadable>::try_terminate(reader);
    assert_eq!(result, Ok(vec![1, 2, 3]));
}

#[test]
fn read_error_display() {
    let string = ReadError::UnexpectedField(Text::new("name")).to_string();
    assert_eq!(string, "Unexpected field: 'name'");

    let string = ReadError::DuplicateField(Text::new("name")).to_string();
    assert_eq!(string, "Field 'name' ocurred more than once.");

    let string = ReadError::UnexpectedSlot.to_string();
    assert_eq!(string, "Unexpected slot in record.");

    let string = ReadError::InconsistentState.to_string();
    assert_eq!(string, "The deserialization state became corrupted.");

    let string = ReadError::UnexpectedAttribute(Text::new("name")).to_string();
    assert_eq!(string, "Unexpected attribute: 'name'");

    let string =
        ReadError::MissingFields(vec![Text::new("first"), Text::new("second")]).to_string();
    assert_eq!(string, "Fields [first, second] are required.");

    let string = ReadError::IncompleteRecord.to_string();
    assert_eq!(
        string,
        "The record ended before all parts of the value were deserialized."
    );

    let string = ReadError::ReaderOverflow.to_string();
    assert_eq!(string, "Record more deeply nested than expected for type.");

    let string = ReadError::DoubleSlot.to_string();
    assert_eq!(
        string,
        "Slot divider encountered within the value of a slot."
    );

    let string = ReadError::ReaderUnderflow.to_string();
    assert_eq!(string, "Stack undeflow deserializing the value.");

    let string = ReadError::UnexpectedKind(ValueKind::Text).to_string();
    assert_eq!(string, "Unexpected value kind: Text");
}

struct GeneralType<S, T> {
    first: S,
    second: T,
}

type GeneralTypeFields<S, T> = (
    Option<S>,
    Option<T>,
    Option<<S as StructuralReadable>::Reader>,
    Option<<T as StructuralReadable>::Reader>,
);

impl<S: StructuralReadable, T: StructuralReadable> ValueReadable for GeneralType<S, T> {}

impl<S: StructuralReadable, T: StructuralReadable> StructuralReadable for GeneralType<S, T> {
    type Reader = Builder<GeneralType<S, T>, GeneralTypeFields<S, T>>;

    fn make_reader() -> Result<Self::Reader, ReadError> {
        Ok(Builder::default())
    }

    fn try_terminate(reader: <Self::Reader as HeaderReader>::Body) -> Result<Self, ReadError> {
        let Builder { state, .. } = reader;
        match state {
            (Some(first), Some(second), ..) => Ok(GeneralType { first, second }),
            (_, Some(second), ..) => {
                if let Some(first) = S::on_absent() {
                    Ok(GeneralType { first, second })
                } else {
                    Err(ReadError::MissingFields(vec![Text::new("first")]))
                }
            }
            (Some(first), ..) => {
                if let Some(second) = T::on_absent() {
                    Ok(GeneralType { first, second })
                } else {
                    Err(ReadError::MissingFields(vec![Text::new("second")]))
                }
            }
            _ => S::on_absent()
                .and_then(|first| T::on_absent().map(|second| GeneralType { first, second }))
                .ok_or_else(|| {
                    ReadError::MissingFields(vec![Text::new("first"), Text::new("second")])
                }),
        }
    }
}

impl<S, T> NoAttributes for Builder<GeneralType<S, T>, GeneralTypeFields<S, T>>
where
    S: StructuralReadable,
    T: StructuralReadable,
{
}

impl<S: StructuralReadable, T: StructuralReadable>
    Builder<GeneralType<S, T>, GeneralTypeFields<S, T>>
{
    fn push_prim<V, F1, F2>(
        &mut self,
        f1: F1,
        f2: F2,
        value: V,
        kind: ValueKind,
    ) -> Result<bool, ReadError>
    where
        F1: FnOnce(V) -> Result<S, ReadError>,
        F2: FnOnce(V) -> Result<T, ReadError>,
    {
        let (first, second, _, _) = &mut self.state;

        if self.reading_slot {
            self.reading_slot = false;
            match self.current_field.take() {
                Some(0) => {
                    let value = f1(value)?;
                    if first.is_some() {
                        Err(ReadError::DuplicateField(Text::new("first")))
                    } else {
                        *first = Some(value);
                        Ok(second.is_none())
                    }
                }
                Some(1) => {
                    let value = f2(value)?;
                    if second.is_some() {
                        Err(ReadError::DuplicateField(Text::new("second")))
                    } else {
                        *second = Some(value);
                        Ok(first.is_none())
                    }
                }
                Some(_) => Err(ReadError::UnexpectedSlot),
                _ => Err(ReadError::UnexpectedKind(ValueKind::Extant)),
            }
        } else {
            Err(ReadError::UnexpectedKind(kind))
        }
    }
}

impl<S, T> BodyReader for Builder<GeneralType<S, T>, GeneralTypeFields<S, T>>
where
    S: StructuralReadable,
    T: StructuralReadable,
{
    type Delegate = Either<Wrapped<Self, S::Reader>, Wrapped<Self, T::Reader>>;

    fn push_extant(&mut self) -> Result<bool, ReadError> {
        let (first, second, _, _) = &mut self.state;

        if self.reading_slot {
            self.reading_slot = false;
            match self.current_field.take() {
                Some(0) => {
                    let value = S::read_extant()?;
                    if first.is_some() {
                        Err(ReadError::DuplicateField(Text::new("first")))
                    } else {
                        *first = Some(value);
                        Ok(second.is_none())
                    }
                }
                Some(1) => {
                    let value = T::read_extant()?;
                    if second.is_some() {
                        Err(ReadError::DuplicateField(Text::new("second")))
                    } else {
                        *second = Some(value);
                        Ok(first.is_none())
                    }
                }
                Some(_) => Err(ReadError::UnexpectedSlot),
                _ => Err(ReadError::UnexpectedKind(ValueKind::Extant)),
            }
        } else {
            Err(ReadError::UnexpectedKind(ValueKind::Extant))
        }
    }

    fn push_i32(&mut self, value: i32) -> Result<bool, ReadError> {
        self.push_prim(S::read_i32, T::read_i32, value, ValueKind::Int32)
    }

    fn push_i64(&mut self, value: i64) -> Result<bool, ReadError> {
        self.push_prim(S::read_i64, T::read_i64, value, ValueKind::Int64)
    }

    fn push_u32(&mut self, value: u32) -> Result<bool, ReadError> {
        self.push_prim(S::read_u32, T::read_u32, value, ValueKind::UInt32)
    }

    fn push_u64(&mut self, value: u64) -> Result<bool, ReadError> {
        self.push_prim(S::read_u64, T::read_u64, value, ValueKind::UInt64)
    }

    fn push_f64(&mut self, value: f64) -> Result<bool, ReadError> {
        self.push_prim(S::read_f64, T::read_f64, value, ValueKind::Float64)
    }

    fn push_bool(&mut self, value: bool) -> Result<bool, ReadError> {
        self.push_prim(S::read_bool, T::read_bool, value, ValueKind::Boolean)
    }

    fn push_big_int(&mut self, value: BigInt) -> Result<bool, ReadError> {
        self.push_prim(S::read_big_int, T::read_big_int, value, ValueKind::BigInt)
    }

    fn push_big_uint(&mut self, value: BigUint) -> Result<bool, ReadError> {
        self.push_prim(
            S::read_big_uint,
            T::read_big_uint,
            value,
            ValueKind::BigUint,
        )
    }

    fn push_text<'a>(&mut self, value: Cow<'a, str>) -> Result<bool, ReadError> {
        let (first, second, _, _) = &mut self.state;

        if self.reading_slot {
            self.reading_slot = false;
            match self.current_field.take() {
                Some(0) => {
                    let value = S::read_text(value)?;
                    if first.is_some() {
                        Err(ReadError::DuplicateField(Text::new("first")))
                    } else {
                        *first = Some(value);
                        Ok(second.is_none())
                    }
                }
                Some(1) => {
                    let value = T::read_text(value)?;
                    if second.is_some() {
                        Err(ReadError::DuplicateField(Text::new("second")))
                    } else {
                        *second = Some(value);
                        Ok(first.is_none())
                    }
                }
                _ => Err(ReadError::UnexpectedSlot),
            }
        } else {
            match value.as_ref() {
                "first" => {
                    self.current_field = Some(0);
                    Ok(first.is_none())
                }
                "second" => {
                    self.current_field = Some(0);
                    Ok(second.is_none())
                }
                _ => Err(ReadError::UnexpectedKind(ValueKind::Text)),
            }
        }
    }

    fn push_blob(&mut self, value: Vec<u8>) -> Result<bool, ReadError> {
        self.push_prim(S::read_blob, T::read_blob, value, ValueKind::Data)
    }

    fn start_slot(&mut self) -> Result<(), ReadError> {
        self.reading_slot = true;
        Ok(())
    }

    fn push_record(self) -> Result<Self::Delegate, ReadError> {
        if self.reading_slot {
            match &self.current_field {
                Some(0) => Ok(Either::Left(Wrapped {
                    payload: self,
                    reader: S::make_reader()?,
                })),
                Some(1) => Ok(Either::Right(Wrapped {
                    payload: self,
                    reader: T::make_reader()?,
                })),
                _ => Err(ReadError::UnexpectedSlot),
            }
        } else {
            Err(ReadError::UnexpectedKind(ValueKind::Record))
        }
    }

    fn restore(delegate: <Self::Delegate as HeaderReader>::Body) -> Result<Self, ReadError> {
        match delegate {
            Either::Left(Wrapped {
                mut payload,
                reader,
            }) => {
                let (first, _, _, _) = &mut payload.state;
                *first = Some(S::try_terminate(reader)?);
                Ok(payload)
            }
            Either::Right(Wrapped {
                mut payload,
                reader,
            }) => {
                let (_, second, _, _) = &mut payload.state;
                *second = Some(T::try_terminate(reader)?);
                Ok(payload)
            }
        }
    }
}
