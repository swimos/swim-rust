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

use crate::{FormSerializeErr, SerializerResult, ValueSerializer};
use serde::ser::{
    SerializeStruct, SerializeStructVariant, SerializeTuple, SerializeTupleStruct,
    SerializeTupleVariant,
};
use serde::Serialize;

use crate::{FormSerializeErr, Result, ValueSerializer};

impl<'a> SerializeTuple for &'a mut ValueSerializer {
    type Ok = ();
    type Error = FormSerializeErr;

    fn serialize_element<T>(&mut self, value: &T) -> SerializerResult<()>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(&mut **self)
    }

    fn end(self) -> SerializerResult<()> {
        self.exit_nested();
        Ok(())
    }
}

impl<'a> SerializeTupleStruct for &'a mut ValueSerializer {
    type Ok = ();
    type Error = FormSerializeErr;

    fn serialize_field<T>(&mut self, value: &T) -> SerializerResult<()>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(&mut **self)
    }

    fn end(self) -> SerializerResult<()> {
        self.exit_nested();
        Ok(())
    }
}

impl<'a> SerializeTupleVariant for &'a mut ValueSerializer {
    type Ok = ();
    type Error = FormSerializeErr;

    fn serialize_field<T>(&mut self, value: &T) -> SerializerResult<()>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(&mut **self)
    }

    fn end(self) -> SerializerResult<()> {
        self.exit_nested();
        Ok(())
    }
}

impl<'a> SerializeStruct for &'a mut ValueSerializer {
    type Ok = ();
    type Error = FormSerializeErr;

    fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> SerializerResult<()>
    where
        T: ?Sized + Serialize,
    {
        self.current_state.attr_name = Some(key.to_owned());
        value.serialize(&mut **self)
    }

    fn end(self) -> SerializerResult<()> {
        self.exit_nested();

        Ok(())
    }
}

impl<'a> SerializeStructVariant for &'a mut ValueSerializer {
    type Ok = ();
    type Error = FormSerializeErr;

    fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> SerializerResult<()>
    where
        T: ?Sized + Serialize,
    {
        self.current_state.attr_name = Some(key.to_owned());
        value.serialize(&mut **self)
    }

    fn end(self) -> SerializerResult<()> {
        self.exit_nested();
        Ok(())
    }
}
