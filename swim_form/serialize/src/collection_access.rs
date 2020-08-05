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

use crate::{FormSerializeErr, SerializerResult, SerializerState, ValueSerializer};
use serde::ser::{SerializeMap, SerializeSeq};
use serde::Serialize;

impl<'a> SerializeSeq for &'a mut ValueSerializer {
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

impl<'a> SerializeMap for &'a mut ValueSerializer {
    type Ok = ();
    type Error = FormSerializeErr;

    fn serialize_key<T>(&mut self, key: &T) -> SerializerResult<()>
    where
        T: ?Sized + Serialize,
    {
        if let SerializerState::ReadingMap(ref mut b) = self.current_state.serializer_state {
            *b = true;
        } else {
            panic!("Illegal state")
        }
        key.serialize(&mut **self)
    }

    fn serialize_value<T>(&mut self, value: &T) -> SerializerResult<()>
    where
        T: ?Sized + Serialize,
    {
        if let SerializerState::ReadingMap(ref mut b) = self.current_state.serializer_state {
            *b = false;
        } else {
            panic!("Illegal state")
        }

        value.serialize(&mut **self)
    }

    fn end(self) -> SerializerResult<()> {
        self.exit_nested();
        Ok(())
    }
}
