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

use crate::form::{Form, FormErr};
use crate::model::Value;

pub trait HList: Sized {
    const LEN: usize;

    fn len(&self) -> usize {
        Self::LEN
    }

    fn is_empty(&self) -> bool {
        Self::LEN == 0
    }

    fn prepend<H>(self, h: H) -> HCons<H, Self> {
        HCons {
            head: h,
            tail: self,
        }
    }
}

#[derive(PartialEq, Debug, Eq, Hash, Copy, Clone)]
pub struct HCons<H, T> {
    pub head: H,
    pub tail: T,
}

impl<H, T> HList for HCons<H, T>
where
    T: HList,
{
    const LEN: usize = 1 + T::LEN;

    fn len(&self) -> usize {
        Self::LEN
    }
}

#[derive(PartialEq, Debug, Eq, Hash, Copy, Clone)]
pub struct HNil;

impl HList for HNil {
    const LEN: usize = 0;

    fn len(&self) -> usize {
        Self::LEN
    }
}

impl Form for HNil {
    fn as_value(&self) -> Value {
        Value::empty_record()
    }

    fn try_from_value(_value: &Value) -> Result<Self, FormErr> {
        unimplemented!()
    }
}
