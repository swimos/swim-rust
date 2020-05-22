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

use std::cmp::Ordering;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::ops::Deref;

pub trait Addressed {

    type Referrent: ?Sized;

    fn addr(&self) -> *const Self::Referrent;

}

impl<T: Deref> Addressed for T {
    type Referrent = T::Target;

    fn addr(&self) -> *const Self::Referrent {
        self.deref() as *const Self::Referrent
    }
}


pub struct PtrKey<A>(pub A);

impl<A: Addressed> Addressed for PtrKey<A> {
    type Referrent = A::Referrent;

    fn addr(&self) -> *const Self::Referrent {
        let PtrKey(inner) = self;
        inner.addr()
    }
}


impl<A: Addressed> PartialEq for PtrKey<A> {
    fn eq(&self, other: &Self) -> bool {
        self.addr().eq(&other.addr())
    }
}

impl<A: Addressed> Eq for PtrKey<A> {}

impl<A: Addressed> PartialOrd for PtrKey<A> {

    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.addr().partial_cmp(&other.addr())
    }

}

impl<A: Addressed> Ord for PtrKey<A> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.addr().cmp(&other.addr())
    }
}

impl<A: Addressed> Hash for PtrKey<A> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.addr().hash(state)
    }
}