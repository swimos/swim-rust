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

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::marker::PhantomData;
use std::hash::{Hash, Hasher};
use std::fmt::{Debug, Formatter};

static COUNT: AtomicU64 = AtomicU64::new(0);

/// A transaction-local variable. This will start with a default value within each transaction
/// and the value within one transaction will have no effect on the value observed in any other.
/// Values set within a branch of a transaction that fails (by retry or abort) will be reset to
/// their state before that branch started.
pub struct TLocal<T> {
    index: u64,
    default: Arc<T>,
    _data_type: PhantomData<T>,
}

impl<T> TLocal<T> {

    /// The default value will be the value held by the variable at the start of every transaction
    /// that refers to it.
    pub fn new(default: T) -> Self {
        let index = COUNT.fetch_add(1, Ordering::Relaxed);
        if index == u64::max_value() {
            panic!("TLocal count overflowed.")
        }
        TLocal {
            index,
            default: Arc::new(default),
            _data_type: PhantomData,
        }
    }

    /// An [`Stm`] instance that will read the value of the variable.
    pub fn get(&self) -> TLocalRead<T> {
        TLocalRead(self.clone())
    }

    /// An ['Stm'] instance that will write the value of the variable.
    pub fn put(&self, value: T) -> TLocalWrite<T> {
        TLocalWrite(self.clone(), Arc::new(value))
    }

}

impl<T> Clone for TLocal<T> {
    fn clone(&self) -> Self {
        TLocal {
            index: self.index,
            default: self.default.clone(),
            _data_type: PhantomData,
        }
    }
}

impl<T> PartialEq for TLocal<T> {
    fn eq(&self, other: &Self) -> bool {
        self.index.eq(&other.index)
    }
}

impl<T> Eq for TLocal<T> {}

impl<T> Hash for TLocal<T> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.index.hash(state)
    }
}

impl<T: Debug> Debug for TLocal<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "TLocal(#{}, default = {:?})", self.index, self.default)
    }
}

/// [`Stm`] instance reading from a [`TLocal`].
#[derive(Debug, Clone)]
pub struct TLocalRead<T>(pub(crate) TLocal<T>);

/// [`Stm`] instance writing to a [`TLocal`].
#[derive(Debug, Clone)]
pub struct TLocalWrite<T>(pub(crate) TLocal<T>, pub(crate) Arc<T>);