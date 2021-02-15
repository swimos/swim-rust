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

#[cfg(test)]
mod tests;

use crate::downlink::typed::UntypedCommandDownlink;
use crate::downlink::{Downlink, DownlinkError};
use std::any::type_name;
use std::cmp::Ordering;
use std::error::Error;
use std::fmt::{Debug, Display, Formatter};
use std::marker::PhantomData;
use std::sync::Arc;
use swim_common::form::{Form, ValidatedForm};
use swim_common::model::schema::StandardSchema;
use utilities::sync::promise;

/// A downlink that sends commands to a remote downlink and does not link to the remote lane.
pub struct TypedCommandDownlink<T> {
    inner: Arc<UntypedCommandDownlink>,
    _type: PhantomData<fn(T)>,
}

impl<T> TypedCommandDownlink<T> {
    pub(crate) fn new(inner: Arc<UntypedCommandDownlink>) -> Self {
        TypedCommandDownlink {
            inner,
            _type: PhantomData,
        }
    }
}

impl<T> Debug for TypedCommandDownlink<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TypedCommandDownlink")
            .field("inner", &self.inner)
            .field("type", &type_name::<T>())
            .finish()
    }
}

impl<T> Clone for TypedCommandDownlink<T> {
    fn clone(&self) -> Self {
        TypedCommandDownlink {
            inner: self.inner.clone(),
            _type: PhantomData,
        }
    }
}

impl<T> Downlink for TypedCommandDownlink<T> {
    fn is_stopped(&self) -> bool {
        self.inner.is_stopped()
    }

    fn await_stopped(&self) -> promise::Receiver<Result<(), DownlinkError>> {
        self.inner.await_stopped()
    }

    fn same_downlink(left: &Self, right: &Self) -> bool {
        Arc::ptr_eq(&left.inner, &right.inner)
    }
}

impl<T: Form> TypedCommandDownlink<T> {
    /// Send a command to the remote lane.
    pub async fn command(&self, command: T) -> Result<(), DownlinkError> {
        Ok(self.inner.send(command.into_value()).await?)
    }
}

/// Error type returned when creating a view
/// for a command downlink with incompatible type.
#[derive(Debug, Clone)]
pub struct CommandViewError {
    // A validation schema for the type of the original value downlink.
    existing: StandardSchema,
    // A validation schema for the type of the requested view.
    requested: StandardSchema,
}

impl Display for CommandViewError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "A Write Only view of a command downlink with schema {} was requested but the original command downlink is running with schema {}.", self.requested, self.existing)
    }
}

impl Error for CommandViewError {}

impl<T: ValidatedForm> TypedCommandDownlink<T> {
    /// Create a sender for a more refined type (the [`ValidatedForm`] implementation for `U`
    /// will always produce a [`swim_common::model::Value`] that is acceptable to the [`ValidatedForm`] implementation
    /// for `T`) to the downlink.
    pub fn contravariant_view<U: ValidatedForm>(
        &self,
    ) -> Result<TypedCommandDownlink<U>, CommandViewError> {
        let schema_cmp = U::schema().partial_cmp(&T::schema());

        if schema_cmp.is_some() && schema_cmp != Some(Ordering::Less) {
            Ok(TypedCommandDownlink::new(self.inner.clone()))
        } else {
            Err(CommandViewError {
                existing: T::schema(),
                requested: U::schema(),
            })
        }
    }
}
