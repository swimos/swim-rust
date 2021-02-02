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

use std::marker::PhantomData;
use std::sync::Arc;
use crate::downlink::improved::typed::{UntypedCommandDownlink, ViewMode};
use std::fmt::{Debug, Formatter};
use std::any::type_name;
use swim_common::form::{Form, ValidatedForm};
use utilities::sync::promise;
use crate::downlink::DownlinkError;
use swim_common::model::schema::StandardSchema;
use std::cmp::Ordering;

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
            _type: PhantomData
        }
    }
}

impl<T> TypedCommandDownlink<T> {

    pub fn is_stopped(&self) -> bool {
        self.inner.is_stopped()
    }

    /// Get a promise that will complete when the downlink stops running.
    pub fn await_stopped(&self) -> promise::Receiver<Result<(), DownlinkError>> {
        self.inner.await_stopped()
    }

}

impl<T: Form> TypedCommandDownlink<T> {

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
    // The mode of the view.
    mode: ViewMode,
}

impl<T: ValidatedForm> TypedCommandDownlink<T> {

    /// Create a read-only view for a value downlink that converts all received values to a new type.
    /// The type of the view must have an equal or greater schema than the original downlink.
    pub async fn contravariant_cast<U: ValidatedForm>(
        &self,
    ) -> Result<TypedCommandDownlink<U>, CommandViewError>
    {
        let schema_cmp = U::schema().partial_cmp(&T::schema());

        if schema_cmp.is_some() && schema_cmp != Some(Ordering::Greater) {
            Ok(TypedCommandDownlink::new(self.inner.clone()))
        } else {
            Err(CommandViewError {
                existing: T::schema(),
                requested: U::schema(),
                mode: ViewMode::ReadOnly,
            })
        }
    }

}