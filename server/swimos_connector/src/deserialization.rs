// Copyright 2015-2024 Swim Inc.
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

use std::error::Error;

#[derive(Debug, thiserror::Error)]
#[error(transparent)]
pub struct DeserializationError(Box<dyn Error + Send + 'static>);

impl DeserializationError {
    pub fn new<E>(err: E) -> DeserializationError
    where
        E: Error + Send + 'static,
    {
        DeserializationError(Box::new(err))
    }
}

pub trait Deferred {
    type Out;

    fn get(&mut self) -> Result<&Self::Out, DeserializationError>;

    fn take(self) -> Result<Self::Out, DeserializationError>;
}

pub struct Computed<F, O> {
    inner: Option<O>,
    f: F,
}

impl<F, O> Computed<F, O>
where
    F: Fn() -> Result<O, DeserializationError>,
{
    pub fn new(f: F) -> Self {
        Computed { inner: None, f }
    }
}

impl<F, O> Deferred for Computed<F, O>
where
    F: Fn() -> Result<O, DeserializationError>,
{
    type Out = O;

    fn get(&mut self) -> Result<&Self::Out, DeserializationError> {
        let Computed { inner, f } = self;
        if let Some(v) = inner {
            Ok(v)
        } else {
            Ok(inner.insert(f()?))
        }
    }

    fn take(self) -> Result<Self::Out, DeserializationError> {
        let Computed { inner, f } = self;
        if let Some(v) = inner {
            Ok(v)
        } else {
            Ok(f()?)
        }
    }
}
