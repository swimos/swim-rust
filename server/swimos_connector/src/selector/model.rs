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

use crate::selector::SelectorError;
use crate::{ConnectorAgent, DeserializationError};
use frunk::{hlist::HList, HCons, HNil};
use std::fmt::Debug;
use swimos_agent::{
    event_handler::HandlerAction,
    reexport::coproduct::{CNil, Coproduct},
};
use swimos_model::Value;

/// A value selector attempts to choose some sub-component of a [`Value`], matching against a
/// pattern, returning nothing if the pattern does not match.
pub trait ValueSelector: Debug {
    /// Attempt to select some sub-component of the provided [`Value`].
    fn select_value<'a>(&self, value: &'a Value) -> Option<&'a Value>;
}

/// A dynamic selector which attempts to choose some sub-component of a [`Value`] from some
/// arguments, matching against a pattern, returning nothing if the pattern does not match.
pub trait Selector<A> {
    /// Attempt to select some sub-component of the provided [`Value`] from the arguments this
    /// selector accepts.
    fn select(&self, from: &mut A) -> Result<Option<Value>, DeserializationError>;
}

impl<'a, L, R, Head, Tail> Selector<HCons<Head, Tail>> for Coproduct<L, R>
where
    L: Selector<Head>,
    R: Selector<Tail>,
    Tail: HList,
{
    fn select(&self, from: &mut HCons<Head, Tail>) -> Result<Option<Value>, DeserializationError> {
        match self {
            Coproduct::Inl(l) => l.select(&mut from.head),
            Coproduct::Inr(r) => r.select(&mut from.tail),
        }
    }
}

impl<'a> Selector<HNil> for CNil {
    fn select(&self, _from: &mut HNil) -> Result<Option<Value>, DeserializationError> {
        Ok(None)
    }
}

pub trait SelectHandler<A> {
    type Handler: HandlerAction<ConnectorAgent, Completion = ()> + 'static;

    fn select_handler(&self, args: &mut A) -> Result<Self::Handler, SelectorError>;
}
