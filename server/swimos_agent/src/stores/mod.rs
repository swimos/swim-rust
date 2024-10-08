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

use bytes::BytesMut;

use crate::{agent_model::WriteResult, item::AgentItem};

#[doc(hidden)]
pub mod map;
#[doc(hidden)]
pub mod value;

#[doc(inline)]
pub use self::{map::MapStore, value::ValueStore};

/// Base trait for all agent items that model stores (are not directly exposed outside the agent).
pub trait StoreItem: AgentItem {
    /// If the state of the store has changed, write an event into the buffer.
    fn write_to_buffer(&self, buffer: &mut BytesMut) -> WriteResult;
}
