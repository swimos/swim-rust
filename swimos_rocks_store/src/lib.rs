// Copyright 2015-2023 Swim Inc.
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

mod agent;
pub(crate) mod engine;
mod plane;
mod server;

pub use agent::StoreWrapper;
pub use engine::*;
pub use plane::SwimPlaneStore;
pub use server::{
    rocks::{default_db_opts, default_keyspaces},
    ServerStore, SwimStore,
};

/// An enumeration over the keyspaces that exist in a store.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
enum KeyspaceName {
    Lane,
    Value,
    Map,
}
