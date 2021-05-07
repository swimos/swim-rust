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

use crate::engines::keyspaces::KeyspaceResolver;
use crate::StoreError;
use swim_common::model::text::Text;

pub trait OwnedEngineRefIterator: for<'t> EngineRefIterator<'t, 't> {}
impl<D> OwnedEngineRefIterator for D where D: for<'t> EngineRefIterator<'t, 't> {}

pub enum IteratorKey {
    Start,
    End,
    ToKey(Text),
}

pub trait EngineIterator {
    fn seek_first(&mut self) -> Result<bool, StoreError> {
        self.seek_to(IteratorKey::Start)
    }

    fn seek_end(&mut self) -> Result<bool, StoreError> {
        self.seek_to(IteratorKey::End)
    }

    fn seek_to(&mut self, key: IteratorKey) -> Result<bool, StoreError>;

    fn seek_next(&mut self) -> Result<bool, StoreError>;

    fn key(&self) -> Option<&[u8]>;

    fn value(&self) -> Option<&[u8]>;

    fn valid(&self) -> Result<bool, StoreError>;
}

pub trait EnginePrefixIterator {
    fn seek_next(&mut self) -> Result<bool, StoreError>;

    fn key(&mut self) -> Option<&[u8]>;

    fn value(&self) -> Option<&[u8]>;

    fn valid(&self) -> Result<bool, StoreError>;
}

// todo
#[derive(Default)]
pub struct EngineIterOpts;

pub trait EngineRefIterator<'a: 'b, 'b>: KeyspaceResolver {
    type EngineIterator: EngineIterator;
    type EnginePrefixIterator: EnginePrefixIterator;

    fn iterator(
        &'a self,
        space: &'b Self::ResolvedKeyspace,
    ) -> Result<Self::EngineIterator, StoreError> {
        self.iterator_opt(space, EngineIterOpts::default())
    }

    fn iterator_opt(
        &'a self,
        space: &'b Self::ResolvedKeyspace,
        opts: EngineIterOpts,
    ) -> Result<Self::EngineIterator, StoreError>;

    fn prefix_iterator(
        &'a self,
        space: &'b Self::ResolvedKeyspace,
        prefix: Text,
    ) -> Result<Self::EnginePrefixIterator, StoreError> {
        self.prefix_iterator_opt(space, EngineIterOpts::default(), prefix)
    }

    fn prefix_iterator_opt(
        &'a self,
        space: &'b Self::ResolvedKeyspace,
        opts: EngineIterOpts,
        prefix: Text,
    ) -> Result<Self::EnginePrefixIterator, StoreError>;
}
