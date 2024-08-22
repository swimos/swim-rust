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

pub trait Selector {
    type Value;

    fn name(&self) -> String;

    fn select<'a>(&self, value: &'a Self::Value) -> Option<&'a Self::Value>;

    fn select_owned(&self, value: Self::Value) -> Option<Self::Value>;
}

pub enum CompiledSelector<F> {
    Chain(ChainSelector<F>),
}

impl<F> CompiledSelector<F> {
    pub fn chain<I>(selectors: I) -> CompiledSelector<F>
    where
        I: Iterator<Item = F>,
    {
        CompiledSelector::Chain(ChainSelector {
            selectors: selectors.collect(),
        })
    }
}

impl<F, V> Selector for CompiledSelector<F>
where
    F: Selector<Value = V>,
{
    type Value = V;

    fn name(&self) -> String {
        match self {
            CompiledSelector::Chain(s) => s.name(),
        }
    }

    fn select<'a>(&self, value: &'a Self::Value) -> Option<&'a Self::Value> {
        match self {
            CompiledSelector::Chain(s) => s.select(value),
        }
    }

    fn select_owned(&self, value: Self::Value) -> Option<Self::Value> {
        match self {
            CompiledSelector::Chain(s) => s.select_owned(value),
        }
    }
}

pub struct ChainSelector<F> {
    selectors: Vec<F>,
}

impl<F, V> Selector for ChainSelector<F>
where
    F: Selector<Value = V>,
{
    type Value = V;

    fn name(&self) -> String {
        self.selectors.iter().fold(String::new(), |mut stack, s| {
            stack.push_str(format!(".{}", s.name()).as_str());
            stack
        })
    }

    fn select<'a>(&self, value: &'a Self::Value) -> Option<&'a Self::Value> {
        self.selectors.iter().try_fold(value, |v, s| s.select(v))
    }

    fn select_owned<'a>(&self, value: Self::Value) -> Option<Self::Value> {
        self.selectors
            .iter()
            .try_fold(value, |v, s| s.select_owned(v))
    }
}
