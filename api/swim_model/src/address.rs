// Copyright 2015-2021 Swim Inc.
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

use std::fmt::{Debug, Display};

use crate::Text;

#[derive(Default, Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RelativeAddress<T> {
    pub node: T,
    pub lane: T,
}

#[derive(Default, Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Address<T> {
    pub host: Option<T>,
    pub node: T,
    pub lane: T,
}

impl<T: Display + Debug> Display for Address<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Address(host = {:?}, ode = {}, lane = {})",
            self.host, self.node, self.lane
        )
    }
}

impl<T: Display> Display for RelativeAddress<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "LocalAddress(node = {}, lane = {})",
            self.node, self.lane
        )
    }
}

impl<T> RelativeAddress<T> {
    pub fn new(node: T, lane: T) -> Self {
        RelativeAddress { node, lane }
    }
}

impl<T> From<RelativeAddress<T>> for Address<T> {
    fn from(addr: RelativeAddress<T>) -> Self {
        Address {
            host: None,
            node: addr.node,
            lane: addr.lane,
        }
    }
}

impl RelativeAddress<Text> {
    pub fn text(node: &str, lane: &str) -> Self {
        RelativeAddress::new(Text::new(node), Text::new(lane))
    }
}

impl<T> Address<T> {
    pub fn new(host: Option<T>, node: T, lane: T) -> Self {
        Address { host, node, lane }
    }

    pub fn local(node: T, lane: T) -> Self {
        Address {
            host: None,
            node,
            lane,
        }
    }
}

impl Address<Text> {
    pub fn text(host: Option<&str>, node: &str, lane: &str) -> Self {
        Address {
            host: host.map(Text::new),
            node: Text::new(node),
            lane: Text::new(lane),
        }
    }
}
