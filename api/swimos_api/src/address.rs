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

//! Types to describe the addresses of local and remote lanes.

use std::fmt::{Debug, Display};

use swimos_form::Form;
use swimos_utilities::encoding::BytesStr;

use swimos_model::Text;

/// An address of a Swim lane, omitting the host to which it belongs.
#[derive(Default, Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RelativeAddress<T> {
    /// The node URI of the agent that contains the lane.
    pub node: T,
    /// The name of the lane.
    pub lane: T,
}

/// A fully qualified address of a Swim lane.
#[derive(Default, Debug, Clone, Copy, PartialEq, Eq, Hash, Form)]
pub struct Address<T> {
    /// The host at which the lane can be found. If absent this will be inferred from the routing mesh.
    pub host: Option<T>,
    /// The node URI of the agent that contains the lane.
    pub node: T,
    /// The name of the lane.
    pub lane: T,
}

impl<T> Address<T> {
    pub fn borrow_parts<R: ?Sized>(&self) -> Address<&R>
    where
        T: AsRef<R>,
    {
        let Address { host, node, lane } = self;
        Address {
            host: host.as_ref().map(AsRef::as_ref),
            node: node.as_ref(),
            lane: lane.as_ref(),
        }
    }
}

impl Address<&str> {
    pub fn owned(&self) -> Address<String> {
        let Address { host, node, lane } = self;
        Address {
            host: host.as_ref().map(|s| s.to_string()),
            node: node.to_string(),
            lane: lane.to_string(),
        }
    }
}

impl<T: Display + Debug> Display for Address<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Address(host = {:?}, node = {}, lane = {})",
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

impl<T> RelativeAddress<T> {
    pub fn borrow_parts<R: ?Sized>(&self) -> RelativeAddress<&R>
    where
        T: AsRef<R>,
    {
        let RelativeAddress { node, lane } = self;
        RelativeAddress {
            node: node.as_ref(),
            lane: lane.as_ref(),
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

impl<S: AsRef<str>> Address<S> {
    pub fn to_text(&self) -> Address<Text> {
        let Address { host, node, lane } = self;
        Address::text(
            host.as_ref().map(AsRef::as_ref),
            node.as_ref(),
            lane.as_ref(),
        )
    }
}

impl PartialEq<RelativeAddress<&str>> for RelativeAddress<Text> {
    fn eq(&self, other: &RelativeAddress<&str>) -> bool {
        self.node.as_str() == other.node && self.lane.as_str() == other.lane
    }
}

impl PartialEq<Address<&str>> for Address<Text> {
    fn eq(&self, other: &Address<&str>) -> bool {
        self.host.as_ref().map(Text::as_str) == other.host
            && self.node.as_str() == other.node
            && self.lane.as_str() == other.lane
    }
}

impl PartialEq<Address<&str>> for Address<BytesStr> {
    fn eq(&self, other: &Address<&str>) -> bool {
        self.host.as_ref().map(BytesStr::as_ref) == other.host
            && self.node.as_str() == other.node
            && self.lane.as_str() == other.lane
    }
}

impl PartialEq<RelativeAddress<&str>> for RelativeAddress<BytesStr> {
    fn eq(&self, other: &RelativeAddress<&str>) -> bool {
        self.node.as_str() == other.node && self.lane.as_str() == other.lane
    }
}

impl PartialEq<Address<String>> for Address<Text> {
    fn eq(&self, other: &Address<String>) -> bool {
        self.borrow_parts::<str>() == other.borrow_parts()
    }
}

impl PartialEq<Address<Text>> for Address<String> {
    fn eq(&self, other: &Address<Text>) -> bool {
        self.borrow_parts::<str>() == other.borrow_parts()
    }
}

impl From<Address<Text>> for Address<String> {
    fn from(value: Address<Text>) -> Self {
        let Address { host, node, lane } = value;
        Address {
            host: host.map(Into::into),
            node: node.into(),
            lane: lane.into(),
        }
    }
}
