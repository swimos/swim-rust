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

use crate::routing::RoutingAddr;

/// Iterator to generate a sequence of distinct remote routing addresses.
#[derive(Debug, Default)]
pub struct RemoteRoutingAddresses(u32);

impl Iterator for RemoteRoutingAddresses {
    type Item = RoutingAddr;

    fn next(&mut self) -> Option<Self::Item> {
        let RemoteRoutingAddresses(count) = self;
        let addr = RoutingAddr::remote(*count);
        if let Some(next) = count.checked_add(1) {
            *count = next;
            Some(addr)
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::routing::remote::addresses::RemoteRoutingAddresses;

    #[test]
    fn generate_addresses() {
        let mut addrs = RemoteRoutingAddresses::default();
        let first = addrs.next().unwrap();
        let second = addrs.next().unwrap();
        assert!(first.is_remote());
        assert!(second.is_remote());
        assert_ne!(first, second);
    }
}
