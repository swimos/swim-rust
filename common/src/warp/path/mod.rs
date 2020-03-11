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

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Hash)]
pub struct AbsolutePath {
    pub host: String,
    pub node: String,
    pub lane: String,
}

impl AbsolutePath {

    pub fn new(host: &str, node: &str, lane: &str) -> AbsolutePath {
        AbsolutePath {
            host: host.to_string(),
            node: node.to_string(),
            lane: lane.to_string(),
        }
    }

    pub fn split(self) -> (String, RelativePath) {
        let AbsolutePath {
            host, node, lane
        } = self;
        (host, RelativePath { node, lane })
    }

}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Hash)]
pub struct RelativePath {
    pub node: String,
    pub lane: String,
}

impl RelativePath {
    pub fn new(node: &str, lane: &str) -> RelativePath {
        RelativePath {
            node: node.to_string(),
            lane: lane.to_string(),
        }
    }

    pub fn for_host(self, host: &str) -> AbsolutePath {
        let RelativePath { node, lane} = self;
        AbsolutePath {
            host: host.to_string(),
            node,
            lane,
        }
    }
}