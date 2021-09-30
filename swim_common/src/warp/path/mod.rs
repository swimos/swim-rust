// Copyright 2015-2021 SWIM.AI inc.
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

use swim_model::Text;
use std::fmt::{Display, Formatter};

/// Absolute path to an agent lane, on a specific host.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Hash)]
pub struct AbsolutePath {
    pub host: url::Url,
    pub node: Text,
    pub lane: Text,
}

impl AbsolutePath {
    pub fn new(host: url::Url, node: &str, lane: &str) -> AbsolutePath {
        AbsolutePath {
            host,
            node: node.into(),
            lane: lane.into(),
        }
    }

    /// Split an absolute path into the host and relative components.
    ///
    /// # Examples
    /// ```
    /// use swim_common::warp::path::*;
    ///
    /// let abs = AbsolutePath::new(url::Url::parse("ws://127.0.0.1").unwrap(), "node", "lane");
    ///
    /// assert_eq!(abs.split(), (url::Url::parse("ws://127.0.0.1").unwrap(), RelativePath::new("node", "lane")));
    /// ```
    pub fn split(self) -> (url::Url, RelativePath) {
        let AbsolutePath { host, node, lane } = self;
        (host, RelativePath { node, lane })
    }

    pub fn relative_path(&self) -> RelativePath {
        RelativePath {
            node: self.node.clone(),
            lane: self.lane.clone(),
        }
    }
}

impl Display for AbsolutePath {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "AbsolutePath[{}, {}, {}]",
            self.host, self.node, self.lane
        )
    }
}

/// Relative path to an agent lane, leaving the host unspecified.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Hash)]
pub struct RelativePath {
    pub node: Text,
    pub lane: Text,
}

impl RelativePath {
    pub fn new<N, L>(node: N, lane: L) -> RelativePath
    where
        N: Into<Text>,
        L: Into<Text>,
    {
        RelativePath {
            node: node.into(),
            lane: lane.into(),
        }
    }

    /// Resolve a relative path against a host, producing an absolute path.
    ///
    /// # Examples
    /// ```
    /// use swim_common::warp::path::*;
    ///
    /// let rel = RelativePath::new("node", "lane");
    ///
    /// assert_eq!(rel.for_host(url::Url::parse("ws://127.0.0.1").unwrap()), AbsolutePath::new(url::Url::parse("ws://127.0.0.1").unwrap(), "node", "lane"))
    /// ```
    pub fn for_host(self, host: url::Url) -> AbsolutePath {
        let RelativePath { node, lane } = self;
        AbsolutePath { host, node, lane }
    }
}

impl Display for RelativePath {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "RelativePath[{}, {}]", self.node, self.lane)
    }
}
