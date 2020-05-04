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

use std::fmt::{Display, Formatter};
use url::ParseError;

/// Absolute path to an agent lane, on a specific host.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Hash)]
pub struct AbsolutePath {
    pub host: url::Url,
    pub node: String,
    pub lane: String,
}

impl AbsolutePath {
    pub fn new(host: &str, node: &str, lane: &str) -> Result<AbsolutePath, ParseError> {
        Ok(AbsolutePath {
            host: url::Url::parse(host)?,
            node: node.to_string(),
            lane: lane.to_string(),
        })
    }

    /// Split an absolute path into the host and relative components.
    ///
    /// # Examples
    /// ```
    /// use common::warp::path::*;
    ///
    /// let abs = AbsolutePath::new("ws://127.0.0.1/", "node", "lane").unwrap();
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

    /// Resolve a relative path against a host, producing an absolute path.
    ///
    /// # Examples
    /// ```
    /// use common::warp::path::*;
    ///
    /// let rel = RelativePath::new("node", "lane");
    ///
    /// assert_eq!(rel.for_host("host"), AbsolutePath::new("host", "node", "lane"))
    /// ```
    pub fn for_host(self, host: &str) -> Result<AbsolutePath, ParseError> {
        let RelativePath { node, lane } = self;
        Ok(AbsolutePath {
            host: url::Url::parse(host)?,
            node,
            lane,
        })
    }

    pub fn to_string(&self) -> String {
        format!("{}/{}", self.node, self.lane)
    }
}

impl Display for RelativePath {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "RelativePath[{}, {}]", self.node, self.lane)
    }
}
