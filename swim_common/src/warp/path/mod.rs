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

use crate::model::text::Text;
use std::fmt::{Debug, Display, Formatter};
use std::hash::Hash;
use url::Url;
use crate::routing::remote::BadUrl;
use crate::routing::remote::unpack_url;

pub trait Addressable:
    Clone + PartialEq + Eq + PartialOrd + Ord + Hash + Debug + Display + Send + Sync + 'static
{
    fn node(&self) -> Text;

    fn lane(&self) -> Text;

    fn relative_path(&self) -> RelativePath;

    fn host(&self) -> Option<Url>;
}

/// Wrapper around absolute and relative paths for addressing remote or local lanes respectively.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Hash)]
pub enum Path {
    Remote(AbsolutePath),
    Local(RelativePath),
}

impl Display for Path {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Path::Remote(abs_path) => write!(f, "{}", abs_path.to_string()),
            Path::Local(rel_path) => write!(f, "{}", rel_path.to_string()),
        }
    }
}

impl Addressable for Path {
    fn node(&self) -> Text {
        match self {
            Path::Remote(abs_path) => abs_path.node(),
            Path::Local(rel_path) => rel_path.node(),
        }
    }

    fn lane(&self) -> Text {
        match self {
            Path::Remote(abs_path) => abs_path.lane(),
            Path::Local(rel_path) => rel_path.lane(),
        }
    }

    fn relative_path(&self) -> RelativePath {
        match self {
            Path::Remote(abs_path) => abs_path.relative_path(),
            Path::Local(rel_path) => rel_path.relative_path(),
        }
    }

    fn host(&self) -> Option<Url> {
        match self {
            Path::Remote(abs_path) => abs_path.host(),
            Path::Local(rel_path) => rel_path.host(),
        }
    }
}

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

impl Addressable for AbsolutePath {
    fn node(&self) -> Text {
        self.node.clone()
    }

    fn lane(&self) -> Text {
        self.lane.clone()
    }

    fn relative_path(&self) -> RelativePath {
        self.relative_path()
    }

    fn host(&self) -> Option<Url> {
        Some(self.host.clone())
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

impl Addressable for RelativePath {
    fn node(&self) -> Text {
        self.node.clone()
    }

    fn lane(&self) -> Text {
        self.lane.clone()
    }

    fn relative_path(&self) -> RelativePath {
        self.clone()
    }

    fn host(&self) -> Option<Url> {
        None
    }
}
