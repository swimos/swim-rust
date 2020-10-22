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

use crate::agent::meta::{META_EDGE, META_HOST, META_MESH, META_NODE, META_PART};
use crate::routing::{MetaKind, MetaPath, RoutingAddr};
use swim_common::warp::path::RelativePath;

#[test]
fn routing_addr_display() {
    let string = format!("{}", RoutingAddr::remote(0x1));
    assert_eq!(string, "Remote(1)");

    let string = format!("{}", RoutingAddr::local(0x1a));
    assert_eq!(string, "Local(1A)");
}

#[test]
fn test_meta_paths() {
    fn assert(kind: MetaKind, node: &str) {
        let path = RelativePath::new(node.to_owned() + "/test/node", "unit".to_string());
        assert_eq!(
            Ok((kind, RelativePath::new("test/node", "unit"))),
            path.into_kind_and_path()
        );
    }

    assert(MetaKind::Edge, META_EDGE);
    assert(MetaKind::Mesh, META_MESH);
    assert(MetaKind::Part, META_PART);
    assert(MetaKind::Host, META_HOST);
    assert(MetaKind::Node, META_NODE);

    let path = RelativePath::new(META_EDGE, "unit");
    assert_eq!(Err(path.clone()), path.into_kind_and_path());

    let path = RelativePath::new(META_EDGE.to_owned() + "/", "unit".to_string());
    assert_eq!(Err(path.clone()), path.into_kind_and_path());

    let path = RelativePath::new("swim:not::a_path", "unit");
    assert_eq!(Err(path.clone()), path.into_kind_and_path());
}
