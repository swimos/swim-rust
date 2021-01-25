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

// use crate::agent::meta::{
//     MetaKind, MetaPath, META_EDGE, META_HOST, META_MESH, META_NODE, META_PART,
// };
// use swim_common::warp::path::RelativePath;
//
// #[test]
// fn test_meta_paths() {
//     fn assert(kind: MetaKind, node: &str) {
//         let path = RelativePath::new(node.to_owned() + "/test/node", "unit".to_string());
//         assert_eq!(
//             Ok((kind, RelativePath::new("test/node", "unit"))),
//             path.into_kind_and_path()
//         );
//     }
//
//     assert(MetaKind::Edge, META_EDGE);
//     assert(MetaKind::Mesh, META_MESH);
//     assert(MetaKind::Part, META_PART);
//     assert(MetaKind::Host, META_HOST);
//     assert(MetaKind::Node("foo".into()), META_NODE);
//
//     let path = RelativePath::new(META_EDGE, "unit");
//     assert_eq!(Err(path.clone()), path.into_kind_and_path());
//
//     let path = RelativePath::new(META_EDGE.to_owned() + "/", "unit".to_string());
//     assert_eq!(Err(path.clone()), path.into_kind_and_path());
//
//     let path = RelativePath::new("swim:not::a_path", "unit");
//     assert_eq!(Err(path.clone()), path.into_kind_and_path());
// }

use crate::agent::meta::log::{DEBUG_URI, ERROR_URI, FAIL_URI, INFO_URI, TRACE_URI, WARN_URI};
use crate::agent::meta::LogLevel;

#[test]
fn log_level_uri() {
    assert_eq!(LogLevel::Trace.uri_ref(), TRACE_URI);
    assert_eq!(LogLevel::Debug.uri_ref(), DEBUG_URI);
    assert_eq!(LogLevel::Info.uri_ref(), INFO_URI);
    assert_eq!(LogLevel::Warn.uri_ref(), WARN_URI);
    assert_eq!(LogLevel::Error.uri_ref(), ERROR_URI);
    assert_eq!(LogLevel::Fail.uri_ref(), FAIL_URI);
}
