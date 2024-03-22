// Copyright 2015-2023 Swim Inc.
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

use swimos_utilities::routing::route_pattern::RoutePattern;

/// The name of the route paramter containing the encoded node URI.
pub const NODE_PARAM: &str = "node_uri";
/// The name of the route parameter containing the encoded lane name.
pub const LANE_PARAM: &str = "lane_name";

const NODE_PATTERN: &str = "swimos:meta:node/:node_uri";
const LANE_PATTERN: &str = "swimos:meta:node/:node_uri/lane/:lane_name";
const MESH_PATTERN: &str = "swimos:meta:mesh";

/// Create a route pattern for the mesh meta-agents.
pub fn mesh_pattern() -> RoutePattern {
    RoutePattern::parse_str(MESH_PATTERN).expect("Mesh pattern should be valid.")
}

/// Create a route pattern for the node meta-agents.
pub fn node_pattern() -> RoutePattern {
    RoutePattern::parse_str(NODE_PATTERN).expect("Node pattern should be valid.")
}

/// Create a route pattern for the lane meta-agents.
pub fn lane_pattern() -> RoutePattern {
    RoutePattern::parse_str(LANE_PATTERN).expect("Lane pattern should be valid.")
}

#[cfg(test)]
mod tests {
    use crate::route::mesh_pattern;
    use swimos_utilities::routing::route_uri::RouteUri;

    use super::{lane_pattern, node_pattern, LANE_PARAM, NODE_PARAM};

    #[test]
    fn recognize_mesh() {
        let uri = "swimos:meta:mesh".parse::<RouteUri>().unwrap();
        let pattern = mesh_pattern();
        let params = pattern.unapply_route_uri(&uri);
        assert!(params.is_ok());
        let map = params.unwrap();
        assert_eq!(map.len(), 0);
    }

    #[test]
    fn recognize_node() {
        let uri = "swimos:meta:node/unit%2Ffoo".parse::<RouteUri>().unwrap();
        let pattern = node_pattern();
        let params = pattern.unapply_route_uri(&uri);
        assert!(params.is_ok());
        let map = params.unwrap();
        assert_eq!(map.len(), 1);
        assert_eq!(map.get(NODE_PARAM), Some(&"unit/foo".to_string()));
    }

    #[test]
    fn recognize_lane() {
        let uri = "swimos:meta:node/unit%2Ffoo/lane/pulse"
            .parse::<RouteUri>()
            .unwrap();
        let pattern = lane_pattern();
        let params = pattern.unapply_route_uri(&uri);
        assert!(params.is_ok());
        let map = params.unwrap();
        assert_eq!(map.len(), 2);
        assert_eq!(map.get(NODE_PARAM), Some(&"unit/foo".to_string()));
        assert_eq!(map.get(LANE_PARAM), Some(&"pulse".to_string()));
    }
}
