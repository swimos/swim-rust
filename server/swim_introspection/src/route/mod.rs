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

use swim_utilities::routing::route_pattern::RoutePattern;

const NODE_PULSE: &str = "swim:meta:node/:node_uri/pulse";

pub fn node_pulse_pattern() -> RoutePattern {
    RoutePattern::parse_str(NODE_PULSE).expect("Node pulse pattern should be valid.")
}

#[cfg(test)]
mod tests {
    use swim_utilities::routing::route_uri::RouteUri;

    use super::node_pulse_pattern;

    #[test]
    fn recognize_node_pulse() {
        let uri = "swim:meta:node/unit%2Ffoo/pulse"
            .parse::<RouteUri>()
            .unwrap();
        let pattern = node_pulse_pattern();
        let params = pattern.unapply_route_uri(&uri);
        assert!(params.is_ok());
        let map = params.unwrap();
        assert_eq!(map.len(), 1);
        assert_eq!(map.get("node_uri"), Some(&"unit/foo".to_string()));
    }
}
