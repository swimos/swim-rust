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

use crate::routing::error::{ResolutionError, RouterError};
use crate::routing::remote::task::DispatchError;
use crate::routing::remote::ConnectionDropped;
use http::Uri;
use utilities::uri::{BadRelativeUri, UriIsAbsolute};

#[test]
fn dispatch_error_test() {
    let bad_uri: Uri = "swim://localhost/hello".parse().unwrap();
    let string =
        DispatchError::BadNodeUri(BadRelativeUri::Absolute(UriIsAbsolute(bad_uri.clone())))
            .to_string();
    assert_eq!(
        string,
        "Invalid relative URI: ''swim://localhost/hello' is an absolute URI.'"
    );

    let string = DispatchError::Unresolvable(ResolutionError::RouterDropped).to_string();
    assert_eq!(
        string,
        "Could not resolve a router endpoint: 'The router channel was dropped.'"
    );

    let string = DispatchError::RoutingProblem(RouterError::RouterDropped).to_string();
    assert_eq!(
        string,
        "Could not find a router endpoint: 'The router channel was dropped.'"
    );

    let string = DispatchError::Dropped(ConnectionDropped::Closed).to_string();
    assert_eq!(
        string,
        "The routing channel was dropped: 'The connection was explicitly closed.'"
    );
}
