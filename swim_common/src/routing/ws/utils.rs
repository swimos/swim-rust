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

use crate::routing::{InvalidUriError, InvalidUriErrorKind};
use http::uri::Scheme;
use http::{Request, Uri};
use std::str::FromStr;

/// If the request scheme is `warp`, `swim`, `swims` or `warps` then it is replaced with a
/// supported format. If the scheme is invalid then an error is returned.
pub fn maybe_resolve_scheme<T>(request: Request<T>) -> Result<Request<T>, InvalidUriError> {
    let uri = request.uri().clone();

    let new_scheme = match uri.scheme_str() {
        Some("swim") | Some("warp") | Some("ws") => "ws",
        Some("swims") | Some("warps") | Some("wss") => "wss",
        Some(s) => {
            return Err(InvalidUriError::new(
                InvalidUriErrorKind::UnsupportedScheme,
                Some(s.into()),
            ))
        }
        None => {
            return Err(InvalidUriError::new(
                InvalidUriErrorKind::MissingScheme,
                None,
            ))
        }
    };

    let (mut request_parts, request_t) = request.into_parts();
    let mut uri_parts = uri.into_parts();
    uri_parts.scheme = Some(Scheme::from_str(new_scheme)?);

    // infallible as `ws` and `wss` are valid schemes and the previous scheme already parsed.
    let uri = Uri::from_parts(uri_parts).unwrap();
    request_parts.uri = uri;

    Ok(Request::from_parts(request_parts, request_t))
}
