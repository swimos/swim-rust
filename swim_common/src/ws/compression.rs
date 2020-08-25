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

use crate::ws::WebSocketHandler;
use http::header::CONTENT_ENCODING;
use http::{HeaderValue, Request, Response};

struct DeflateHandler;

impl WebSocketHandler for DeflateHandler {
    fn on_request(&mut self, request: &mut Request<()>) {
        request
            .headers_mut()
            .insert(CONTENT_ENCODING, HeaderValue::from_static("deflate"));
    }

    fn on_response(&mut self, _response: &mut Response<()>) {}
}
