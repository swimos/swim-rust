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

use swim_model::http::{Header, Uri};

pub mod on_delete;
pub mod on_get;
pub mod on_post;
pub mod on_put;

pub struct HttpRequestContext {
    uri: Uri,
    headers: Vec<Header>,
}

impl HttpRequestContext {
    pub(crate) fn new(uri: Uri, headers: Vec<Header>) -> Self {
        HttpRequestContext { uri, headers }
    }

    pub fn uri(&self) -> &Uri {
        &self.uri
    }

    pub fn headers(&self) -> &[Header] {
        self.headers.as_slice()
    }
}
