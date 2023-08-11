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

use swim_model::http::{Header, HttpResponse, Method, StatusCode, Uri, Version};

pub enum MethodAndPayload<PostT, PutT = PostT> {
    Get,
    Head,
    Post(PostT),
    Put(PutT),
    Delete,
}

pub struct Request<PostT, PutT = PostT> {
    pub method_and_payload: MethodAndPayload<PostT, PutT>,
    pub uri: Uri,
    pub headers: Vec<Header>,
}

impl<PostT, PutT> Request<PostT, PutT> {
    pub fn method(&self) -> Method {
        match self.method_and_payload {
            MethodAndPayload::Get => Method::GET,
            MethodAndPayload::Head => Method::HEAD,
            MethodAndPayload::Post(_) => Method::POST,
            MethodAndPayload::Put(_) => Method::PUT,
            MethodAndPayload::Delete => Method::DELETE,
        }
    }
}

pub struct Response<T> {
    status_code: StatusCode,
    payload: T,
    headers: Vec<Header>,
}

pub type UnitResponse = Response<()>;

impl<T: Default> Default for Response<T> {
    fn default() -> Self {
        Self {
            status_code: StatusCode::OK,
            payload: Default::default(),
            headers: vec![],
        }
    }
}

impl<T> From<T> for Response<T> {
    fn from(value: T) -> Self {
        Response {
            status_code: StatusCode::OK,
            payload: value,
            headers: vec![],
        }
    }
}

impl Response<()> {
    pub fn not_supported() -> Self {
        Response {
            status_code: StatusCode::METHOD_NOT_ALLOWED,
            payload: (),
            headers: vec![],
        }
    }
}

impl<T> From<Response<T>> for HttpResponse<T> {
    fn from(value: Response<T>) -> Self {
        let Response {
            status_code,
            payload,
            headers,
        } = value;
        HttpResponse {
            status_code,
            version: Version::HTTP_1_1,
            headers,
            payload,
        }
    }
}
