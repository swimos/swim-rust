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

use crate::policy::PolicyDirective;
use futures::Future;
use std::fmt::Display;
use swim_common::form::Form;

pub mod googleid;
pub mod openid;
pub mod policy;

#[derive(Debug)]
pub enum AuthenticationError {
    MalformattedResponse(String),
    ServerError,
}

impl AuthenticationError {
    pub fn malformatted<A: Display>(msg: &str, cause: A) -> AuthenticationError {
        AuthenticationError::MalformattedResponse(format!("{}: {}", msg, cause))
    }
}

pub trait Authenticator<'s> {
    type Credentials: Form;
    type AuthenticateFuture: Future<Output = Result<PolicyDirective, AuthenticationError>> + 's;

    fn authenticate(&'s mut self, credentials: Self::Credentials) -> Self::AuthenticateFuture;
}
