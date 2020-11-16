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
use im::HashSet;
use std::fmt::Display;
use swim_common::form::Form;

pub mod googleid;
pub mod openid;
pub mod policy;

pub enum AuthenticationError {
    ServerError,
    MalformattedResponse(String),
}

impl AuthenticationError {
    pub fn malformatted<A: Display>(msg: &str, cause: A) -> AuthenticationError {
        AuthenticationError::MalformattedResponse(format!("{}: {}", msg, cause))
    }
}

pub trait Authenticator<'s> {
    type Credentials: Form;
    type StartFuture: Future<Output = Result<(), AuthenticationError>>;
    type AuthenticateFuture: Future<Output = Result<PolicyDirective, AuthenticationError>> + 's;

    fn start(&'s mut self) -> Self::StartFuture;

    fn authenticate(&'s mut self, credentials: Self::Credentials) -> Self::AuthenticateFuture;
}

pub trait VerifyOpenIdToken {
    fn verify_audience(&self, audiences: &HashSet<String>) -> Result<(), AuthenticationError>;

    fn verify_email(&self, emails: &HashSet<String>) -> Result<(), AuthenticationError>;
}
