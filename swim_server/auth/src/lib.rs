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

use crate::googleid::GoogleIdAuthenticator;
use crate::policy::{IssuedPolicy, PolicyDirective};
use crate::token::Token;
use futures::future::ready;
use futures::Future;
use futures_util::future::Ready;
use std::fmt::Display;
use swim_common::form::Form;
use swim_model::Value;

pub mod googleid;
pub mod policy;
mod token;

pub enum AgentAuthenticator {
    AlwaysAllow(AlwaysAllowAuthenticator),
    AlwaysDeny(AlwaysDenyAuthenticator),
    AlwaysForbid(AlwaysForbidAuthenticator),
    GoogleId(Box<GoogleIdAuthenticator>),
}

#[derive(Debug, Eq, PartialEq)]
pub enum AuthenticationError {
    KeyStoreError(String),
    ServerError,
    Malformatted(String),
}

impl AuthenticationError {
    pub fn malformatted<A: Display>(msg: &str, cause: A) -> AuthenticationError {
        AuthenticationError::Malformatted(format!("{}: {}", msg, cause))
    }
}

/// A trait for defining authenticators that validates a remote host or user credentials against
/// some authentication implementation.
pub trait Authenticator<'s>: Form {
    /// The type of the structure that this authenticator requires.
    type Credentials: Form;
    /// A future that resolves to either a policy for the remote host or an associated error.
    type AuthenticateFuture: Future<Output = Result<IssuedPolicy, AuthenticationError>> + 's;

    /// Attempt to authenticate the remote host or user.
    fn authenticate(&'s mut self, credentials: Self::Credentials) -> Self::AuthenticateFuture;
}

/// An authenticator that will always allow the remote host.
#[derive(Form)]
#[form(tag = "allow")]
pub struct AlwaysAllowAuthenticator;

impl<'s> Authenticator<'s> for AlwaysAllowAuthenticator {
    type Credentials = Value;
    type AuthenticateFuture = Ready<Result<IssuedPolicy, AuthenticationError>>;

    fn authenticate(&'s mut self, credentials: Self::Credentials) -> Self::AuthenticateFuture {
        ready(Ok(IssuedPolicy::new(
            Token::empty(),
            PolicyDirective::allow(credentials),
        )))
    }
}

/// An authenticator that will always deny the remote host.
#[derive(Form)]
#[form(tag = "deny")]
pub struct AlwaysDenyAuthenticator;
impl<'s> Authenticator<'s> for AlwaysDenyAuthenticator {
    type Credentials = Value;
    type AuthenticateFuture = Ready<Result<IssuedPolicy, AuthenticationError>>;

    fn authenticate(&'s mut self, credentials: Self::Credentials) -> Self::AuthenticateFuture {
        ready(Ok(IssuedPolicy::new(
            Token::empty(),
            PolicyDirective::deny(credentials),
        )))
    }
}

/// An authenticator that will always forbid the remote host.
#[derive(Form)]
#[form(tag = "forbid")]
pub struct AlwaysForbidAuthenticator;
impl<'s> Authenticator<'s> for AlwaysForbidAuthenticator {
    type Credentials = Value;
    type AuthenticateFuture = Ready<Result<IssuedPolicy, AuthenticationError>>;

    fn authenticate(&'s mut self, credentials: Self::Credentials) -> Self::AuthenticateFuture {
        ready(Ok(IssuedPolicy::new(
            Token::empty(),
            PolicyDirective::forbid(credentials),
        )))
    }
}
