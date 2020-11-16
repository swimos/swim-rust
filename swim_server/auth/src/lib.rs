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

use crate::googleid::GoogleIdAuthenticator;
use crate::policy::PolicyDirective;
use chrono::{Duration, Utc};
use futures::future::ready;
use futures::Future;
use futures_util::future::Ready;
use std::collections::HashMap;
use std::fmt::Display;
use swim_common::form::Form;
use swim_common::model::time::Timestamp;
use swim_common::model::Value;

pub mod googleid;
pub mod policy;

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
    MalformattedResponse(String),
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

pub struct AlwaysAllowAuthenticator;
impl<'s> Authenticator<'s> for AlwaysAllowAuthenticator {
    type Credentials = Value;
    type AuthenticateFuture = Ready<Result<PolicyDirective, AuthenticationError>>;

    fn authenticate(&'s mut self, credentials: Self::Credentials) -> Self::AuthenticateFuture {
        ready(Ok(PolicyDirective::allow(credentials)))
    }
}

pub struct AlwaysDenyAuthenticator;
impl<'s> Authenticator<'s> for AlwaysDenyAuthenticator {
    type Credentials = Value;
    type AuthenticateFuture = Ready<Result<PolicyDirective, AuthenticationError>>;

    fn authenticate(&'s mut self, credentials: Self::Credentials) -> Self::AuthenticateFuture {
        ready(Ok(PolicyDirective::deny(credentials)))
    }
}

pub struct AlwaysForbidAuthenticator;
impl<'s> Authenticator<'s> for AlwaysForbidAuthenticator {
    type Credentials = Value;
    type AuthenticateFuture = Ready<Result<PolicyDirective, AuthenticationError>>;

    fn authenticate(&'s mut self, credentials: Self::Credentials) -> Self::AuthenticateFuture {
        ready(Ok(PolicyDirective::forbid(credentials)))
    }
}

#[derive(Clone, PartialEq, Eq, Debug, Ord, PartialOrd, Hash)]
pub struct Token {
    id: String,
    expires: Timestamp,
}

impl Token {
    pub fn new(id: String, expires: Timestamp) -> Token {
        Token { id, expires }
    }
}

pub trait Expired {
    fn expired(&self, skew: i64) -> bool;
}

impl Expired for Token {
    fn expired(&self, skew: i64) -> bool {
        self.expires
            .as_ref()
            .lt(&(Utc::now() + Duration::seconds(skew)))
    }
}

#[derive(Debug)]
pub struct TokenDirective {
    token: Token,
    policy: PolicyDirective,
}

impl Expired for TokenDirective {
    fn expired(&self, skew: i64) -> bool {
        self.token.expired(skew)
    }
}

#[derive(Debug)]
struct TokenStore {
    skew: i64,
    tokens: HashMap<String, TokenDirective>,
}

impl TokenStore {
    pub fn new(skew: i64) -> TokenStore {
        TokenStore {
            skew,
            tokens: Default::default(),
        }
    }
}

impl TokenStore {
    pub fn insert(&mut self, token: Token, policy: PolicyDirective) {
        let id = token.id.clone();
        let token_directive = TokenDirective { token, policy };

        self.tokens.insert(id, token_directive);
    }

    pub fn get(&mut self, key: &String) -> Option<&TokenDirective> {
        let TokenStore { skew, tokens } = self;

        tokens.retain(|_k, token| !token.expired(*skew));
        tokens.get(key)
    }
}
