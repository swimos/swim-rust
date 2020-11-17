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

use crate::token::{Expired, Token};
use swim_common::model::Value;

#[derive(Debug, Clone, PartialEq)]
pub struct PolicyDirective {
    value: Value,
    policy: PolicyEffect,
}

impl Default for PolicyDirective {
    fn default() -> Self {
        PolicyDirective::allow(Value::Extant)
    }
}

impl PolicyDirective {
    pub fn allow(value: Value) -> PolicyDirective {
        PolicyDirective {
            value,
            policy: PolicyEffect::Allow,
        }
    }

    pub fn deny(value: Value) -> PolicyDirective {
        PolicyDirective {
            value,
            policy: PolicyEffect::Deny,
        }
    }

    pub fn forbid(value: Value) -> PolicyDirective {
        PolicyDirective {
            value,
            policy: PolicyEffect::Forbid,
        }
    }

    pub fn allowed(&self) -> bool {
        matches!(self.policy, PolicyEffect::Allow)
    }

    pub fn denied(&self) -> bool {
        matches!(self.policy, PolicyEffect::Deny)
    }

    pub fn forbidden(&self) -> bool {
        matches!(self.policy, PolicyEffect::Forbid)
    }
}

#[derive(Debug, PartialOrd, PartialEq, Eq, Copy, Clone)]
pub enum PolicyEffect {
    Allow,
    Deny,
    Forbid,
}

pub struct IssuedPolicy {
    token: Token,
    policy: PolicyDirective,
}

impl IssuedPolicy {
    pub fn new(token: Token, policy: PolicyDirective) -> IssuedPolicy {
        IssuedPolicy { token, policy }
    }

    pub fn allow(value: Value) -> IssuedPolicy {
        IssuedPolicy::new(Token::empty(), PolicyDirective::allow(value))
    }

    pub fn deny(value: Value) -> IssuedPolicy {
        IssuedPolicy::new(Token::empty(), PolicyDirective::deny(value))
    }

    pub fn forbid(value: Value) -> IssuedPolicy {
        IssuedPolicy::new(Token::empty(), PolicyDirective::forbid(value))
    }

    pub fn policy(&self) -> &PolicyDirective {
        &self.policy
    }

    pub fn value(&self) -> &Value {
        &self.policy.value
    }

    pub fn into_policy_value(self) -> Value {
        self.policy.value
    }
}

impl Expired for IssuedPolicy {
    fn expired(&self, skew: i64) -> bool {
        self.token.expired(skew)
    }
}
