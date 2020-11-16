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

use swim_common::model::Value;

#[derive(Debug, Clone)]
pub struct PolicyDirective {
    value: Value,
    policy: PolicyEffect,
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

    pub fn allows(&self) -> bool {
        matches!(self.policy, PolicyEffect::Allow)
    }

    pub fn denies(&self) -> bool {
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
