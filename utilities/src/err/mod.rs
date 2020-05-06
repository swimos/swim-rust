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

/// A trait to determine whether or not an error is transient or permanent. This is useful for
/// determining if a HTTP request is transient (such as the host being temporarily unavailable) or
/// permanent (such as a host not being found)
pub trait MaybeTransientErr {
    // Whether or not the current error is transient.
    fn is_transient(&self) -> bool;
    /// Return a new instance indicating a failed or final state. Useful for changing between a failed
    /// internal state to a more general state for the caller.
    fn permanent(&self) -> Self;
}
