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

#[derive(Default)]
pub struct Random {
    #[cfg(not(target_arch = "arm"))]
    inner: nanorand::WyRand,
    #[cfg(target_arch = "arm")]
    inner: rand::rngs::SmallRng,
}

impl Random {
    #[cfg(not(target_arch = "arm"))]
    pub fn generate_u32(&mut self) -> u32 {
        nanorand::RNG::generate(&mut self.inner)
    }

    #[cfg(target_arch = "arm")]
    pub fn generate_u32(&mut self) -> u32 {
        RngCore::next_u32(self.inner)
    }
}
