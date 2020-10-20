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

use crate::agent::lane::channels::LaneKind;
use crate::agent::Lane;
use std::str::FromStr;
use swim_common::form::Form;
use utilities::uri::RelativeUri;

#[derive(Form)]
pub struct LaneInfo {
    lane_uri: RelativeUri,
    lane_type: LaneKind,
}

trait MakeLaneInfo {
    fn lane_info(&self) -> LaneInfo;
}

impl<L: Lane> MakeLaneInfo for L {
    fn lane_info(&self) -> LaneInfo {
        LaneInfo {
            lane_uri: RelativeUri::from_str(self.name())
                .expect("Previously validated URI failed to parse"),
            lane_type: self.kind(),
        }
    }
}
