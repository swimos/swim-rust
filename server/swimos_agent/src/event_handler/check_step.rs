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

use std::fmt::Debug;

use super::{Modification, ModificationFlags, StepResult};

pub(crate) fn check_is_continue<T>(
    result: StepResult<T>,
    id: u64,
    expected_flags: ModificationFlags,
) {
    match result {
        StepResult::Continue {
            modified_item: Some(Modification { item_id, flags }),
        } => {
            assert_eq!(item_id, id);
            assert_eq!(flags, expected_flags);
        }
        _ => panic!("Unexpected step result."),
    }
}

pub(crate) fn check_is_complete<T: Eq + Debug>(
    result: StepResult<T>,
    id: u64,
    expected_value: &T,
    expected_flags: ModificationFlags,
) {
    match result {
        StepResult::Complete {
            modified_item: Some(Modification { item_id, flags }),
            result,
        } => {
            assert_eq!(item_id, id);
            assert_eq!(&result, expected_value);
            assert_eq!(flags, expected_flags);
        }
        _ => panic!("Unexpected step result."),
    }
}
