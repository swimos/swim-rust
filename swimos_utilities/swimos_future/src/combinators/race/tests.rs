// Copyright 2015-2024 Swim Inc.
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

use crate::combinators::race;
use futures::future::{pending, ready};
use futures::FutureExt;

#[tokio::test]
async fn race2_test() {
    let left_ready = race(ready(()), pending::<()>()).await;
    assert!(left_ready.is_left());

    let right_ready = race(pending::<()>(), ready(())).await;
    assert!(right_ready.is_right());

    let both_pending = race(pending::<()>(), pending::<()>());
    assert!(both_pending.now_or_never().is_none());

    let both_ready = race(ready(()), ready(())).await;
    assert!(both_ready.is_left());
}
