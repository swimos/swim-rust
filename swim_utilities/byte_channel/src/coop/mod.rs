// Copyright 2015-2021 Swim Inc.
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

use std::{
    cell::Cell,
    task::{Context, Poll},
};

const START_BUDGET: usize = 64;

thread_local! {
    static TASK_BUDGET: Cell<Option<usize>> = Cell::new(None);
}

#[inline]
pub fn consume_budget(context: &mut Context<'_>) -> Poll<()> {
    TASK_BUDGET.with(|budget| match budget.get() {
        Some(mut b) => {
            b = b.saturating_sub(1);
            if b == 0 {
                budget.set(None);
                context.waker().wake_by_ref();
                Poll::Pending
            } else {
                budget.set(Some(b));
                Poll::Ready(())
            }
        }
        None => {
            budget.set(Some(START_BUDGET));
            Poll::Ready(())
        }
    })
}

#[inline]
pub fn track_progress<T>(poll: Poll<T>) -> Poll<T> {
    if poll.is_pending() {
        TASK_BUDGET.with(|budget| {
            if let Some(mut b) = budget.get() {
                b = b.saturating_add(1);
                budget.set(Some(b));
            }
        })
    }
    poll
}
