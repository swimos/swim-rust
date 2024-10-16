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

use std::{
    cell::Cell,
    num::NonZeroUsize,
    pin::Pin,
    task::{Context, Poll},
};

use futures::{ready, Future};
use pin_project::pin_project;

#[cfg(test)]
mod tests;

const DEFAULT_START_BUDGET: NonZeroUsize = unsafe { NonZeroUsize::new_unchecked(64) };

thread_local! {
    static TASK_BUDGET: Cell<Option<usize>> = const { Cell::new(None) };
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
            budget.set(Some(DEFAULT_START_BUDGET.get()));
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

fn set_budget(n: usize) {
    TASK_BUDGET.with(|budget| {
        budget.set(Some(n));
    })
}

/// Wraps a futures and ensures that the task budget is reset each time it is polled.
#[pin_project]
#[derive(Debug, Clone, Copy)]
pub struct RunWithBudget<F> {
    budget: NonZeroUsize,
    #[pin]
    fut: F,
}

impl<F> RunWithBudget<F> {
    /// Create a new wrapper with the default budget.
    pub fn new(fut: F) -> Self {
        RunWithBudget {
            budget: DEFAULT_START_BUDGET,
            fut,
        }
    }

    /// Create a new wrapper that sets the budget to the specified value each time it is polled.
    pub fn with_budget(budget: NonZeroUsize, fut: F) -> Self {
        RunWithBudget { budget, fut }
    }
}

/// Extension trait to allow futures to be run with a task budget.
pub trait BudgetedFutureExt: Sized + Future {
    /// Run this future with the default task budget.
    fn budgeted(self) -> RunWithBudget<Self> {
        RunWithBudget::new(self)
    }

    /// Run this future with the specified task budget.
    fn with_budget(self, budget: NonZeroUsize) -> RunWithBudget<Self> {
        RunWithBudget::with_budget(budget, self)
    }

    /// Run this future wit a specified task budget or the default if not is specified.
    fn with_budget_or_default(self, budget: Option<NonZeroUsize>) -> RunWithBudget<Self> {
        RunWithBudget::with_budget(budget.unwrap_or(DEFAULT_START_BUDGET), self)
    }

    /// Run this future, consuming budget if it does work.
    fn consuming(self) -> BudgetConsumer<Self> {
        BudgetConsumer::new(self)
    }
}

impl<F: Future> BudgetedFutureExt for F {}

impl<F: Default> Default for RunWithBudget<F> {
    fn default() -> Self {
        Self {
            budget: DEFAULT_START_BUDGET,
            fut: Default::default(),
        }
    }
}

impl<F: Future> Future for RunWithBudget<F> {
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let projected = self.project();
        set_budget(projected.budget.get());
        projected.fut.poll(cx)
    }
}

/// Wraps a future to consume the task budget when it performs work.
#[pin_project]
pub struct BudgetConsumer<F> {
    #[pin]
    fut: F,
}

impl<F> BudgetConsumer<F> {
    pub fn new(fut: F) -> Self {
        BudgetConsumer { fut }
    }
}

impl<F: Future> Future for BudgetConsumer<F> {
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        ready!(consume_budget(cx));
        let projected = self.project();
        track_progress(projected.fut.poll(cx))
    }
}
