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

mod frame_mask;
mod stack;
#[cfg(test)]
mod tests;

use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::sync::Arc;

use crate::ptr::PtrKey;
use crate::var::TVarInner;
use std::error::Error;
use std::fmt::{Display, Formatter};
use crate::stm::{ExecResult, Stm};
use futures::{Future, Stream, StreamExt};
use futures::task::{Context, Poll};
use tokio::macros::support::Pin;
use futures_util::stream::FuturesUnordered;
use futures::stream::FusedStream;
use slab::Slab;
use crate::transaction::stack::NonEmptyStack;
use crate::transaction::frame_mask::FrameMask;

const DEFAULT_LOG_CAP: usize = 32;
const DEFAULT_STACK_SIZE: usize = 8;

#[derive(Debug, Clone)]
enum LogState {
    UnconditionalGet,
    UnconditionalSet,
    ConditionalSet(Arc<dyn Any + Send + Sync>),
}

impl LogState {

    fn has_dependency(&self) -> bool {
        !matches!(self, LogState::UnconditionalSet)
    }

}

impl Default for LogState {
    fn default() -> Self {
        LogState::UnconditionalGet
    }
}

#[derive(Debug)]
struct LogEntry {
    current: Arc<dyn Any + Send + Sync>,
    state: NonEmptyStack<LogState>,
}

impl LogEntry {
    fn get_value<T: Any + Send + Sync>(&self) -> Arc<T> {
        let LogEntry { current, .. } = self;
        match current.clone().downcast() {
            Err(_) => panic!(
                "Inconsistent transaction log. Expected {:?} but was {:?}.",
                TypeId::of::<T>(),
                current.type_id()
            ),
            Ok(t) => t,
        }
    }

    fn set<T: Any + Send + Sync>(&mut self, value: Arc<T>) {
        assert_eq!(value.as_ref().type_id(), self.current.as_ref().type_id());
        let old_value = std::mem::replace(&mut self.current, value);
        let state = self.state.peek_mut();
        let old_state = std::mem::take(state);
        *state = match old_state {
            LogState::UnconditionalGet => LogState::ConditionalSet(old_value),
            LogState::UnconditionalSet => LogState::UnconditionalSet,
            cond => cond,
        };
    }
}



#[derive(Debug)]
pub struct Transaction {
    log_assoc: HashMap<PtrKey<Arc<TVarInner>>, usize>,
    log: Slab<LogEntry>,
    masks: NonEmptyStack<FrameMask>,
    stack_size: usize,
}

impl Transaction {
    pub fn new(stack_size: usize) -> Self {
        Transaction {
            log_assoc: HashMap::new(),
            log: Slab::with_capacity(DEFAULT_LOG_CAP),
            masks: NonEmptyStack::new(FrameMask::new(), stack_size + 1),
            stack_size,
        }
    }

    fn num_dependencies(&self) -> usize {
        self.log.len()
    }

    pub(crate) async fn apply_get<T: Any + Send + Sync>(
        &mut self,
        var: &Arc<TVarInner>,
    ) -> Arc<T> {
        let k = PtrKey(var.clone());
        match self.log_assoc.get(&k).and_then(|i| self.log.get(*i)) {
            Some(entry) => entry.get_value(),
            _ => {
                let value = var.read().await;
                let result = value.clone().downcast().unwrap();
                let entry = LogEntry {
                    current: value,
                    state: NonEmptyStack::new(LogState::UnconditionalGet, self.stack_size + 1),
                };
                let i = self.log.insert(entry);
                self.log_assoc.insert(k, i);
                result
            }
        }
    }

    fn entry_for_set<T: Any + Send + Sync>(&mut self, k: PtrKey<Arc<TVarInner>>, value: Arc<T>) {
        let entry = LogEntry {
            current: value,
            state: NonEmptyStack::new(LogState::UnconditionalSet, self.stack_size),
        };
        let i = self.log.insert(entry);
        self.masks.peek_mut().insert(i);
        self.log_assoc.insert(k, i);
    }

    pub(crate) fn apply_set<T: Any + Send + Sync>(&mut self, var: &Arc<TVarInner>, value: Arc<T>) {
        let Transaction { log_assoc, log, masks, .. } = self;
        let k = PtrKey(var.clone());
        if let Some(i) = log_assoc.get(&k) {
            let entry = log.get_mut(*i)
                .expect("Log entries must be defined in they are in the association map.");
            if !masks.peek().contains(*i) {
                let new_state = (*entry.state.peek()).clone();
                entry.state.push(new_state);
            }
            entry.set(value);
            self.masks.peek_mut().insert(*i);
        } else {
            self.entry_for_set(k, value);
        }
    }

    pub(crate) async fn try_commit(&mut self) -> bool {
        let mut reads = FuturesUnordered::new();
        let mut writes = FuturesUnordered::new();
        for (key, i) in self.log_assoc.iter() {
            let LogEntry { current, state } = self.log.remove(*i);
            let PtrKey(var) = key;

            match state.take_top() {
                LogState::UnconditionalGet => {
                    reads.push(var.validate_read(current));
                },
                LogState::UnconditionalSet => {
                    writes.push(var.prepare_write(None, current));
                },
                LogState::ConditionalSet(old) => {
                    writes.push(var.prepare_write(Some(old), current));
                },
            }
        }
        let mut read_locks = Vec::with_capacity(reads.len());
        if !reads.is_empty() {
            while let Some(maybe_lck) = reads.next().await {
                match maybe_lck {
                    Some(lck) => {
                        read_locks.push(lck);
                    }
                    _ => {
                        return false;
                    }
                }
            }
        }
        let mut write_locks = Vec::with_capacity(writes.len());
        if !writes.is_empty() {
            while !writes.is_terminated() {
                while let Some(maybe_lck) = writes.next().await {
                    match maybe_lck {
                        Some(lck) => {
                            write_locks.push(lck);
                        }
                        _ => {
                            return false;
                        }
                    }
                }
            }
        }
        for write in write_locks.into_iter() {
            write.apply();
        }
        true
    }

    fn reads_changed_or_locked(&self) -> bool {
        self.log_assoc.iter().any(|(PtrKey(var), i)| {
            match self.log.get(*i) {
                Some(LogEntry { state, current }) if state.peek().has_dependency()=> {
                    var.has_changed(current)
                },
                _ => false
            }
        })
    }

    fn reset(&mut self) {
        self.log_assoc.clear();
        self.log.clear();
    }


}

#[derive(Debug)]
pub enum TransactionError {
    Aborted {
        error: Box<dyn Error + Send + 'static>,
    },
    HighContention {
        num_failed: usize,
    },
    TransactionDiverged,
    StackOverflow {
        depth: usize,
    },
    TooManyAttempts {
        num_attempts: usize,
    },
    InvalidRetry,
}

impl Display for TransactionError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            TransactionError::Aborted { error } => {
                write!(f, "Transaction aborted: {}", error)
            },
            TransactionError::HighContention { num_failed: num_retries } => {
                write!(f, "Transaction could not commit after {} attempts.", num_retries)
            },
            TransactionError::TransactionDiverged => {
                write!(f, "Transaction took too long to execute.")
            },
            TransactionError::StackOverflow { depth } => {
                write!(f, "The stack depth of the transaction ({}) exceeded the maximum depth.", depth)
            },
            TransactionError::TooManyAttempts { num_attempts } => {
                write!(f, "Failed to complete after {} attempts.", num_attempts)
            }
            TransactionError::InvalidRetry => {
                write!(f, "Retry on transaction with no data dependencies.")
            }
        }
    }
}

impl Error for TransactionError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            TransactionError::Aborted { error } => Some(error.as_ref()),
            _ => None
        }
    }
}


pub trait RetryManager {

    type ContentionManager: Stream<Item = ()> + Unpin;

    fn contention_manager(&self) -> Self::ContentionManager;

    fn max_retries(&self) -> usize;

}

pub async fn atomically<S, Retries>(stm: &S,
                              retries: Retries,
) -> Result<S::Result, TransactionError>
where
    S: Stm,
    Retries: RetryManager,
{

    let mut contention_manager = retries.contention_manager();
    let max_retries = retries.max_retries();
    let mut transaction = Transaction::new(S::required_stack().unwrap_or(DEFAULT_STACK_SIZE));
    let mut failed_commits: usize = 0;
    let mut num_attempts: usize = 0;

    loop {
        num_attempts += 1;
        let exec_result = stm.run_in(&mut transaction).await;
        match exec_result {
            ExecResult::Done(t) => {
                if transaction.try_commit().await {
                    return Ok(t);
                } else {
                    failed_commits = failed_commits.saturating_add(1);
                    if num_attempts >= max_retries.saturating_add(1) {
                        return Err(TransactionError::TooManyAttempts { num_attempts });
                    } else if contention_manager.next().await.is_none() {
                        return Err(TransactionError::HighContention { num_failed: failed_commits })
                    }
                }
            },
            ExecResult::Abort(error) => {
                return Err(TransactionError::Aborted { error });
            },
            ExecResult::Retry => {
                if transaction.num_dependencies() == 0 {
                    return Err(TransactionError::InvalidRetry);
                } else if num_attempts >= max_retries.saturating_add(1) {
                    return Err(TransactionError::TooManyAttempts { num_attempts });
                } else {
                    AwaitChanged::new(&mut transaction).await;
                }
            },
        }
        transaction.reset();
    }

}

pub struct AwaitChanged<'a>(Option<&'a mut Transaction>);

impl<'a> AwaitChanged<'a> {

    fn new(transaction: &'a mut Transaction) -> Self {
        AwaitChanged(Some(transaction))
    }

}


impl<'a> Future for AwaitChanged<'a> {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if let Some(transaction) = self.0.take() {
            if transaction.reads_changed_or_locked() {
                return Poll::Ready(());
            }
            transaction.log_assoc.iter().for_each(|(PtrKey(var), i)| {
                match transaction.log.get(*i) {
                    Some(LogEntry { state, ..}) if state.peek().has_dependency()  => {
                        var.subscribe(cx.waker().clone());
                    },
                    _ => {}
                }
            });
            if transaction.reads_changed_or_locked() {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        } else {
            Poll::Ready(())
        }

    }
}