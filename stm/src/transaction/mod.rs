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
#[cfg(test)]
mod tests;

use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::sync::Arc;

use crate::ptr::PtrKey;
use crate::stm::{ExecResult, Stm, StmError};
use crate::transaction::frame_mask::FrameMask;
use crate::var::{Contents, TVarInner};
use futures::stream::FusedStream;
use futures::task::{Context, Poll};
use futures::{Future, Stream, StreamExt};
use futures_util::stream::FuturesUnordered;
use slab::Slab;
use std::error::Error;
use std::fmt::{Display, Formatter};
use tokio::macros::support::Pin;

const DEFAULT_LOG_CAP: usize = 32;
const DEFAULT_STACK_SIZE: usize = 8;

#[derive(Debug)]
struct MaskStack(Vec<FrameMask>);

impl MaskStack {
    fn new(capacity: usize) -> Self {
        MaskStack(Vec::with_capacity(capacity))
    }

    fn enter(&mut self) {
        let MaskStack(stack) = self;
        stack.push(FrameMask::new());
    }

    fn pop(&mut self) -> Option<FrameMask> {
        let MaskStack(stack) = self;
        stack.pop()
    }

    fn root_or_updated(&self, i: usize) -> bool {
        let MaskStack(stack) = self;
        match stack.last() {
            Some(mask) => mask.contains(i),
            _ => true,
        }
    }

    fn set_updated(&mut self, i: usize) {
        let MaskStack(stack) = self;
        if let Some(mask) = stack.last_mut() {
            mask.insert(i);
        }
    }

    fn clear(&mut self) {
        let MaskStack(stack) = self;
        stack.clear();
    }
}

#[derive(Debug, Clone)]
enum LogState {
    UnconditionalGet(Contents),
    UnconditionalSet,
    ConditionalSet(Contents),
}

impl Default for LogState {
    fn default() -> Self {
        LogState::UnconditionalSet
    }
}

impl LogState {
    fn has_dependency(&self) -> bool {
        !matches!(self, LogState::UnconditionalSet)
    }
}

#[derive(Debug)]
struct LogEntry {
    state: LogState,
    stack: Vec<Contents>,
}

const INCONSISTENT_STATE: &str = "Stack must be non-empty when a value has been set.";

impl LogEntry {
    fn get_value<T: Any + Send + Sync>(&self) -> Arc<T> {
        let LogEntry { state, stack } = self;
        let current = match state {
            LogState::UnconditionalGet(original) => original.clone(),
            LogState::UnconditionalSet | LogState::ConditionalSet(_) => {
                stack.last().expect(INCONSISTENT_STATE).clone()
            }
        };
        match current.clone().downcast() {
            Err(_) => panic!(
                "Inconsistent transaction log. Expected {:?} but was {:?}.",
                TypeId::of::<T>(),
                current.type_id()
            ),
            Ok(t) => t,
        }
    }

    fn update_state_for_set(&mut self) {
        let old_state = std::mem::take(&mut self.state);
        self.state = match old_state {
            LogState::UnconditionalGet(original) | LogState::ConditionalSet(original) => {
                LogState::ConditionalSet(original)
            }
            ow => ow,
        };
    }

    fn set<T: Any + Send + Sync>(&mut self, value: Arc<T>) {
        assert_eq!(value.as_ref().type_id(), self.get_type_id());
        self.update_state_for_set();
        match self.stack.last_mut() {
            Some(top) => *top = value,
            _ => self.stack.push(value),
        }
    }

    fn enter<T: Any + Send + Sync>(&mut self, value: Arc<T>) {
        assert_eq!(value.as_ref().type_id(), self.get_type_id());
        self.update_state_for_set();
        self.stack.push(value);
    }

    fn get_type_id(&self) -> TypeId {
        match &self.state {
            LogState::UnconditionalGet(original) | LogState::ConditionalSet(original) => {
                original.as_ref().type_id()
            }
            LogState::UnconditionalSet => self
                .stack
                .last()
                .expect(INCONSISTENT_STATE)
                .as_ref()
                .type_id(),
        }
    }

    fn pop(&mut self) {
        let LogEntry { state, stack } = self;
        if matches!(state, LogState::UnconditionalSet | LogState::ConditionalSet(_)) {
            stack.pop().expect(INCONSISTENT_STATE);
        }
    }
}

#[derive(Debug)]
pub struct Transaction {
    log_assoc: HashMap<PtrKey<Arc<TVarInner>>, usize>,
    log: Slab<LogEntry>,
    masks: MaskStack,
    stack_size: usize,
}

impl Transaction {
    pub fn new(stack_size: usize) -> Self {
        Transaction {
            log_assoc: HashMap::new(),
            log: Slab::with_capacity(DEFAULT_LOG_CAP),
            masks: MaskStack::new(stack_size),
            stack_size,
        }
    }

    fn num_dependencies(&self) -> usize {
        self.log.len()
    }

    pub(crate) async fn apply_get<T: Any + Send + Sync>(&mut self, var: &Arc<TVarInner>) -> Arc<T> {
        let k = PtrKey(var.clone());
        match self.log_assoc.get(&k).and_then(|i| self.log.get(*i)) {
            Some(entry) => entry.get_value(),
            _ => {
                let value = var.read().await;
                let result = value.clone().downcast().unwrap();
                let entry = LogEntry {
                    state: LogState::UnconditionalGet(value),
                    stack: Vec::with_capacity(self.stack_size),
                };
                let i = self.log.insert(entry);
                self.log_assoc.insert(k, i);
                self.masks.set_updated(i);
                result
            }
        }
    }

    fn entry_for_set<T: Any + Send + Sync>(&mut self, k: PtrKey<Arc<TVarInner>>, value: Arc<T>) {
        let mut stack: Vec<Contents> = Vec::with_capacity(self.stack_size);
        stack.push(value);
        let entry = LogEntry {
            state: LogState::UnconditionalSet,
            stack,
        };
        let i = self.log.insert(entry);
        self.masks.set_updated(i);
        self.log_assoc.insert(k, i);
    }

    pub(crate) fn apply_set<T: Any + Send + Sync>(&mut self, var: &Arc<TVarInner>, value: Arc<T>) {
        let Transaction {
            log_assoc,
            log,
            masks,
            ..
        } = self;
        let k = PtrKey(var.clone());
        if let Some(i) = log_assoc.get(&k) {
            let entry = log
                .get_mut(*i)
                .expect("Log entries must be defined in they are in the association map.");
            if masks.root_or_updated(*i) {
                entry.set(value)
            } else {
                entry.enter(value);
                self.masks.set_updated(*i);
            }
        } else {
            self.entry_for_set(k, value);
        }
    }

    async fn try_commit(&mut self) -> bool {
        let mut reads = FuturesUnordered::new();
        let mut writes = FuturesUnordered::new();
        for (key, i) in self.log_assoc.iter() {
            let LogEntry { state, mut stack } = self.log.remove(*i);
            let PtrKey(var) = key;

            match state {
                LogState::UnconditionalGet(original) => {
                    reads.push(var.validate_read(original));
                }
                LogState::UnconditionalSet => {
                    let current = stack.pop().expect(INCONSISTENT_STATE);
                    writes.push(var.prepare_write(None, current));
                }
                LogState::ConditionalSet(original) => {
                    let current = stack.pop().expect(INCONSISTENT_STATE);
                    writes.push(var.prepare_write(Some(original), current));
                }
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
        self.log_assoc
            .iter()
            .any(|(PtrKey(var), i)| match self.log.get(*i) {
                Some(LogEntry { state, .. }) => match state {
                    LogState::UnconditionalGet(original) | LogState::ConditionalSet(original) => {
                        var.has_changed(original)
                    }
                    _ => false,
                },
                _ => false,
            })
    }

    fn reset(&mut self) {
        self.log_assoc.clear();
        self.log.clear();
        self.masks.clear();
    }

    pub(crate) fn enter_frame(&mut self) {
        self.masks.enter();
    }

    pub(crate) fn pop_frame(&mut self) {
        let Transaction { log, masks, .. } = self;

        match masks.pop() {
            Some(mask) => {
                for index in mask.iter() {
                    if let Some(entry) = log.get_mut(index) {
                        entry.pop();
                    }
                }
            }
            _ => panic!("The root frame of a transaction was popped."),
        }
    }
}

#[derive(Debug)]
pub enum TransactionError {
    Aborted { error: StmError },
    HighContention { num_failed: usize },
    TransactionDiverged,
    StackOverflow { depth: usize },
    TooManyAttempts { num_attempts: usize },
    InvalidRetry,
}

impl Display for TransactionError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            TransactionError::Aborted { error } => write!(f, "Transaction aborted: {}", error),
            TransactionError::HighContention {
                num_failed: num_retries,
            } => write!(
                f,
                "Transaction could not commit after {} attempts.",
                num_retries
            ),
            TransactionError::TransactionDiverged => {
                write!(f, "Transaction took too long to execute.")
            }
            TransactionError::StackOverflow { depth } => write!(
                f,
                "The stack depth of the transaction ({}) exceeded the maximum depth.",
                depth
            ),
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
            TransactionError::Aborted { error } => Some(error.as_error()),
            _ => None,
        }
    }
}

pub trait RetryManager {
    type ContentionManager: Stream<Item = ()> + Unpin;

    fn contention_manager(&self) -> Self::ContentionManager;

    fn max_retries(&self) -> usize;
}

pub async fn atomically<S, Retries>(
    stm: &S,
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
                        return Err(TransactionError::HighContention {
                            num_failed: failed_commits,
                        });
                    }
                }
            }
            ExecResult::Abort(error) => {
                return Err(TransactionError::Aborted { error });
            }
            ExecResult::Retry => {
                if transaction.num_dependencies() == 0 {
                    return Err(TransactionError::InvalidRetry);
                } else if num_attempts >= max_retries.saturating_add(1) {
                    return Err(TransactionError::TooManyAttempts { num_attempts });
                } else {
                    AwaitChanged::new(&mut transaction).await;
                }
            }
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
                    Some(LogEntry { state, .. }) if state.has_dependency() => {
                        var.subscribe(cx.waker().clone());
                    }
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
