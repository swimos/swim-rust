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
use crate::stm::error::StmError;
use crate::stm::stm_futures::{RunIn, TransactionFuture};
use crate::stm::{ExecResult, Stm};
use crate::transaction::frame_mask::{FrameMask, ReadWrite};
use crate::var::{Contents, ReadContentsFuture, TVarInner, TVarRead};
use futures::stream::FusedStream;
use futures::task::{Context, Poll};
use futures::{ready, Future, FutureExt, Stream, StreamExt};
use futures_util::stream::FuturesUnordered;
use slab::Slab;
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::pin::Pin;
use std::task::Waker;

// Default capacity of the transaction log.
const DEFAULT_LOG_CAP: usize = 32;
// Default initial stack size for Stm instances for which the maximum stack depth cannot be
// determined.
const DEFAULT_STACK_SIZE: usize = 8;

#[derive(Debug)]
struct Frame {
    vars: FrameMask,
    locals: FrameMask,
}

impl Frame {
    fn new() -> Self {
        Frame {
            vars: FrameMask::new(),
            locals: FrameMask::new(),
        }
    }
}

/// A stack of masks indicating which variables were read from/written two in each frame.
#[derive(Debug)]
struct MaskStack(Vec<Frame>);

impl MaskStack {
    fn new(capacity: usize) -> Self {
        MaskStack(Vec::with_capacity(capacity))
    }

    // Enter a new frame.
    fn enter(&mut self) {
        let MaskStack(stack) = self;
        stack.push(Frame::new());
    }

    // Pop the top frame.
    fn pop(&mut self) -> Option<Frame> {
        let MaskStack(stack) = self;
        stack.pop()
    }

    // Returns true if the stack is empty or a variable was written in this frame.
    fn root_or_set_in_frame(&self, i: usize) -> bool {
        let MaskStack(stack) = self;
        if let Some(Frame { vars: mask, .. }) = stack.last() {
            matches!(
                mask.get(i),
                Some(ReadWrite::Write) | Some(ReadWrite::ReadWrite)
            )
        } else {
            true
        }
    }

    fn root_or_local_set_in_frame(&self, i: usize) -> bool {
        let MaskStack(stack) = self;
        if let Some(Frame { locals: mask, .. }) = stack.last() {
            matches!(mask.get(i), Some(ReadWrite::Write))
        } else {
            true
        }
    }

    // If the stack is non-empty, record that a variable was read in this frame.
    fn set_read(&mut self, i: usize) {
        let MaskStack(stack) = self;
        if let Some(Frame { vars: mask, .. }) = stack.last_mut() {
            mask.read(i);
        }
    }

    // If the stack is non-empty, record that a variable was written in this frame.
    fn set_written(&mut self, i: usize) {
        let MaskStack(stack) = self;
        if let Some(Frame { vars: mask, .. }) = stack.last_mut() {
            mask.write(i);
        }
    }

    // If the stack is non-empty, record that a variable was written in this frame.
    fn set_local_written(&mut self, i: usize) {
        let MaskStack(stack) = self;
        if let Some(Frame { locals: mask, .. }) = stack.last_mut() {
            mask.write(i);
        }
    }

    // Dump all frames.
    fn clear(&mut self) {
        let MaskStack(stack) = self;
        stack.clear();
    }
}

/// State of an entry in the transaction log. If the state contains a value, this is the value
/// that was originally read from the variable.
#[derive(Debug, Clone)]
enum LogState {
    /// The value of the variable was read in this transaction.
    Get(Contents),
    /// The values of the variable was read in this transaction in a branch that did not
    /// complete.
    GetInFailedPath(Contents),
    /// A variable was written to in this transaction before its value was read and so there is
    /// no dependency on the original value.
    UnconditionalSet,
    /// A variable was written to in this transaction after its value had been read.
    ConditionalSet(Contents),
}

impl Default for LogState {
    fn default() -> Self {
        LogState::UnconditionalSet
    }
}

/// A transaction log entry for a single variable.
#[derive(Debug)]
struct LogEntry {
    /// Current state of the variable in the transaction.
    state: LogState,
    /// Stack of values that have been set into the value for each stack frame.
    stack: Vec<Contents>,
}

/// A transaction-local entry for a single variable.
#[derive(Debug)]
struct LocalEntry {
    stack: Vec<Contents>,
}

impl LocalEntry {
    fn pop(&mut self) {
        self.stack.pop();
    }

    fn set<T: Any + Send + Sync>(&mut self, value: Arc<T>) {
        if let Some(top) = self.stack.last_mut() {
            assert_eq!(value.as_ref().type_id(), top.as_ref().type_id());
            *top = value;
        } else {
            self.stack.push(value);
        }
    }

    // Set the value of a local variable (within the transaction), entering a new stack frame.
    fn enter<T: Any + Send + Sync>(&mut self, value: Arc<T>) {
        if let Some(top) = self.stack.last() {
            assert_eq!(value.as_ref().type_id(), top.as_ref().type_id());
        }
        self.stack.push(value);
    }

    fn get<T: Any + Send + Sync>(&self) -> Option<Arc<T>> {
        self.stack
            .last()
            .map(|current| match current.clone().downcast() {
                Err(_) => panic!(
                    "Inconsistent transaction local variable. Expected {:?} but was {:?}.",
                    TypeId::of::<T>(),
                    current.type_id()
                ),
                Ok(t) => t,
            })
    }
}

// Panic message for when an inconsistent transaction log is detected.
const INCONSISTENT_STATE: &str = "Stack must be non-empty when a value has been set.";

impl LogEntry {
    fn is_active(&self) -> bool {
        !matches!(self.state, LogState::GetInFailedPath(_))
    }

    // Get the current value (within the transaction) associated with this log entry.
    fn get_value<T: Any + Send + Sync>(&mut self) -> Arc<T> {
        let LogEntry { state, stack, .. } = self;
        let old_state = std::mem::take(state);
        let (current, new_state) = match old_state {
            LogState::Get(original) | LogState::GetInFailedPath(original) => {
                (original.clone(), LogState::Get(original))
            }
            s @ LogState::UnconditionalSet | s @ LogState::ConditionalSet(_) => {
                (stack.last().expect(INCONSISTENT_STATE).clone(), s)
            }
        };
        *state = new_state;
        match current.clone().downcast() {
            Err(_) => panic!(
                "Inconsistent transaction log. Expected {:?} but was {:?}.",
                TypeId::of::<T>(),
                current.type_id()
            ),
            Ok(t) => t,
        }
    }

    // Update the state of the entry in preparation for setting new value.
    fn update_state_for_set(&mut self) {
        let old_state = std::mem::take(&mut self.state);
        self.state = match old_state {
            LogState::Get(original) | LogState::ConditionalSet(original) => {
                LogState::ConditionalSet(original)
            }
            ow => ow,
        };
    }

    // Set the value of a variable (within the transaction).
    fn set<T: Any + Send + Sync>(&mut self, value: Arc<T>) {
        assert_eq!(value.as_ref().type_id(), self.get_type_id());
        self.update_state_for_set();
        match self.stack.last_mut() {
            Some(top) => *top = value,
            _ => self.stack.push(value),
        }
    }

    // Set the value of a variable (within the transaction), entering a new stack frame.
    fn enter<T: Any + Send + Sync>(&mut self, value: Arc<T>) {
        assert_eq!(value.as_ref().type_id(), self.get_type_id());
        self.update_state_for_set();
        self.stack.push(value);
    }

    // Get the type ID of the values in this entry.
    fn get_type_id(&self) -> TypeId {
        match &self.state {
            LogState::Get(original)
            | LogState::ConditionalSet(original)
            | LogState::GetInFailedPath(original) => original.as_ref().type_id(),
            LogState::UnconditionalSet => self
                .stack
                .last()
                .expect(INCONSISTENT_STATE)
                .as_ref()
                .type_id(),
        }
    }

    // Pop the stack for this entry. Returns true if the entry should be removed entirely.
    fn pop(&mut self, rw: ReadWrite) -> bool {
        let LogEntry { state, stack } = self;
        let old_state = std::mem::take(state);
        let new_state = match old_state {
            LogState::Get(original) => {
                debug_assert!(rw == ReadWrite::Read);
                Some(LogState::GetInFailedPath(original))
            }
            LogState::UnconditionalSet => {
                debug_assert!(rw == ReadWrite::Write);
                stack.pop().expect(INCONSISTENT_STATE);
                if stack.is_empty() {
                    None
                } else {
                    Some(LogState::UnconditionalSet)
                }
            }
            LogState::ConditionalSet(original) => {
                debug_assert!(rw != ReadWrite::Read);
                stack.pop().expect(INCONSISTENT_STATE);
                if stack.is_empty() {
                    if rw == ReadWrite::Write {
                        Some(LogState::Get(original))
                    } else {
                        Some(LogState::GetInFailedPath(original))
                    }
                } else {
                    Some(LogState::ConditionalSet(original))
                }
            }
            _ => None,
        };
        match new_state {
            Some(s) => {
                *state = s;
                false
            }
            _ => true,
        }
    }
}

/// A transaction within which an [`Stm`] can be executed.
#[derive(Debug)]
pub struct Transaction {
    // Mapping from variables to entries in the log.
    log_assoc: HashMap<PtrKey<TVarInner>, usize>,
    // Mapping from transaction-local indices to entries in the log.
    local_assoc: HashMap<u64, usize>,
    // State of each variable that is referred to in the transaction.
    log: Slab<LogEntry>,
    // State of each transaction-local variable that is referred to in the transaction.
    locals_log: Slab<LocalEntry>,
    // Records which variables have been accessed in each frame of the transaction stack.
    masks: MaskStack,
    // Estimated maximum depth of the transaction stack.
    stack_size: usize,
}

//Get the log entry associated with a variable, if it exists.
fn get_entry<'a>(
    log_assoc: &HashMap<PtrKey<TVarInner>, usize>,
    log: &'a mut Slab<LogEntry>,
    k: &PtrKey<TVarInner>,
) -> Option<(usize, &'a mut LogEntry)> {
    if let Some(i) = log_assoc.get(k) {
        log.get_mut(*i).map(|entry| (*i, entry))
    } else {
        None
    }
}

//Get the log entry associated with a local variable, if it exists.
fn get_local_entry_mut<'a>(
    local_assoc: &HashMap<u64, usize>,
    log: &'a mut Slab<LocalEntry>,
    index: u64,
) -> Option<(usize, &'a mut LocalEntry)> {
    if let Some(i) = local_assoc.get(&index) {
        log.get_mut(*i).map(|entry| (*i, entry))
    } else {
        None
    }
}

//Get the log entry associated with a local variable, if it exists.
fn get_local_entry<'a>(
    local_assoc: &HashMap<u64, usize>,
    log: &'a Slab<LocalEntry>,
    index: u64,
) -> Option<&'a LocalEntry> {
    if let Some(i) = local_assoc.get(&index) {
        log.get(*i)
    } else {
        None
    }
}

impl Transaction {
    /// Create a new transaction with an estimate of the maximum possible depth of the
    /// execution stack.
    pub fn new(stack_size: usize) -> Self {
        Transaction {
            log_assoc: HashMap::new(),
            local_assoc: HashMap::new(),
            log: Slab::with_capacity(DEFAULT_LOG_CAP),
            locals_log: Slab::with_capacity(DEFAULT_LOG_CAP),
            masks: MaskStack::new(stack_size),
            stack_size,
        }
    }

    /// The number of variables that have been accessed by the transaction.
    fn num_dependencies(&self) -> usize {
        self.log.len()
    }

    fn entry_for_set<T: Any + Send + Sync>(&mut self, k: PtrKey<TVarInner>, value: Arc<T>) {
        let mut stack: Vec<Contents> = Vec::with_capacity(self.stack_size);
        stack.push(value);
        let entry = LogEntry {
            state: LogState::UnconditionalSet,
            stack,
        };
        let i = self.log.insert(entry);
        self.masks.set_written(i);
        self.log_assoc.insert(k, i);
    }

    /// Execute a "set" on a variable within the context of the transaction.
    pub(crate) fn apply_set<T: Any + Send + Sync>(&mut self, var: &TVarInner, value: Arc<T>) {
        let Transaction {
            log_assoc,
            log,
            masks,
            ..
        } = self;
        let k = PtrKey(var.clone());
        if let Some((i, entry)) = get_entry(log_assoc, log, &k) {
            if masks.root_or_set_in_frame(i) {
                entry.set(value)
            } else {
                entry.enter(value);
                self.masks.set_written(i);
            }
        } else {
            self.entry_for_set(k, value);
        }
    }

    pub(crate) fn get_local<T: Any + Send + Sync>(&self, index: u64) -> Option<Arc<T>> {
        let Transaction {
            local_assoc,
            locals_log,
            ..
        } = self;
        get_local_entry(local_assoc, locals_log, index).and_then(|entry| entry.get())
    }

    pub(crate) fn set_local<T: Any + Send + Sync>(&mut self, index: u64, value: Arc<T>) {
        let Transaction {
            local_assoc,
            locals_log,
            masks,
            stack_size,
            ..
        } = self;
        if let Some((i, entry)) = get_local_entry_mut(local_assoc, locals_log, index) {
            if masks.root_or_local_set_in_frame(i) {
                entry.set(value)
            } else {
                entry.enter(value);
                masks.set_local_written(i);
            }
        } else {
            let mut stack: Vec<Contents> = Vec::with_capacity(*stack_size);
            stack.push(value);
            let entry = LocalEntry { stack };
            let i = locals_log.insert(entry);
            masks.set_local_written(i);
            local_assoc.insert(index, i);
        }
    }

    // Attempt to commit the transaction, returning true of the commit succeeded.
    async fn try_commit(&mut self) -> bool {
        let mut reads = FuturesUnordered::new();
        let mut writes = FuturesUnordered::new();
        for (key, i) in self.log_assoc.iter() {
            let LogEntry {
                state, mut stack, ..
            } = self.log.remove(*i);
            let PtrKey(var) = key;

            match state {
                LogState::Get(original) => {
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
                _ => {}
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
            write.apply().await;
        }
        true
    }

    // Check the consistency of the transaction log.
    fn reads_changed_or_locked(&self) -> bool {
        self.log_assoc
            .iter()
            .any(|(PtrKey(var), i)| match self.log.get(*i) {
                Some(LogEntry { state, .. }) => match state {
                    LogState::Get(original)
                    | LogState::ConditionalSet(original)
                    | LogState::GetInFailedPath(original) => var.has_changed(original),
                    _ => false,
                },
                _ => false,
            })
    }

    // Register a waker that will be called if any variable changes. If this returns true, a change
    // was detected while registering the waker.
    fn register_waker(&self, waker: &Waker) -> bool {
        self.log_assoc
            .iter()
            .any(|(PtrKey(var), i)| match self.log.get(*i) {
                Some(LogEntry { state, .. }) => match state {
                    LogState::Get(original)
                    | LogState::ConditionalSet(original)
                    | LogState::GetInFailedPath(original) => var.add_waker(original, waker),
                    _ => false,
                },
                _ => false,
            })
    }

    // Reset the transaction after a failed execution/commit.
    fn reset(&mut self) {
        self.log_assoc.clear();
        self.log.clear();
        self.masks.clear();
    }

    /// Push a new frame onto the execution stack.
    pub(crate) fn enter_frame(&mut self) {
        self.masks.enter();
    }

    /// Pop the top frame of the execution stack.
    pub(crate) fn pop_frame(&mut self) {
        let Transaction {
            log,
            locals_log,
            masks,
            ..
        } = self;

        match masks.pop() {
            Some(Frame { vars, locals }) => {
                for (index, rw) in vars.iter() {
                    let remove_entry = if let Some(entry) = log.get_mut(index) {
                        entry.pop(rw)
                    } else {
                        false
                    };
                    if remove_entry {
                        log.remove(index);
                    }
                }
                for (index, _) in locals.iter() {
                    if let Some(entry) = locals_log.get_mut(index) {
                        entry.pop();
                    }
                }
            }
            _ => panic!("The root frame of a transaction was popped."),
        }
    }
}

/// Errors that can occur when attempting to execute a transaction.
#[derive(Debug)]
pub enum TransactionError {
    /// The transaction was aborted.
    Aborted { error: StmError },
    /// The transaction failed to commit after some number of attempts due to contention with other
    /// transactions.
    HighContention { num_failed: usize },
    /// Execution of the transaction ran for too long.
    TransactionDiverged,
    /// The execution stack became too deep (likely due to a recursive transaction).
    StackOverflow { depth: usize },
    /// The transaction attempted to restart too many times.
    TooManyAttempts { num_attempts: usize },
    /// The transaction attempted to restart when it had not read from any variables.
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

/// A strategy for retrying transactions.
pub trait RetryManager {
    type ContentionManager: Stream<Item = ()> + Unpin;
    type RetryFut: Future<Output = bool> + Unpin;

    /// A stream of (potential) delays between attempts to apply the transaction that fail
    /// due to contention. If this stream terminates, the transaction will also.
    fn contention_manager(&self) -> Self::ContentionManager;

    /// Determines whether the transaction will be retried when completing with Retry.
    fn retry(&mut self) -> Self::RetryFut;
}

/// Attempt to run an [`Stm`] instance within a transaction, using the provided retry strategy.
pub async fn atomically<S, Retries>(
    stm: &S,
    mut retries: Retries,
) -> Result<S::Result, TransactionError>
where
    S: Stm,
    Retries: RetryManager,
{
    let mut contention_manager = retries.contention_manager();
    let mut transaction = Transaction::new(S::required_stack().unwrap_or(DEFAULT_STACK_SIZE));
    let mut failed_commits: usize = 0;
    let mut num_attempts: usize = 0;

    loop {
        num_attempts = num_attempts.saturating_add(1);
        let exec_result = RunIn::new(&mut transaction, stm.runner()).await;
        match exec_result {
            ExecResult::Done(t) => {
                if transaction.try_commit().await {
                    return Ok(t);
                } else {
                    failed_commits = failed_commits.saturating_add(1);
                    if !retries.retry().await {
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
                } else if !retries.retry().await {
                    return Err(TransactionError::TooManyAttempts { num_attempts });
                } else {
                    AwaitChanged::new(&mut transaction).await;
                }
            }
        }
        transaction.reset();
    }
}

/// A future that awaits changes in any of the variables that were read in a transaction.
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
            let waker = cx.waker();
            if transaction.register_waker(&waker) {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        } else {
            Poll::Ready(())
        }
    }
}

/// State for [`VarReadFuture`] indicating that the future is waiting to read from a the variable's
/// contents.
pub struct AwaitRead(Option<TVarInner>, ReadContentsFuture);

/// Transaction future for performing a read on a transactional variable.
pub enum VarReadFuture<T> {
    Start(TVarRead<T>),
    Wait(AwaitRead),
}

impl<T> VarReadFuture<T> {
    pub(crate) fn new(read: TVarRead<T>) -> Self {
        VarReadFuture::Start(read)
    }
}

impl<T: Any + Send + Sync> TransactionFuture for VarReadFuture<T> {
    type Output = Arc<T>;

    fn poll_in(
        mut self: Pin<&mut Self>,
        transaction: Pin<&mut Transaction>,
        cx: &mut Context<'_>,
    ) -> Poll<ExecResult<Self::Output>> {
        let Transaction {
            log_assoc,
            log,
            masks,
            stack_size,
            ..
        } = transaction.get_mut();

        loop {
            match self.as_mut().get_mut() {
                VarReadFuture::Start(read) => {
                    let k = PtrKey(read.inner().clone());
                    match get_entry(log_assoc, log, &k) {
                        Some((i, entry)) => {
                            if !entry.is_active() {
                                masks.set_read(i);
                            }
                            return Poll::Ready(ExecResult::Done(entry.get_value()));
                        }
                        _ => {
                            let PtrKey(inner) = k;
                            let read_future = inner.read();
                            self.set(VarReadFuture::Wait(AwaitRead(Some(inner), read_future)));
                        }
                    }
                }
                VarReadFuture::Wait(AwaitRead(inner, wait)) => {
                    let inner = inner.take().expect("Read future used twice.");
                    let value = ready!(wait.poll_unpin(cx));
                    let result = value.clone().downcast().unwrap();
                    let k = PtrKey(inner);
                    let entry = LogEntry {
                        state: LogState::Get(value),
                        stack: Vec::with_capacity(*stack_size),
                    };
                    let i = log.insert(entry);
                    log_assoc.insert(k, i);
                    masks.set_read(i);
                    return Poll::Ready(ExecResult::Done(result));
                }
            }
        }
    }
}
