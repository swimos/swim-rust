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

use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::sync::Arc;

use crate::ptr::PtrKey;
use crate::var::{TVarInner, VarRef};
use std::error::Error;
use std::fmt::{Display, Formatter};
use crate::stm::{ExecResult, Stm};
use futures::{Future, Stream, StreamExt};
use futures::task::{AtomicWaker, Context, Poll};
use tokio::macros::support::Pin;

#[derive(Debug)]
enum LogState {
    UnconditionalGet,
    UnconditionalSet,
    ConditionalSet(Arc<dyn Any>),
}

impl Default for LogState {
    fn default() -> Self {
        LogState::UnconditionalGet
    }
}

#[derive(Debug)]
struct LogEntry {
    current: Arc<dyn Any + Send + Sync>,
    state: LogState,
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

    fn set<T: Any + Send + Sync>(&mut self, value: T) {
        let old_value = std::mem::replace(&mut self.current, Arc::new(value));
        let old_state = std::mem::take(&mut self.state);
        self.state = match old_state {
            LogState::UnconditionalGet => LogState::ConditionalSet(old_value),
            LogState::UnconditionalSet => LogState::UnconditionalSet,
            cond @ LogState::ConditionalSet(_) => cond,
        };
    }
}

#[derive(Debug, Default)]
pub struct Transaction {
    log: HashMap<PtrKey<Arc<dyn VarRef>>, LogEntry>,
    waiter: Option<Arc<AtomicWaker>>,
}

impl Transaction {
    pub fn new() -> Self {
        Transaction {
            log: HashMap::new(),
            waiter: None,
        }
    }

    pub(crate) async fn apply_get<T: Any + Send + Sync>(
        &mut self,
        var: &Arc<TVarInner<T>>,
    ) -> Arc<T> {
        let vref: Arc<dyn VarRef> = var.clone();
        let k = PtrKey(vref);
        match self.log.get_mut(&k) {
            Some(entry) => entry.get_value(),
            _ => {
                let value = var.read().await;
                let entry = LogEntry {
                    current: value.clone(),
                    state: LogState::UnconditionalGet,
                };
                self.log.insert(k, entry);
                value
            }
        }
    }

    pub(crate) fn apply_set<T: Any + Send + Sync>(&mut self, var: &Arc<TVarInner<T>>, value: Arc<T>) {
        let vref: Arc<dyn VarRef> = var.clone();
        let k = PtrKey(vref);
        match self.log.get_mut(&k) {
            Some(entry) => {
                entry.set(value);
            }
            _ => {
                let entry = LogEntry {
                    current: value,
                    state: LogState::UnconditionalSet,
                };
                self.log.insert(k, entry);
            }
        }
    }

    pub(crate) async fn try_commit(&mut self) -> bool {
        unimplemented!()
    }

    fn reads_changed_or_locked(&self) -> bool {
        self.log.iter().any(|(PtrKey(var), entry)| {
            match &entry.state {
                LogState::UnconditionalGet | LogState::ConditionalSet(_) => {
                    var.has_changed(entry.current.clone())
                },
                _ => false
            }
        })
    }


}

#[derive(Debug)]
pub enum TransactionError {
    Aborted {
        error: Box<dyn Error + 'static>,
    },
    HighContention {
        num_failed: usize,
    },
    TransactionDiverged,
    StackOverflow {
        depth: usize,
    }
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


pub async fn atomically<S, C>(stm: &S,
                              mut contention_manager: C,
) -> Result<S::Result, TransactionError>
where
    S: Stm,
    C: Stream<Item = ()> + Unpin,
{

    let mut transaction = Transaction::new();
    let mut failed_commits: usize = 0;

    loop {
        let exec_result = stm.run_in(&mut transaction).await;
        match exec_result {
            ExecResult::Done(t) => {
                if transaction.try_commit().await {
                    return Ok(t);
                } else {
                    failed_commits += 1;
                    if contention_manager.next().await.is_none() {
                        return Err(TransactionError::HighContention { num_failed: failed_commits })
                    }
                }
            },
            ExecResult::Abort(error) => {
                return Err(TransactionError::Aborted { error });
            },
            ExecResult::Retry => {
                AwaitChanged::new(&mut transaction).await;
            },
        }

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
            let waker = transaction.waiter.get_or_insert_with(|| Arc::new(AtomicWaker::new()));
            waker.register(cx.waker());
            transaction.log.iter().for_each(|(PtrKey(var), entry)| {
                match &entry.state {
                    LogState::UnconditionalGet | LogState::ConditionalSet(_) => {
                        var.subscribe(waker.clone());
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